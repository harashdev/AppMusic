package vn.muse.app

import android.webkit.WebResourceRequest
import android.webkit.WebResourceResponse
import org.json.JSONObject
import java.net.HttpURLConnection
import java.net.URL
import java.util.concurrent.ConcurrentHashMap

/**
 * Dự phòng thuần Kotlin để phát nhạc YouTube bằng IP của điện thoại, không cần tiến trình libmuse_audio.so và không qua server.
 *
 * Cách làm giống SimpMusic / NewPipe: app tự hỏi InnerTube (client ANDROID_VR, rồi IOS) lấy địa chỉ luồng âm thanh googlevideo
 * (địa chỉ này gắn với IP máy bạn), rồi tải từng đoạn Range và đưa cho <audio> trong WebView qua shouldInterceptRequest.
 * [LocalAudio.handle] gọi hàm này khi tiến trình cục bộ không có hoặc không lấy được nhạc. Mọi lỗi đều trả null để WebView
 * quay về gọi server như cũ.
 */
object NativeYt {
    private const val PREFIX = "/api/yt/audio/"
    private const val FIRST_CHUNK = 256L * 1024      // đoạn đầu nhỏ để nhạc lên tiếng nhanh
    private const val CHUNK = 2L * 1024 * 1024
    private const val TTL_MS = 3L * 3600 * 1000      // địa chỉ luồng sống vài giờ

    private class YtClient(val name: String, val hid: String, val ver: String, val ua: String, val extra: String)

    private val clients = listOf(
        YtClient(
            "ANDROID_VR", "28", "1.60.19",
            "com.google.android.apps.youtube.vr.oculus/1.60.19 (Linux; U; Android 12L; eureka-user Build/SQ3A.220605.009.A1) gzip",
            """{"deviceMake":"Oculus","deviceModel":"Quest 3","androidSdkVersion":32,"osName":"Android","osVersion":"12L"}"""
        ),
        YtClient(
            "IOS", "5", "20.10.4",
            "com.google.ios.youtube/20.10.4 (iPhone16,2; U; CPU iOS 18_3_2 like Mac OS X;)",
            """{"deviceMake":"Apple","deviceModel":"iPhone16,2","osName":"iPhone","osVersion":"18.3.2.22D82"}"""
        )
    )

    private class Info(val url: String, val size: Long, val mime: String, val ua: String, val at: Long)

    private val cache = ConcurrentHashMap<String, Info>()
    private val idRe = Regex("[A-Za-z0-9_-]{11}")
    private val rangeRe = Regex("bytes=(\\d*)-(\\d*)")

    private fun resolve(id: String, webm: Boolean, force: Boolean): Info? {
        val key = id + "|" + (if (webm) "1" else "0")
        if (force) cache.remove(key)
        val hit = cache[key]
        if (hit != null && System.currentTimeMillis() - hit.at < TTL_MS) return hit
        if (cache.size > 200) cache.clear()
        for (c in clients) {
            val info = try { query(c, id, webm) } catch (e: Exception) { null }
            if (info != null) { cache[key] = info; return info }
        }
        return null
    }

    private fun query(c: YtClient, id: String, webm: Boolean): Info? {
        val client = JSONObject(c.extra)
            .put("clientName", c.name).put("clientVersion", c.ver).put("hl", "en").put("gl", "VN")
        val body = JSONObject()
            .put("context", JSONObject().put("client", client))
            .put("videoId", id).put("contentCheckOk", true).put("racyCheckOk", true)
        val conn = URL("https://youtubei.googleapis.com/youtubei/v1/player?prettyPrint=false").openConnection() as HttpURLConnection
        try {
            conn.requestMethod = "POST"
            conn.connectTimeout = 8000
            conn.readTimeout = 12000
            conn.doOutput = true
            conn.setRequestProperty("User-Agent", c.ua)
            conn.setRequestProperty("Content-Type", "application/json")
            conn.setRequestProperty("X-YouTube-Client-Name", c.hid)
            conn.setRequestProperty("X-YouTube-Client-Version", c.ver)
            conn.outputStream.use { it.write(body.toString().toByteArray(Charsets.UTF_8)) }
            if (conn.responseCode != 200) return null
            val txt = conn.inputStream.bufferedReader(Charsets.UTF_8).use { it.readText() }
            val root = JSONObject(txt)
            if (root.optJSONObject("playabilityStatus")?.optString("status") != "OK") return null
            val fmts = root.optJSONObject("streamingData")?.optJSONArray("adaptiveFormats") ?: return null
            var best: JSONObject? = null
            var bestScore = Long.MIN_VALUE
            for (i in 0 until fmts.length()) {
                val f = fmts.optJSONObject(i) ?: continue
                val mime = f.optString("mimeType")
                if (!mime.startsWith("audio/") || f.optString("url").isEmpty()) continue
                var score = f.optLong("bitrate", 0L) / 1000
                val itag = f.optInt("itag", -1)
                if (webm) {
                    if (mime.startsWith("audio/webm")) score += 100000
                    if (itag == 251) score += 1000000
                } else {
                    if (mime.startsWith("audio/mp4")) score += 100000
                    if (itag == 140) score += 1000000
                }
                if (score > bestScore) { bestScore = score; best = f }
            }
            val f = best ?: return null
            val url = f.optString("url")
            val size = f.optString("contentLength", "0").toLongOrNull() ?: 0L
            if (url.isEmpty() || size <= 0L) return null
            val mime = f.optString("mimeType").substringBefore(';').trim().ifEmpty { "audio/mp4" }
            return Info(url, size, mime, c.ua, System.currentTimeMillis())
        } finally {
            conn.disconnect()
        }
    }

    /** Mở một đoạn [start, end] của luồng. Thử header Range trước, rồi tham số &range= (cách yt-dlp/NewPipe dùng). */
    private fun openRange(info: Info, start: Long, end: Long): HttpURLConnection? {
        for (viaParam in booleanArrayOf(false, true)) {
            try {
                val u = if (viaParam) info.url + "&range=" + start + "-" + end else info.url
                val c = URL(u).openConnection() as HttpURLConnection
                c.connectTimeout = 8000
                c.readTimeout = 20000
                c.setRequestProperty("User-Agent", info.ua)
                if (!viaParam) c.setRequestProperty("Range", "bytes=$start-$end")
                val code = c.responseCode
                // Với header Range phải là 206; 200 nghĩa là máy chủ bỏ qua Range và sẽ gửi cả file.
                if (code == 206 || (viaParam && code == 200)) return c
                c.disconnect()
            } catch (e: Exception) { /* thử cách kế tiếp */ }
        }
        return null
    }

    /** Dùng trong shouldInterceptRequest (luồng nền). Trả null = để WebView gọi server như bình thường. */
    fun handle(req: WebResourceRequest): WebResourceResponse? {
        if (!req.method.equals("GET", true)) return null
        val path = req.url.encodedPath ?: return null
        if (!path.startsWith(PREFIX)) return null
        val id = path.removePrefix(PREFIX)
        if (!idRe.matches(id)) return null
        val webm = req.url.getQueryParameter("f") == "webm"
        val rh = req.requestHeaders["Range"] ?: req.requestHeaders["range"]

        for (attempt in 0..1) {
            val info = resolve(id, webm, attempt == 1) ?: return null
            var start = 0L
            var endReq = Long.MAX_VALUE
            if (rh != null) {
                val m = rangeRe.find(rh)
                if (m != null) {
                    val a = m.groupValues[1].toLongOrNull()
                    val b = m.groupValues[2].toLongOrNull()
                    if (a == null && b != null) { start = maxOf(0L, info.size - b); endReq = info.size - 1 }
                    else { start = a ?: 0L; endReq = b ?: Long.MAX_VALUE }
                }
            }
            if (start >= info.size) return null
            val chunk = if (start == 0L) FIRST_CHUNK else CHUNK
            val end = minOf(endReq, info.size - 1, start + chunk - 1)
            val conn = openRange(info, start, end)
            if (conn == null) {
                // Địa chỉ luồng hết hạn / bị chặn: lấy lại một lần rồi thử tiếp.
                cache.remove(id + "|" + (if (webm) "1" else "0"))
                continue
            }
            val headers = HashMap<String, String>()
            headers["Content-Range"] = "bytes $start-$end/${info.size}"
            headers["Accept-Ranges"] = "bytes"
            headers["Content-Length"] = (end - start + 1).toString()
            headers["Cache-Control"] = "no-store"
            headers["Access-Control-Allow-Origin"] = "*"
            return try {
                WebResourceResponse(info.mime, null, 206, "Partial Content", headers, conn.inputStream)
            } catch (e: Exception) {
                conn.disconnect()
                null
            }
        }
        return null
    }
}
