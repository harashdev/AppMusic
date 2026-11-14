package vn.muse.app

import android.util.Log
import android.webkit.WebResourceRequest
import android.webkit.WebResourceResponse
import org.json.JSONObject
import java.net.HttpURLConnection
import java.net.URL
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.concurrent.thread

/**
 * Lấy nhạc YouTube bằng IP của điện thoại, không cần tiến trình libmuse_audio.so và không qua server.
 *
 * Cách làm giống SimpMusic / NewPipe: app tự hỏi InnerTube lấy địa chỉ luồng âm thanh googlevideo (địa chỉ này gắn với IP máy
 * bạn), rồi tải từng đoạn Range và đưa cho <audio> trong WebView qua shouldInterceptRequest.
 *
 * Khác bản cũ:
 *  - Hỏi NHIỀU client song song (so le 350ms, cái nào có nhạc trước thì dùng) thay vì chỉ 2 client; YouTube hay đổi client nào còn chạy.
 *  - Mỗi link đều được TẢI THỬ vài byte trước khi dùng. Client cần PO token vẫn trả link nhưng link đó 403 khi tải;
 *    bản cũ tin link đó nên hỏng, bản này bỏ link chết và chuyển sang client kế tiếp.
 *  - Cuối cùng mới thử các máy chủ Piped (gọi từ điện thoại) làm đường dự phòng.
 *  - Hết hạn link lấy từ tham số expire của chính link, không đoán 3 giờ.
 *  - Lỗi không còn bị nuốt: lý do từng client được ghi vào logcat (tag MuseYt) và trả về qua [lastError] để giao diện hiện ra.
 *
 * [LocalAudio.handle] gọi hàm này khi tiến trình cục bộ không có hoặc không lấy được nhạc. Mọi lỗi đều trả null để WebView
 * quay về gọi server như cũ.
 */
object NativeYt {
    private const val TAG = "MuseYt"
    private const val PREFIX = "/api/yt/audio/"
    private const val FIRST_CHUNK = 256L * 1024      // đoạn đầu nhỏ để nhạc lên tiếng nhanh
    private const val CHUNK = 2L * 1024 * 1024
    private const val TTL_MS = 3L * 3600 * 1000      // trần thời gian giữ link nếu link không có expire
    private const val STAGGER_MS = 350L              // so le giữa các client để không bắn cùng lúc
    private const val PIPED_DELAY_MS = 1800L         // Piped chỉ là đường cuối
    private const val RACE_TIMEOUT_MS = 30_000L

    private const val WEB_UA = "Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Mobile Safari/537.36"

    private class YtClient(
        val label: String, val name: String, val hid: String, val ver: String,
        val ua: String, val extra: String, val embed: Boolean = false
    )

    private const val VR_EXTRA = """{"deviceMake":"Oculus","deviceModel":"Quest 3","androidSdkVersion":32,"osName":"Android","osVersion":"12L"}"""
    private const val IOS_EXTRA = """{"deviceMake":"Apple","deviceModel":"iPhone16,2","osName":"iPhone","osVersion":"18.3.2.22D82"}"""

    // Thứ tự = thứ tự bắt đầu. Cái nào cho link tải thử được trước thì thắng.
    private val clients = listOf(
        YtClient("ANDROID_VR 1.60.19", "ANDROID_VR", "28", "1.60.19",
            "com.google.android.apps.youtube.vr.oculus/1.60.19 (Linux; U; Android 12L; eureka-user Build/SQ3A.220605.009.A1) gzip", VR_EXTRA),
        YtClient("ANDROID_VR 1.65.10", "ANDROID_VR", "28", "1.65.10",
            "com.google.android.apps.youtube.vr.oculus/1.65.10 (Linux; U; Android 12L; eureka-user Build/SQ3A.220605.009.A1) gzip", VR_EXTRA),
        YtClient("TV_EMBEDDED 2.0", "TVHTML5_SIMPLY_EMBEDDED_PLAYER", "85", "2.0",
            "Mozilla/5.0 (ChromiumStylePlatform) Cobalt/Version", "{}", embed = true),
        YtClient("IOS 21.26.4", "IOS", "5", "21.26.4",
            "com.google.ios.youtube/21.26.4 (iPhone16,2; U; CPU iOS 18_3_2 like Mac OS X;)", IOS_EXTRA),
        YtClient("IOS 20.10.4", "IOS", "5", "20.10.4",
            "com.google.ios.youtube/20.10.4 (iPhone16,2; U; CPU iOS 18_3_2 like Mac OS X;)", IOS_EXTRA),
        YtClient("TV_SIMPLY 1.0", "TVHTML5_SIMPLY", "75", "1.0",
            "Mozilla/5.0 (ChromiumStylePlatform) Cobalt/Version", "{}")
    )

    private val pipedBases = listOf(
        "https://pipedapi.kavin.rocks",
        "https://api.piped.private.coffee",
        "https://pipedapi.adminforge.de",
        "https://pipedapi.ducks.party"
    )

    private data class Info(val url: String, val size: Long, val mime: String, val ua: String, val expAt: Long)
    private class Res(val info: Info?)

    private val cache = ConcurrentHashMap<String, Info>()
    private val lastErr = ConcurrentHashMap<String, String>()
    private val idRe = Regex("[A-Za-z0-9_-]{11}")
    private val rangeRe = Regex("bytes=(\\d*)-(\\d*)")
    private val expireRe = Regex("[?&]expire=(\\d+)")
    private val totalRe = Regex("/(\\d+)\\s*$")

    /** Lý do lần lấy nhạc gần nhất của bài [id] thất bại (để hiện cho người dùng). Rỗng nếu chưa có lỗi. */
    fun lastError(id: String): String = lastErr[id] ?: ""

    private fun keyOf(id: String, webm: Boolean) = id + "|" + (if (webm) "1" else "0")

    private fun expiryOf(url: String): Long {
        val now = System.currentTimeMillis()
        val cap = now + TTL_MS
        val e = expireRe.find(url)?.groupValues?.get(1)?.toLongOrNull() ?: return cap
        // trừ 5 phút cho chắc; link còn dưới 1 phút thì vẫn dùng được cho lần này
        return minOf(cap, maxOf(now + 60_000L, e * 1000L - 5 * 60_000L))
    }

    private fun resolve(id: String, webm: Boolean, force: Boolean): Info? {
        val key = keyOf(id, webm)
        if (force) cache.remove(key)
        val hit = cache[key]
        if (hit != null && System.currentTimeMillis() < hit.expAt) return hit
        cache.remove(key)
        if (cache.size > 200) cache.clear()

        val errs = ConcurrentHashMap<Int, String>()
        val got = race(id, webm, errs)
        if (got != null) {
            cache[key] = got
            lastErr.remove(id)
            return got
        }
        val msg = errs.entries.sortedBy { it.key }.joinToString(" | ") { it.value }
            .ifEmpty { "không lấy được luồng âm thanh (hết thời gian chờ)" }
        lastErr[id] = msg
        Log.w(TAG, "[$id] thất bại hết: $msg")
        return null
    }

    /** Chạy các client so le nhau, trả về luồng đầu tiên TẢI THỬ được. */
    private fun race(id: String, webm: Boolean, errs: MutableMap<Int, String>): Info? {
        val total = clients.size + 1 // +1 là Piped
        val q = LinkedBlockingQueue<Res>()
        val stop = AtomicBoolean(false)

        clients.forEachIndexed { i, c ->
            thread(isDaemon = true, name = "muse-yt-$i") {
                var r: Info? = null
                try {
                    if (i > 0) Thread.sleep(i * STAGGER_MS)
                    if (!stop.get()) r = verified(viaClient(c, id, webm))
                } catch (e: Exception) {
                    val m = c.label + ": " + (e.message ?: e.javaClass.simpleName)
                    errs[i] = m
                    Log.w(TAG, "[$id] $m")
                }
                q.put(Res(r))
            }
        }
        thread(isDaemon = true, name = "muse-yt-piped") {
            var r: Info? = null
            try {
                Thread.sleep(PIPED_DELAY_MS)
                if (!stop.get()) r = viaPiped(id, webm)
            } catch (e: Exception) {
                val m = "Piped: " + (e.message ?: e.javaClass.simpleName)
                errs[clients.size] = m
                Log.w(TAG, "[$id] $m")
            }
            q.put(Res(r))
        }

        val deadline = System.currentTimeMillis() + RACE_TIMEOUT_MS
        var finished = 0
        try {
            while (finished < total) {
                val left = deadline - System.currentTimeMillis()
                if (left <= 0) break
                val r = q.poll(left, TimeUnit.MILLISECONDS) ?: break
                finished++
                if (r.info != null) return r.info
            }
        } finally {
            stop.set(true)
        }
        return null
    }

    private fun readText(conn: HttpURLConnection): String =
        conn.inputStream.bufferedReader(Charsets.UTF_8).use { it.readText() }

    private fun pickAudio(fmts: org.json.JSONArray, webm: Boolean): JSONObject? {
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
        return best
    }

    /** Hỏi InnerTube bằng một client; ném Exception có lý do nếu không có link. */
    private fun viaClient(c: YtClient, id: String, webm: Boolean): Info {
        val client = JSONObject(c.extra)
            .put("clientName", c.name).put("clientVersion", c.ver).put("hl", "en").put("gl", "VN")
        val ctx = JSONObject().put("client", client)
        if (c.embed) ctx.put("thirdParty", JSONObject().put("embedUrl", "https://www.youtube.com/"))
        val body = JSONObject()
            .put("context", ctx)
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
            val code = conn.responseCode
            if (code != 200) throw Exception("YouTube trả HTTP $code")
            val root = JSONObject(readText(conn))
            val st = root.optJSONObject("playabilityStatus")
            val status = st?.optString("status").orEmpty()
            if (status != "OK") {
                val reason = st?.optString("reason").orEmpty().ifEmpty { "không phát được" }
                throw Exception("$reason ($status)")
            }
            val fmts = root.optJSONObject("streamingData")?.optJSONArray("adaptiveFormats")
                ?: throw Exception("không có danh sách định dạng")
            val f = pickAudio(fmts, webm) ?: throw Exception("không có URL trực tiếp (cần PO token)")
            val url = f.optString("url")
            val size = f.optString("contentLength", "0").toLongOrNull() ?: 0L
            val mime = f.optString("mimeType").substringBefore(';').trim().ifEmpty { "audio/mp4" }
            return Info(url, size, mime, c.ua, expiryOf(url))
        } finally {
            conn.disconnect()
        }
    }

    /** Đường dự phòng: nhờ máy chủ Piped (gọi từ điện thoại) đưa link luồng. Link Piped không gắn IP máy. */
    private fun viaPiped(id: String, webm: Boolean): Info {
        var last = "không có máy chủ nào dùng được"
        for (base in pipedBases) {
            try {
                val conn = URL("$base/streams/$id").openConnection() as HttpURLConnection
                var txt = ""
                var httpCode = 0
                try {
                    conn.connectTimeout = 6000
                    conn.readTimeout = 10000
                    conn.setRequestProperty("User-Agent", WEB_UA)
                    httpCode = conn.responseCode
                    if (httpCode == 200) txt = readText(conn)
                } finally { conn.disconnect() }
                if (httpCode != 200) { last = "$base: HTTP $httpCode"; continue }
                val list = JSONObject(txt).optJSONArray("audioStreams")
                if (list == null) { last = "$base: không có audioStreams"; continue }
                val f = pickAudio(list, webm)
                if (f == null) { last = "$base: không có luồng âm thanh"; continue }
                val url = f.optString("url")
                val size = f.optLong("contentLength", 0L)
                val mime = f.optString("mimeType").substringBefore(';').trim().ifEmpty { "audio/mp4" }
                val info = Info(url, size, mime, WEB_UA, minOf(System.currentTimeMillis() + 30 * 60_000L, expiryOf(url)))
                return verified(info)
            } catch (e: Exception) {
                last = "$base: ${e.message ?: e.javaClass.simpleName}"
            }
        }
        throw Exception(last)
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

    /** Tải thử vài byte đầu: chỉ link thật sự tải được mới qua. Cũng bù tổng dung lượng nếu client không báo. */
    private fun verified(info: Info): Info {
        val conn = openRange(info, 0, 1023) ?: throw Exception("link không tải được (403/hết hạn/cần PO token)")
        try {
            val buf = ByteArray(16)
            val n = conn.inputStream.read(buf)
            if (n <= 0) throw Exception("link trả dữ liệu rỗng")
            var size = info.size
            if (size <= 0L) {
                size = conn.getHeaderField("Content-Range")?.let { totalRe.find(it)?.groupValues?.get(1)?.toLongOrNull() } ?: 0L
            }
            if (size <= 0L) throw Exception("không biết dung lượng luồng")
            return if (size == info.size) info else info.copy(size = size)
        } finally {
            conn.disconnect()
        }
    }

    /** Tải một đoạn (từ byte [start], tối đa CHUNK) thẳng bằng IP điện thoại, không đi qua WebView. Dùng cho nút "Tải xuống". */
    fun fetchChunk(id: String, webm: Boolean, start: Long): AudioChunk? {
        if (!idRe.matches(id)) return null
        for (attempt in 0..1) {
            val info = resolve(id, webm, attempt == 1) ?: return null
            if (start >= info.size) return null
            val end = minOf(info.size - 1, start + CHUNK - 1)
            val conn = openRange(info, start, end)
            if (conn == null) {
                cache.remove(keyOf(id, webm))
                lastErr[id] = "link hết hạn hoặc đổi mạng giữa chừng (tải đoạn $start thất bại)"
                continue
            }
            return try {
                val b = conn.inputStream.use { it.readBytes() }
                if (b.isEmpty()) { lastErr[id] = "nhận đoạn dữ liệu rỗng"; null } else AudioChunk(b, info.size, info.mime)
            } catch (e: Exception) {
                lastErr[id] = "đứt kết nối khi tải đoạn $start: ${e.message ?: e.javaClass.simpleName}"
                cache.remove(keyOf(id, webm))
                null
            } finally { conn.disconnect() }
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
                // Địa chỉ luồng hết hạn / đổi IP (Wi-Fi <-> 4G) / bị chặn: bỏ link, lấy link mới rồi thử tiếp.
                cache.remove(keyOf(id, webm))
                lastErr[id] = "link hết hạn hoặc đổi mạng giữa chừng"
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
