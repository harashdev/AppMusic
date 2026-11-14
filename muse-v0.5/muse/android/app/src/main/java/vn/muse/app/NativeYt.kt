package vn.muse.app

import android.util.Log
import android.webkit.WebResourceRequest
import android.webkit.WebResourceResponse
import org.json.JSONObject
import org.schabi.newpipe.extractor.NewPipe
import org.schabi.newpipe.extractor.downloader.Downloader
import org.schabi.newpipe.extractor.downloader.Request
import org.schabi.newpipe.extractor.downloader.Response
import org.schabi.newpipe.extractor.exceptions.ReCaptchaException
import org.schabi.newpipe.extractor.stream.AudioStream
import org.schabi.newpipe.extractor.stream.DeliveryMethod
import org.schabi.newpipe.extractor.stream.StreamInfo
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
 *  - NewPipeExtractor (thư viện của NewPipe) chạy song song như một nguồn nữa: nó tự giải mã chữ ký/tham số n và đổi client
 *    theo YouTube, nên khi các client viết tay bị chặn thì vẫn có link. Mọi yêu cầu của nó cũng đi bằng IP điện thoại.
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
    private const val PIPED_DELAY_MS = 4500L         // Piped chỉ là đường cuối
    private const val LATE_DELAY_MS = 2500L          // thêm cho client cần PO token (iOS, TV_SIMPLY)
    private const val NEWPIPE_DELAY_MS = 900L        // NewPipeExtractor chờ ANDROID_VR một chút rồi chạy song song
    private const val RACE_TIMEOUT_MS = 30_000L
    // YouTube cho tải ~1MB đầu của link thiếu PO token rồi trả 403. Thử tải ở sâu trong file để loại link đó ngay từ đầu.
    private const val DEEP_PROBE = 1536L * 1024

    private const val WEB_UA = "Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Mobile Safari/537.36"

    private class YtClient(
        val label: String, val name: String, val hid: String, val ver: String,
        val ua: String, val extra: String, val embed: Boolean = false,
        val late: Boolean = false // client cần PO token (iOS, TV_SIMPLY): chỉ để dự phòng, bắt đầu muộn hơn
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
            "com.google.ios.youtube/21.26.4 (iPhone16,2; U; CPU iOS 18_3_2 like Mac OS X;)", IOS_EXTRA, late = true),
        YtClient("IOS 20.10.4", "IOS", "5", "20.10.4",
            "com.google.ios.youtube/20.10.4 (iPhone16,2; U; CPU iOS 18_3_2 like Mac OS X;)", IOS_EXTRA, late = true),
        YtClient("TV_SIMPLY 1.0", "TVHTML5_SIMPLY", "75", "1.0",
            "Mozilla/5.0 (ChromiumStylePlatform) Cobalt/Version", "{}", late = true)
    )

    private val pipedBases = listOf(
        "https://pipedapi.kavin.rocks",
        "https://api.piped.private.coffee",
        "https://pipedapi.adminforge.de",
        "https://pipedapi.ducks.party"
    )

    private data class Info(val url: String, val size: Long, val mime: String, val ua: String, val expAt: Long, val src: String = "")
    private class Res(val info: Info?)

    private val cache = ConcurrentHashMap<String, Info>()
    private val lastErr = ConcurrentHashMap<String, String>()
    private val idRe = Regex("[A-Za-z0-9_-]{11}")
    private val rangeRe = Regex("bytes=(\\d*)-(\\d*)")
    private val expireRe = Regex("[?&]expire=(\\d+)")
    private val totalRe = Regex("/(\\d+)\\s*$")

    // Nguồn (nhãn client / "Piped") đã làm đứt tải bài này: lần lấy link kế tiếp bỏ qua nguồn đó.
    // Mỗi mục tự hết hạn sau BAD_TTL_MS để một lỗi thoáng qua không loại nguồn đó mãi mãi.
    private val badSrc = ConcurrentHashMap<String, ConcurrentHashMap<String, Long>>()
    private const val BAD_TTL_MS = 10 * 60_000L

    private fun badSet(id: String): Set<String> {
        val m = badSrc[id] ?: return emptySet()
        val now = System.currentTimeMillis()
        m.entries.removeAll { now - it.value > BAD_TTL_MS }
        return m.keys.toSet()
    }

    /** Lý do lần lấy nhạc gần nhất của bài [id] thất bại (để hiện cho người dùng). Rỗng nếu chưa có lỗi. */
    fun lastError(id: String): String = lastErr[id] ?: ""

    /** Cho nơi khác (MuseBridge) ghi lại lý do lỗi thật, thay vì câu chung chung. */
    fun setError(id: String, msg: String) { lastErr[id] = msg }

    private fun markBad(id: String, src: String) {
        if (src.isEmpty()) return
        badSrc.getOrPut(id) { ConcurrentHashMap<String, Long>() }[src] = System.currentTimeMillis()
        cache.keys.removeAll { it.startsWith("$id|") }
    }

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
        val total = clients.size + 2 // +2 là NewPipe và Piped
        val q = LinkedBlockingQueue<Res>()
        val stop = AtomicBoolean(false)
        // Nếu mọi nguồn đều từng làm đứt tải bài này thì cho thử lại tất cả từ đầu.
        val bad: Set<String> = badSet(id).let { if (it.size >= total) { badSrc.remove(id); emptySet() } else it }

        clients.forEachIndexed { i, c ->
            thread(isDaemon = true, name = "muse-yt-$i") {
                var r: Info? = null
                try {
                    if (c.label in bad) throw Exception("bỏ qua (link lần trước bị đứt giữa chừng)")
                    // Client cần PO token chỉ là dự phòng: chờ thêm để client không cần token (ANDROID_VR, TV) kịp trả lời trước.
                    val wait = i * STAGGER_MS + (if (c.late) LATE_DELAY_MS else 0L)
                    if (wait > 0) Thread.sleep(wait)
                    if (!stop.get()) r = verified(viaClient(c, id, webm))
                } catch (e: Exception) {
                    val m = c.label + ": " + (e.message ?: e.javaClass.simpleName)
                    errs[i] = m
                    Log.w(TAG, "[$id] $m")
                }
                q.put(Res(r))
            }
        }
        // NewPipeExtractor: tự theo kịp thay đổi của YouTube. Chạy song song, nhưng chờ một chút để nếu client nhanh
        // (ANDROID_VR) đã có link dùng được thì khỏi tải thêm trang YouTube + player JS.
        thread(isDaemon = true, name = "muse-yt-newpipe") {
            var r: Info? = null
            try {
                if ("NewPipe" in bad) throw Exception("bỏ qua (link lần trước bị đứt giữa chừng)")
                Thread.sleep(NEWPIPE_DELAY_MS)
                if (!stop.get()) r = verified(viaNewPipe(id, webm))
            } catch (e: Throwable) { // cả lỗi nạp lớp/thiếu thư viện cũng không được làm sập cuộc đua
                val m = "NewPipe: " + (e.message ?: e.javaClass.simpleName)
                errs[clients.size + 1] = m
                Log.w(TAG, "[$id] $m")
            }
            q.put(Res(r))
        }
        thread(isDaemon = true, name = "muse-yt-piped") {
            var r: Info? = null
            try {
                if ("Piped" in bad) throw Exception("bỏ qua (link lần trước bị đứt giữa chừng)")
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
            return Info(url, size, mime, c.ua, expiryOf(url), c.label)
        } finally {
            conn.disconnect()
        }
    }

    // ---- NewPipeExtractor ------------------------------------------------------------------------------------------------

    /** Cầu nối HTTP cho NewPipeExtractor: mọi yêu cầu của nó đi thẳng từ điện thoại (IP máy), không qua server. */
    private class NpDownloader : Downloader() {
        override fun execute(request: Request): Response {
            val conn = URL(request.url()).openConnection() as HttpURLConnection
            try {
                conn.requestMethod = request.httpMethod()
                conn.connectTimeout = 10_000
                conn.readTimeout = 20_000
                conn.instanceFollowRedirects = true
                conn.setRequestProperty("User-Agent", WEB_UA)
                for ((k, vs) in request.headers()) {
                    // Bỏ Accept-Encoding: để HttpURLConnection tự xin gzip và tự giải nén cho mình.
                    if (k.equals("Accept-Encoding", true)) continue
                    vs.forEachIndexed { i, v -> if (i == 0) conn.setRequestProperty(k, v) else conn.addRequestProperty(k, v) }
                }
                val body = request.dataToSend()
                if (body != null) {
                    conn.doOutput = true
                    conn.outputStream.use { it.write(body) }
                }
                val code = conn.responseCode
                if (code == 429) throw ReCaptchaException("YouTube yêu cầu xác minh (HTTP 429)", request.url())
                val text = (if (code >= 400) conn.errorStream else conn.inputStream)
                    ?.bufferedReader(Charsets.UTF_8)?.use { it.readText() } ?: ""
                val headers = HashMap<String, List<String>>()
                for ((k, v) in conn.headerFields) if (k != null) headers[k] = v
                return Response(code, conn.responseMessage ?: "", headers, text, conn.url.toString())
            } finally {
                conn.disconnect()
            }
        }
    }

    @Volatile private var npReady = false
    private fun npInit() {
        if (npReady) return
        synchronized(this) {
            if (npReady) return
            NewPipe.init(NpDownloader())
            npReady = true
        }
    }

    /** Link googlevideo gắn với client tạo ra nó (tham số c=): dùng đúng User-Agent của client đó khi tải, như NewPipe làm. */
    private fun uaForUrl(url: String): String {
        val c = Regex("[?&]c=([A-Z0-9_]+)").find(url)?.groupValues?.get(1).orEmpty()
        val want = when {
            c == "ANDROID_VR" -> "ANDROID_VR"
            c == "IOS" -> "IOS"
            c.startsWith("TV") -> "TVHTML5_SIMPLY_EMBEDDED_PLAYER"
            else -> ""
        }
        return clients.firstOrNull { it.name == want }?.ua ?: WEB_UA
    }

    /** Lấy link âm thanh bằng NewPipeExtractor (tự giải mã chữ ký / tham số n và chọn client còn chạy). Ném Exception có lý do nếu không được. */
    private fun viaNewPipe(id: String, webm: Boolean): Info {
        npInit()
        val info = StreamInfo.getInfo("https://www.youtube.com/watch?v=$id")
        val list = info.audioStreams.filter {
            it.isUrl && it.deliveryMethod == DeliveryMethod.PROGRESSIVE_HTTP && !it.content.isNullOrEmpty()
        }
        if (list.isEmpty()) throw Exception("NewPipe: không có luồng âm thanh tải trực tiếp được")
        val want = if (webm) "audio/webm" else "audio/mp4"
        fun mimeOf(s: AudioStream): String = s.format?.mimeType.orEmpty()
        val best = list.maxByOrNull { (if (mimeOf(it).startsWith(want)) 1_000_000 else 0) + it.averageBitrate }
            ?: throw Exception("NewPipe: không chọn được luồng")
        val url = best.content
        val mime = mimeOf(best).ifEmpty { want }
        // Dung lượng để 0: verified() sẽ lấy từ Content-Range khi tải thử.
        return Info(url, 0L, mime, uaForUrl(url), expiryOf(url), "NewPipe")
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
                val info = Info(url, size, mime, WEB_UA, minOf(System.currentTimeMillis() + 30 * 60_000L, expiryOf(url)), "Piped")
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
            val ok = if (size == info.size) info else info.copy(size = size)
            // Link thiếu PO token qua được 1KB đầu nhưng chết ở sâu hơn: thử thêm một đoạn ở ~1.5MB và một đoạn gần cuối file.
            for (at in listOf(DEEP_PROBE, size - 64 * 1024).distinct()) {
                if (at <= 1024L || at + 16 >= size) continue
                val c2 = openRange(ok, at, at + 15)
                    ?: throw Exception("link chỉ tải được phần đầu, bị chặn từ byte $at (thiếu PO token)")
                try {
                    if (c2.inputStream.read(ByteArray(16)) <= 0) throw Exception("link trả dữ liệu rỗng ở byte $at")
                } finally {
                    c2.disconnect()
                }
            }
            return ok
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
                markBad(id, info.src) // lần lấy link kế tiếp dùng client khác
                lastErr[id] = "${info.src.ifEmpty { "link" }} bị YouTube từ chối ở byte $start (hết hạn, đổi mạng hoặc thiếu PO token)"
                continue
            }
            return try {
                val b = conn.inputStream.use { it.readBytes() }
                if (b.isEmpty()) { lastErr[id] = "nhận đoạn dữ liệu rỗng"; markBad(id, info.src); null } else AudioChunk(b, info.size, info.mime)
            } catch (e: Exception) {
                lastErr[id] = "đứt kết nối khi tải đoạn $start: ${e.message ?: e.javaClass.simpleName}"
                markBad(id, info.src)
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
