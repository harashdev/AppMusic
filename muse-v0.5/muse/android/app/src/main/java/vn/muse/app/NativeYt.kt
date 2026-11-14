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
import java.io.IOException
import java.io.InputStream
import java.net.HttpURLConnection
import java.net.InetAddress
import java.net.ServerSocket
import java.net.Socket
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
 *  - Không còn Piped: link Piped gắn với IP máy chủ Piped chứ không phải IP điện thoại. Giờ 100% là IP của người dùng.
 *  - Hết hạn link lấy từ tham số expire của chính link, không đoán 3 giờ.
 *  - Lỗi không còn bị nuốt: lý do từng client được ghi vào logcat (tag MuseYt) và trả về qua [lastError] để giao diện hiện ra.
 *
 * PHÁT NHẠC: NativeYt tự mở một máy chủ HTTP nhỏ ở 127.0.0.1:<cổng riêng> ([ensureProxy]). Trang web phát thẳng từ địa chỉ đó
 * (MuseNative.audioUrl) nên thẻ <audio> dùng mạng của Chromium như phát một file bình thường, KHÔNG còn đi qua
 * shouldInterceptRequest/WebResourceResponse (thứ từng cắt luồng thành từng lát 512KB–2MB rồi đứt giữa chừng gây giật/dừng).
 * Proxy trả MỘT phản hồi cho cả đoạn được yêu cầu, tự tải nối tiếp từng đoạn từ googlevideo bằng IP điện thoại; nếu link
 * đứt/hết hạn/đổi mạng giữa chừng thì tự lấy link mới và nối tiếp đúng byte đó, trình phát không hề biết.
 */
object NativeYt {
    private const val TAG = "MuseYt"
    private const val PREFIX = "/api/yt/audio/"
    private const val FIRST_CHUNK = 512L * 1024      // đoạn đầu 512 KB: đủ buffer cho kết nối chậm/server free-tier
    private const val CHUNK = 2L * 1024 * 1024
    private const val TTL_MS = 3L * 3600 * 1000      // trần thời gian giữ link nếu link không có expire
    private const val STAGGER_MS = 350L              // so le giữa các client để không bắn cùng lúc
    private const val UP_CHUNK = 1L * 1024 * 1024    // mỗi lần xin googlevideo 1MB (proxy nối các đoạn lại thành một luồng)
    private const val MAX_FAILS = 4                  // số lần đứt liên tiếp trước khi bỏ cuộc cho một kết nối
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

    private data class Info(val url: String, val size: Long, val mime: String, val ua: String, val expAt: Long, val src: String = "")
    private class Res(val info: Info?)

    private val cache = ConcurrentHashMap<String, Info>()
    private val lastErr = ConcurrentHashMap<String, String>()
    private val idRe = Regex("[A-Za-z0-9_-]{11}")
    private val rangeRe = Regex("bytes=(\\d*)-(\\d*)")
    private val expireRe = Regex("[?&]expire=(\\d+)")
    private val totalRe = Regex("/(\\d+)\\s*$")

    // Nguồn (nhãn client / "NewPipe") đã làm đứt tải bài này: lần lấy link kế tiếp bỏ qua nguồn đó.
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

    private val locks = ConcurrentHashMap<String, Any>()

    /**
     * Lấy link luồng cho bài [id]. [stale] là link vừa bị hỏng: nếu cache đã có link khác (luồng khác vừa làm mới xong) thì dùng luôn,
     * không đua lại với YouTube. Mỗi bài chỉ có một cuộc đua tại một thời điểm, các luồng khác chờ rồi lấy kết quả.
     */
    private fun resolve(id: String, webm: Boolean, stale: Info? = null): Info? {
        val key = keyOf(id, webm)
        return synchronized(locks.getOrPut(key) { Any() }) {
            val hit = cache[key]
            if (hit != null && hit !== stale && System.currentTimeMillis() < hit.expAt) return hit
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
            null
        }
    }

    /** Lấy sẵn link của bài sắp phát (chạy nền) để lúc chuyển bài không phải chờ cuộc đua + tải thử. */
    fun prefetch(id: String, webm: Boolean) {
        if (!idRe.matches(id)) return
        val hit = cache[keyOf(id, webm)]
        if (hit != null && System.currentTimeMillis() < hit.expAt - 10 * 60_000L) return
        thread(isDaemon = true, name = "muse-yt-prefetch") { runCatching { resolve(id, webm) } }
    }

    /** Chạy các client so le nhau, trả về luồng đầu tiên TẢI THỬ được. */
    private fun race(id: String, webm: Boolean, errs: MutableMap<Int, String>): Info? {
        val total = clients.size + 1 // +1 là NewPipe
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
        var stale: Info? = null
        for (attempt in 0..1) {
            val info = resolve(id, webm, stale) ?: return null
            if (start >= info.size) return null
            val end = minOf(info.size - 1, start + CHUNK - 1)
            val conn = openRange(info, start, end)
            if (conn == null) {
                cache.remove(keyOf(id, webm))
                markBad(id, info.src) // lần lấy link kế tiếp dùng client khác
                lastErr[id] = "${info.src.ifEmpty { "link" }} bị YouTube từ chối ở byte $start (hết hạn, đổi mạng hoặc thiếu PO token)"
                stale = info
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

    // ---- Máy chủ HTTP cục bộ (127.0.0.1) -----------------------------------------------------------------------------------

    @Volatile private var proxySock: ServerSocket? = null
    private val proxyLock = Any()

    /** Mở (nếu chưa mở) máy chủ cục bộ và trả về cổng. Chỉ nhận kết nối từ chính điện thoại (loopback). */
    fun ensureProxy(): Int {
        proxySock?.let { if (!it.isClosed) return it.localPort }
        return synchronized(proxyLock) {
            proxySock?.let { if (!it.isClosed) return it.localPort }
            val ss = ServerSocket(0, 32, InetAddress.getByName("127.0.0.1"))
            proxySock = ss
            thread(isDaemon = true, name = "muse-yt-proxy") {
                while (!ss.isClosed) {
                    val s = try { ss.accept() } catch (e: Exception) { break }
                    thread(isDaemon = true, name = "muse-yt-conn") { serve(s) }
                }
                synchronized(proxyLock) { if (proxySock === ss) proxySock = null }
            }
            ss.localPort
        }
    }

    fun shutdown() {
        synchronized(proxyLock) { runCatching { proxySock?.close() }; proxySock = null }
    }

    /** Địa chỉ để thẻ <audio> phát thẳng; rỗng nếu không mở được máy chủ cục bộ. */
    fun audioUrl(id: String, webm: Boolean): String {
        if (!idRe.matches(id)) return ""
        return try { "http://127.0.0.1:${ensureProxy()}/a/$id?f=${if (webm) "webm" else "mp4"}" } catch (e: Exception) { "" }
    }

    private fun readLine(inp: InputStream): String? {
        val sb = StringBuilder()
        while (true) {
            val c = inp.read()
            if (c < 0) return if (sb.isEmpty()) null else sb.toString()
            if (c == '\n'.code) return sb.toString().trimEnd('\r')
            if (sb.length > 8192) return null
            sb.append(c.toChar())
        }
    }

    private fun sendHead(out: java.io.OutputStream, code: Int, reason: String, hdr: Map<String, String>) {
        val sb = StringBuilder("HTTP/1.1 $code $reason\r\n")
        for ((k, v) in hdr) sb.append(k).append(": ").append(v).append("\r\n")
        sb.append("Connection: close\r\n\r\n")
        out.write(sb.toString().toByteArray(Charsets.ISO_8859_1))
        out.flush()
    }

    private val corsHdr = linkedMapOf(
        "Access-Control-Allow-Origin" to "*",
        "Access-Control-Allow-Headers" to "Range",
        "Access-Control-Expose-Headers" to "Content-Range, Content-Length, Accept-Ranges",
        "Cache-Control" to "no-store"
    )

    private fun fail(out: java.io.OutputStream, code: Int, reason: String, msg: String) {
        val body = msg.toByteArray(Charsets.UTF_8)
        val h = LinkedHashMap(corsHdr)
        h["Content-Type"] = "text/plain; charset=utf-8"
        h["Content-Length"] = body.size.toString()
        sendHead(out, code, reason, h)
        out.write(body); out.flush()
    }

    /** Xử lý một kết nối từ trình phát: GET /a/<id>?f=webm|mp4 kèm Range. Trả MỘT phản hồi liền mạch cho cả đoạn được xin. */
    private fun serve(s: Socket) {
        try {
            s.soTimeout = 20_000
            s.tcpNoDelay = true
            val inp = s.getInputStream().buffered()
            val out = s.getOutputStream().buffered(64 * 1024)
            val reqLine = readLine(inp) ?: return
            val parts = reqLine.split(' ')
            if (parts.size < 2) return
            val method = parts[0].uppercase()
            var range: String? = null
            while (true) {
                val l = readLine(inp) ?: return
                if (l.isEmpty()) break
                val i = l.indexOf(':')
                if (i > 0 && l.substring(0, i).trim().equals("Range", true)) range = l.substring(i + 1).trim()
            }
            s.soTimeout = 0
            if (method == "OPTIONS") {
                val h = LinkedHashMap(corsHdr); h["Content-Length"] = "0"
                sendHead(out, 204, "No Content", h); return
            }
            if (method != "GET" && method != "HEAD") { fail(out, 405, "Method Not Allowed", "chỉ hỗ trợ GET/HEAD"); return }
            val target = parts[1]
            val path = target.substringBefore('?')
            val query = target.substringAfter('?', "")
            val id = path.removePrefix("/a/").removePrefix("/api/yt/audio/")
            if (!idRe.matches(id)) { fail(out, 404, "Not Found", "id không hợp lệ"); return }
            val webm = query.split('&').any { it == "f=webm" }

            var info: Info = resolve(id, webm)
                ?: run { fail(out, 503, "Service Unavailable", lastError(id).ifEmpty { "không lấy được luồng âm thanh trên máy" }); return }

            var start = 0L
            var endReq = Long.MAX_VALUE
            if (range != null) {
                val m = rangeRe.find(range)
                if (m != null) {
                    val a = m.groupValues[1].toLongOrNull()
                    val b = m.groupValues[2].toLongOrNull()
                    if (a == null && b != null) { start = maxOf(0L, info.size - b); endReq = info.size - 1 }
                    else { start = a ?: 0L; endReq = b ?: Long.MAX_VALUE }
                }
            }
            if (start >= info.size) {
                val h = LinkedHashMap(corsHdr); h["Content-Range"] = "bytes */${info.size}"; h["Content-Length"] = "0"
                sendHead(out, 416, "Range Not Satisfiable", h); return
            }
            val end = minOf(endReq, info.size - 1)
            val total = info.size
            val partial = range != null
            val h = LinkedHashMap(corsHdr)
            h["Content-Type"] = info.mime
            h["Accept-Ranges"] = "bytes"
            h["Content-Length"] = (end - start + 1).toString()
            if (partial) h["Content-Range"] = "bytes $start-$end/$total"
            sendHead(out, if (partial) 206 else 200, if (partial) "Partial Content" else "OK", h)
            if (method == "HEAD") { out.flush(); return }

            var pos = start
            var fails = 0
            val buf = ByteArray(32 * 1024)
            while (pos <= end) {
                val chunkEnd = minOf(end, pos + UP_CHUNK - 1)
                val conn = openRange(info, pos, chunkEnd)
                var broken = conn == null
                if (conn != null) {
                    try {
                        val up = conn.inputStream
                        var left = chunkEnd - pos + 1
                        while (left > 0) {
                            val n = try { up.read(buf, 0, minOf(buf.size.toLong(), left).toInt()) } catch (e: IOException) { -2 }
                            if (n == -2) { broken = true; break }
                            if (n < 0) { broken = true; break }
                            out.write(buf, 0, n) // lỗi ghi = trình phát đã đóng kết nối (tua/đổi bài): thoát luôn qua catch bên ngoài
                            pos += n; left -= n
                        }
                    } finally { conn.disconnect() }
                }
                if (!broken) { fails = 0; continue }
                // Đứt giữa chừng (link chết, đổi Wi-Fi <-> 4G, YouTube ngắt): thử lại cùng link, rồi mới đổi link, nối tiếp đúng byte [pos].
                out.flush()
                if (++fails > MAX_FAILS) { lastErr[id] = "đứt kết nối nhiều lần ở byte $pos"; return }
                Log.w(TAG, "[$id] đứt ở byte $pos (lần $fails), nguồn ${info.src}")
                if (fails >= 2) {
                    cache.remove(keyOf(id, webm))
                    markBad(id, info.src)
                    val n = resolve(id, webm, info)
                    if (n == null) return
                    if (n.size != total) { lastErr[id] = "link mới có dung lượng khác (${n.size} ≠ $total)"; return }
                    info = n
                } else Thread.sleep(300)
            }
            out.flush()
        } catch (e: Exception) {
            // trình phát đóng kết nối: bình thường
        } finally {
            runCatching { s.close() }
        }
    }

    /**
     * Dùng trong shouldInterceptRequest cho bản web cũ vẫn gọi /api/yt/audio/<id>. Không tự nối googlevideo nữa: chuyển yêu cầu
     * tới máy chủ cục bộ ở trên. Trả null = không xử lý được (LocalAudio sẽ trả lỗi chứ không rơi về server, để giữ 100% IP máy).
     */
    fun handle(req: WebResourceRequest): WebResourceResponse? {
        if (!req.method.equals("GET", true)) return null
        val path = req.url.encodedPath ?: return null
        if (!path.startsWith(PREFIX)) return null
        val id = path.removePrefix(PREFIX)
        if (!idRe.matches(id)) return null
        val webm = req.url.getQueryParameter("f") == "webm"
        val port = try { ensureProxy() } catch (e: Exception) { return null }
        return try {
            val c = URL("http://127.0.0.1:$port/a/$id?f=${if (webm) "webm" else "mp4"}").openConnection() as HttpURLConnection
            c.connectTimeout = 3000
            c.readTimeout = 60_000
            (req.requestHeaders["Range"] ?: req.requestHeaders["range"])?.let { c.setRequestProperty("Range", it) }
            val code = c.responseCode
            if (code != 200 && code != 206) { c.disconnect(); return null }
            val mime = c.contentType?.substringBefore(';')?.trim().takeUnless { it.isNullOrEmpty() } ?: "audio/mp4"
            val headers = HashMap<String, String>()
            for (k in listOf("Content-Range", "Accept-Ranges", "Content-Length", "Cache-Control")) c.getHeaderField(k)?.let { headers[k] = it }
            headers["Access-Control-Allow-Origin"] = "*"
            WebResourceResponse(mime, null, code, if (code == 206) "Partial Content" else "OK", headers, c.inputStream)
        } catch (e: Exception) { null }
    }
}
