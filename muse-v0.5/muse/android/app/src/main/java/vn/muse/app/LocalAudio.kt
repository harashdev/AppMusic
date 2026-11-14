package vn.muse.app

import android.content.Context
import android.webkit.WebResourceRequest
import android.webkit.WebResourceResponse
import java.io.ByteArrayInputStream
import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.File
import java.net.URLEncoder
import java.security.MessageDigest
import java.net.HttpURLConnection
import java.net.InetSocketAddress
import java.net.ServerSocket
import java.net.Socket
import java.net.URL
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.concurrent.thread

/** Một đoạn nhạc đã tải xong: dữ liệu, tổng dung lượng cả bài, định dạng. */
class AudioChunk(val bytes: ByteArray, val total: Long, val mime: String)

/**
 * Lấy dữ liệu YouTube (nhạc, tìm kiếm, album, nghệ sĩ) ngay trên điện thoại (IP nhà), không đi qua server.
 *
 * Tiến trình `libmuse_audio.so` (chính là muse-server build cho Android, chạy với AUDIO_ONLY=1) lắng nghe ở 127.0.0.1.
 * WebView vẫn gọi `/api/yt/...` như cũ; [handle] chặn đúng các yêu cầu đó và chuyển tới tiến trình cục bộ,
 * nên giao diện web không phải sửa gì. Nếu file không có trong APK (vd. máy 32-bit) hoặc cục bộ lỗi thì trả null:
 * WebView tự gọi server như trước. Mọi thứ khác (tài khoản, bạn bè, phòng nghe chung, video MV...) không bị đụng tới.
 */
object LocalAudio {
    private const val LIB = "libmuse_audio.so"
    // Mọi route YouTube đều lấy bằng IP nhà của điện thoại (nhạc, tìm kiếm, album, nghệ sĩ...), không đi qua server.
    // Không gồm /api/yt/video/ vì cần yt-dlp (không có trên Android); cái đó vẫn do server lo.
    private val PREFIXES = listOf(
        "/api/yt/audio/", "/api/yt/search", "/api/yt/browse", "/api/yt/similar",
        "/api/yt/fresh", "/api/yt/artists", "/api/yt/best-artists", "/api/yt/this-is",
    )
    @Volatile private var serverBase = ""

    @Volatile private var proc: Process? = null
    @Volatile private var port = 0
    private val lock = Any()

    /** Gọi một lần khi mở app (chạy nền, không chặn giao diện). */
    fun start(ctx: Context, server: String = "") {
        serverBase = server.trim().trimEnd('/')
        appCtx = ctx.applicationContext
        thread(isDaemon = true, name = "muse-yt-proxy-boot") { runCatching { NativeYt.ensureProxy() } } // proxy phát nhạc IP máy: mở sẵn
        thread(isDaemon = true, name = "muse-yt-warm") { runCatching { NativeYt.warmUp() } } // làm nóng kết nối YouTube trước bài đầu tiên
        thread(isDaemon = true, name = "muse-local-audio") { ensureRunning(ctx.applicationContext) }
    }

    fun stop() {
        synchronized(lock) {
            runCatching { proc?.destroy() }
            proc = null; port = 0
        }
        NativeYt.shutdown()
    }

    private var appCtx: Context? = null

    private fun ensureRunning(ctx: Context): Boolean {
        appCtx = ctx
        synchronized(lock) {
            val p = proc
            if (p != null && p.isAlive && port > 0) return true
            val bin = File(ctx.applicationInfo.nativeLibraryDir, LIB)
            if (!bin.exists()) return false
            val free = runCatching { ServerSocket(0).use { it.localPort } }.getOrNull() ?: return false
            return try {
                val pb = ProcessBuilder(bin.absolutePath).redirectErrorStream(true)
                pb.environment().apply {
                    put("AUDIO_ONLY", "1")
                    // Để tiến trình cục bộ hỏi server chính xem tài khoản có bật "nội dung nhạy cảm" không.
                    if (serverBase.isNotBlank()) put("GATEWAY_REMOTE", serverBase)
                    put("PORT", free.toString())
                    put("HOME", ctx.filesDir.absolutePath)
                    put("TMPDIR", ctx.cacheDir.absolutePath)
                }
                pb.directory(ctx.filesDir)
                val np = pb.start()
                // Phải đọc hết đầu ra của tiến trình, không thì nó bị nghẽn khi buffer đầy.
                thread(isDaemon = true, name = "muse-local-audio-log") {
                    runCatching { np.inputStream.bufferedReader().forEachLine { /* bỏ qua */ } }
                }
                proc = np; port = free
                // Chờ cổng mở (tối đa ~3 giây).
                var ok = false
                for (i in 0 until 30) {
                    if (!np.isAlive) break
                    ok = runCatching { Socket().use { it.connect(InetSocketAddress("127.0.0.1", free), 150); true } }.getOrDefault(false)
                    if (ok) break
                    Thread.sleep(100)
                }
                if (!ok) { runCatching { np.destroy() }; proc = null; port = 0 }
                ok
            } catch (e: Exception) { proc = null; port = 0; false }
        }
    }

    /**
     * Tải một đoạn nhạc (từ byte [start]) thẳng từ tiến trình cục bộ, không đi qua WebView. Dùng cho nút "Tải xuống":
     * fetch() trong WebView có thể nhận về body rỗng với phản hồi do shouldInterceptRequest tạo ra.
     * Chạy ở luồng nền. Trả null nếu không có tiến trình cục bộ hoặc không lấy được.
     */
    fun fetchChunk(id: String, fmt: String, start: Long): AudioChunk? {
        val ctx = appCtx ?: return null
        if (!ensureRunning(ctx)) return null
        return try {
            val c = URL("http://127.0.0.1:$port/api/yt/audio/${URLEncoder.encode(id, "UTF-8")}?f=${if (fmt == "webm") "webm" else "mp4"}")
                .openConnection() as HttpURLConnection
            try {
                c.connectTimeout = 3000
                c.readTimeout = 45000
                c.setRequestProperty("Range", "bytes=$start-")
                val code = c.responseCode
                if (code != 206 && !(code == 200 && start == 0L)) return null
                val b = c.inputStream.use { it.readBytes() }
                if (b.isEmpty()) return null
                val total = c.getHeaderField("Content-Range")?.substringAfter('/')?.trim()?.toLongOrNull()
                    ?: (if (code == 200) b.size.toLong() else return null)
                val mime = c.contentType?.substringBefore(';')?.trim().takeUnless { it.isNullOrEmpty() } ?: "audio/mp4"
                AudioChunk(b, total, mime)
            } finally { c.disconnect() }
        } catch (e: Exception) { null }
    }

    /** Dùng trong WebViewClient.shouldInterceptRequest (chạy ở luồng nền). Trả null = để WebView gọi server như bình thường. */
    fun handle(req: WebResourceRequest): WebResourceResponse? {
        val path = req.url.encodedPath.orEmpty()
        if (path.startsWith("/api/yt/audio/")) {
            // Nhạc: NativeYt (NewPipe + InnerTube, qua proxy 127.0.0.1) trước, rồi tiến trình libmuse_audio.so. KHÔNG rơi về server:
            // link của server gắn IP server, đúng thứ gây ngắt/giật. Cả hai lỗi thì báo 503 để trình phát tự thử lại.
            return NativeYt.handle(req) ?: handleLocal(req) ?: unavailable()
        }
        return handleLocal(req) // search/browse...: tiến trình cục bộ, lỗi thì null để server lo
    }

    private fun unavailable() = WebResourceResponse(
        "text/plain", "UTF-8", 503, "Service Unavailable",
        mapOf("Access-Control-Allow-Origin" to "*", "Cache-Control" to "no-store"),
        ByteArrayInputStream(ByteArray(0))
    )

    private fun handleLocal(req: WebResourceRequest): WebResourceResponse? {
        if (!req.method.equals("GET", true)) return null
        val path = req.url.encodedPath ?: return null
        if (PREFIXES.none { path.startsWith(it) }) return null
        val ctx = appCtx ?: return null
        if (!ensureRunning(ctx)) return null
        val q = req.url.encodedQuery?.let { "?$it" } ?: ""
        return try {
            val c = URL("http://127.0.0.1:$port$path$q").openConnection() as HttpURLConnection
            c.connectTimeout = 3000
            c.readTimeout = 45000
            req.requestHeaders["Range"]?.let { c.setRequestProperty("Range", it) }
            req.requestHeaders["Authorization"]?.let { c.setRequestProperty("Authorization", it) }
            val code = c.responseCode
            // Cục bộ không lấy được (YouTube chặn, không có mạng...): để server thử, đừng báo lỗi cho trình phát.
            if (code != 200 && code != 206) { c.disconnect(); return null }
            val mime = c.contentType?.substringBefore(';')?.trim().takeUnless { it.isNullOrEmpty() } ?: "audio/mp4"
            val headers = HashMap<String, String>()
            for (k in listOf("Content-Range", "Accept-Ranges", "Content-Length", "Cache-Control")) {
                c.getHeaderField(k)?.let { headers[k] = it }
            }
            val reason = if (code == 206) "Partial Content" else "OK"
            WebResourceResponse(mime, null, code, reason, headers, c.inputStream)
        } catch (e: Exception) {
            null
        }
    }

    // ------------------------------------------------------------------ ảnh bìa

    private val imgHost = Regex("^(i\\d?\\.ytimg\\.com|img\\.youtube\\.com|[\\w-]+\\.(googleusercontent|ggpht)\\.com)$")
    private val badThumb = Regex("^(https://[^/]+/vi(?:_webp)?/[\\w-]{11}/)(hq720|maxresdefault|sddefault)(\\.(?:jpg|webp))(.*)$")

    private class Img(val mime: String, val bytes: ByteArray)
    private class Fetched(val idx: Int, val code: Int, val img: Img?)

    // RAM (~8MB) đứng trước đĩa: ảnh đã xem rồi hiện ngay, không đọc đĩa, không hỏi mạng.
    private val ramImgs = object : android.util.LruCache<String, Img>(8 * 1024 * 1024) {
        override fun sizeOf(key: String, value: Img): Int = value.bytes.size + 64
    }
    private val imgHdr = mapOf("Access-Control-Allow-Origin" to "*", "Cache-Control" to "max-age=86400")

    private fun sha1(s: String) = MessageDigest.getInstance("SHA-1").digest(s.toByteArray()).joinToString("") { "%02x".format(it) }

    private fun imgResp(i: Img) = WebResourceResponse(i.mime, null, 200, "OK", imgHdr, ByteArrayInputStream(i.bytes))

    private fun fetchImg(idx: Int, url: String, ua: String?, timeoutMs: Int): Fetched = try {
        val c = URL(url).openConnection() as HttpURLConnection
        try {
            c.connectTimeout = timeoutMs
            c.readTimeout = timeoutMs + 3000
            if (!ua.isNullOrBlank()) c.setRequestProperty("User-Agent", ua)
            val code = c.responseCode
            val ct = c.contentType?.substringBefore(';')?.trim().orEmpty()
            if (code == 200 && ct.startsWith("image/")) {
                val b = c.inputStream.use { it.readBytes() }
                if (b.isNotEmpty() && b.size < 8 * 1024 * 1024) Fetched(idx, code, Img(ct, b)) else Fetched(idx, 0, null)
            } else Fetched(idx, code, null)
        } finally { c.disconnect() }
    } catch (e: Exception) { Fetched(idx, 0, null) }

    /**
     * Tải ảnh: nguồn gốc (IP điện thoại) chạy ngay; các nguồn dự phòng (wsrv.nl, DuckDuckGo, weserv.nl, server) chỉ bắt đầu
     * SAU KHI nguồn gốc xong mà lỗi/treo (hoặc sau một khoảng ngắn), và chạy song song, cái nào xong trước thắng.
     * Nguồn gốc trả 404/410 nghĩa là ảnh không tồn tại: trả null ngay (không hỏi proxy, vì proxy cũng sẽ 404 và chỉ tốn thời gian).
     */
    private fun raceImg(url: String, ua: String?, serverBase: String): Img? {
        val enc = URLEncoder.encode(url, "UTF-8")
        val urls = ArrayList<String>()
        urls.add(url)
        urls.add("https://wsrv.nl/?url=$enc")
        urls.add("https://external-content.duckduckgo.com/iu/?u=$enc&f=1")
        urls.add("https://images.weserv.nl/?url=$enc")
        if (serverBase.isNotBlank()) urls.add("$serverBase/api/img?u=$enc")
        val q = LinkedBlockingQueue<Fetched>()
        val stop = AtomicBoolean(false)
        val directDone = AtomicBoolean(false)
        urls.forEachIndexed { i, u ->
            thread(isDaemon = true, name = "muse-img-$i") {
                var f = Fetched(i, 0, null)
                try {
                    if (i > 0) {
                        // chờ nguồn gốc xong (lỗi/treo) hoặc tối đa 1.5s, rồi so le nhẹ giữa các dự phòng
                        val t0 = System.currentTimeMillis()
                        while (!stop.get() && !directDone.get() && System.currentTimeMillis() - t0 < 1500L) Thread.sleep(50)
                        if (i > 1) Thread.sleep(150L * (i - 1))
                    }
                    if (!stop.get()) f = fetchImg(i, u, ua, if (i == 0) 5000 else 8000)
                } catch (e: Exception) { /* bỏ qua */ }
                if (i == 0) directDone.set(true)
                q.put(f)
            }
        }
        val deadline = System.currentTimeMillis() + 20_000L
        var done = 0
        try {
            while (done < urls.size) {
                val left = deadline - System.currentTimeMillis()
                if (left <= 0) break
                val f = q.poll(left, TimeUnit.MILLISECONDS) ?: break
                done++
                if (f.img != null) return f.img
                if (f.idx == 0 && (f.code == 404 || f.code == 410)) return null
            }
        } finally {
            stop.set(true)
        }
        return null
    }

    /**
     * Ảnh bìa YouTube/Google: tải bằng mạng gốc của điện thoại (không qua WebView, nên né được các kiểu chặn riêng của WebView/Chrome),
     * lưu vào RAM và bộ nhớ đệm trong máy. Thumbnail hq720/maxres/sd không có thì hạ ngay xuống mqdefault tại đây (không cần
     * web thử lại). Mạng máy không tới được thì nhờ proxy ảnh/server tải hộ. Tất cả đều lỗi thì trả null.
     */
    fun handleImage(req: WebResourceRequest, serverBase: String): WebResourceResponse? {
        if (!req.method.equals("GET", true)) return null
        val u = req.url
        val host = u.host?.lowercase() ?: return null
        if (u.scheme != "https" || !imgHost.matches(host)) return null
        val ctx = appCtx ?: return null
        val url = u.toString()
        ramImgs.get(url)?.let { return imgResp(it) }
        val dir = File(ctx.cacheDir, "img").apply { mkdirs() }
        val f = File(dir, sha1(url))
        try {
            if (f.exists()) {
                DataInputStream(f.inputStream().buffered()).use { di ->
                    val mime = di.readUTF()
                    val b = di.readBytes()
                    if (b.isNotEmpty()) {
                        val img = Img(mime, b)
                        ramImgs.put(url, img)
                        return imgResp(img)
                    }
                }
            }
        } catch (e: Exception) { runCatching { f.delete() } }
        val ua = req.requestHeaders["User-Agent"]
        var got = raceImg(url, ua, serverBase)
        if (got == null) {
            val m = badThumb.find(url)
            if (m != null) {
                val low = m.groupValues[1] + "mqdefault" + m.groupValues[3] + m.groupValues[4]
                got = raceImg(low, ua, serverBase)
            }
        }
        if (got == null) return null
        ramImgs.put(url, got)
        val img = got
        runCatching {
            DataOutputStream(f.outputStream().buffered()).use { o -> o.writeUTF(img.mime); o.write(img.bytes) }
            val all = dir.listFiles()
            if (all != null && all.size > 800) all.sortedBy { it.lastModified() }.take(200).forEach { it.delete() }
        }
        return imgResp(img)
    }
}
