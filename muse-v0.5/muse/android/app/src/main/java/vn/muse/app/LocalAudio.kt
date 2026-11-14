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
import kotlin.concurrent.thread

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
        thread(isDaemon = true, name = "muse-local-audio") { ensureRunning(ctx.applicationContext) }
    }

    fun stop() {
        synchronized(lock) {
            runCatching { proc?.destroy() }
            proc = null; port = 0
        }
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

    /** Dùng trong WebViewClient.shouldInterceptRequest (chạy ở luồng nền). Trả null = để WebView gọi server như bình thường. */
    fun handle(req: WebResourceRequest): WebResourceResponse? =
        handleLocal(req) ?: NativeYt.handle(req) // tiến trình cục bộ trước, rồi bản Kotlin thuần (chỉ phần nhạc); cả hai lỗi mới nhờ server

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

    private val imgHost = Regex("^(i\\.ytimg\\.com|[\\w-]+\\.(googleusercontent|ggpht)\\.com)$")

    private fun sha1(s: String) = MessageDigest.getInstance("SHA-1").digest(s.toByteArray()).joinToString("") { "%02x".format(it) }

    private fun download(url: String, ua: String?, timeoutMs: Int): Pair<String, ByteArray>? = try {
        val c = URL(url).openConnection() as HttpURLConnection
        c.connectTimeout = timeoutMs; c.readTimeout = timeoutMs + 5000
        if (!ua.isNullOrBlank()) c.setRequestProperty("User-Agent", ua)
        val ct = c.contentType?.substringBefore(';')?.trim().orEmpty()
        if (c.responseCode == 200 && ct.startsWith("image/")) {
            val b = c.inputStream.use { it.readBytes() }
            c.disconnect()
            if (b.isNotEmpty() && b.size < 8 * 1024 * 1024) ct to b else null
        } else { c.disconnect(); null }
    } catch (e: Exception) { null }

    /**
     * Ảnh bìa YouTube/Google: tải bằng mạng gốc của điện thoại (không qua WebView, nên né được các kiểu chặn riêng của WebView/Chrome),
     * lưu vào bộ nhớ đệm trong máy. Nếu mạng máy không tới được thì nhờ server tải hộ (/api/img). Cả hai đều lỗi thì trả null.
     */
    fun handleImage(req: WebResourceRequest, serverBase: String): WebResourceResponse? {
        if (!req.method.equals("GET", true)) return null
        val u = req.url
        val host = u.host?.lowercase() ?: return null
        if (u.scheme != "https" || !imgHost.matches(host)) return null
        val ctx = appCtx ?: return null
        val url = u.toString()
        val dir = File(ctx.cacheDir, "img").apply { mkdirs() }
        val f = File(dir, sha1(url))
        val hdr = mapOf("Access-Control-Allow-Origin" to "*", "Cache-Control" to "max-age=86400")
        try {
            if (f.exists()) {
                DataInputStream(f.inputStream().buffered()).use { di ->
                    val mime = di.readUTF()
                    val b = di.readBytes()
                    if (b.isNotEmpty()) return WebResourceResponse(mime, null, 200, "OK", hdr, ByteArrayInputStream(b))
                }
            }
        } catch (e: Exception) { runCatching { f.delete() } }
        val ua = req.requestHeaders["User-Agent"]
        val enc = URLEncoder.encode(url, "UTF-8")
        // Thử lần lượt, tất cả bằng mạng của điện thoại: trực tiếp -> wsrv.nl -> DuckDuckGo -> weserv.nl -> server (cuối cùng).
        val got = download(url, ua, 6000)
            ?: download("https://wsrv.nl/?url=$enc", ua, 8000)
            ?: download("https://external-content.duckduckgo.com/iu/?u=$enc&f=1", ua, 8000)
            ?: download("https://images.weserv.nl/?url=$enc", ua, 8000)
            ?: if (serverBase.isNotBlank()) download("$serverBase/api/img?u=$enc", ua, 15000) else null
        if (got == null) return null
        runCatching {
            DataOutputStream(f.outputStream().buffered()).use { o -> o.writeUTF(got.first); o.write(got.second) }
            val all = dir.listFiles()
            if (all != null && all.size > 800) all.sortedBy { it.lastModified() }.take(200).forEach { it.delete() }
        }
        return WebResourceResponse(got.first, null, 200, "OK", hdr, ByteArrayInputStream(got.second))
    }
}
