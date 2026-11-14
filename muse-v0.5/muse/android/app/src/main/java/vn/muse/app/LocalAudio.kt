package vn.muse.app

import android.content.Context
import android.webkit.WebResourceRequest
import android.webkit.WebResourceResponse
import java.io.File
import java.net.HttpURLConnection
import java.net.InetSocketAddress
import java.net.ServerSocket
import java.net.Socket
import java.net.URL
import kotlin.concurrent.thread

/**
 * Lấy nhạc YouTube ngay trên điện thoại (IP nhà), không đi qua server.
 *
 * Tiến trình `libmuse_audio.so` (chính là muse-server build cho Android, chạy với AUDIO_ONLY=1) lắng nghe ở 127.0.0.1.
 * WebView vẫn gọi `/api/yt/audio/...` như cũ; [handle] chặn đúng các yêu cầu đó và chuyển tới tiến trình cục bộ,
 * nên giao diện web không phải sửa gì. Nếu file không có trong APK (vd. máy 32-bit) hoặc cục bộ lỗi thì trả null:
 * WebView tự gọi server như trước. Mọi thứ khác (tài khoản, bạn bè, phòng nghe chung, video MV...) không bị đụng tới.
 */
object LocalAudio {
    private const val LIB = "libmuse_audio.so"
    private const val PREFIX = "/api/yt/audio/"

    @Volatile private var proc: Process? = null
    @Volatile private var port = 0
    private val lock = Any()

    /** Gọi một lần khi mở app (chạy nền, không chặn giao diện). */
    fun start(ctx: Context) {
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
    fun handle(req: WebResourceRequest): WebResourceResponse? {
        if (!req.method.equals("GET", true)) return null
        val path = req.url.encodedPath ?: return null
        if (!path.startsWith(PREFIX)) return null
        val ctx = appCtx ?: return null
        if (!ensureRunning(ctx)) return null
        val q = req.url.encodedQuery?.let { "?$it" } ?: ""
        return try {
            val c = URL("http://127.0.0.1:$port$path$q").openConnection() as HttpURLConnection
            c.connectTimeout = 3000
            c.readTimeout = 45000
            req.requestHeaders["Range"]?.let { c.setRequestProperty("Range", it) }
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
}
