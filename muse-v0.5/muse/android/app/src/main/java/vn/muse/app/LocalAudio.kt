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
import java.util.concurrent.Callable
import java.util.concurrent.ExecutorCompletionService
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.TimeUnit
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
    @Volatile private var lastErr = ""

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
    private const val AUDIO = "/api/yt/audio/"

    /**
     * Tải cả bài (?full=1): trả MỘT phản hồi 200 liên tục, không dùng Range/206. Phía trong vẫn lấy từng đoạn (tiến trình trên máy,
     * rồi bản Kotlin). Cách này tránh hẳn lỗi WebView trả thân rỗng cho fetch() có Range ("Nhận dữ liệu rỗng").
     */
    private fun handleFull(req: WebResourceRequest): WebResourceResponse? {
        val id = (req.url.encodedPath ?: return null).removePrefix(AUDIO)
        if (!Regex("[A-Za-z0-9_-]{11}").matches(id)) return null
        val q = (req.url.encodedQuery ?: "").split('&').filter { it.isNotEmpty() && it != "full=1" }.joinToString("&")
        val webm = req.url.getQueryParameter("f") == "webm"
        var src = "local"
        var first: NativeYt.Chunk? = openLocal(id, q, 0L)
        var opener: (Long) -> NativeYt.Chunk? = { p -> openLocal(id, q, p) }
        if (first == null) {
            src = "native"
            first = NativeYt.openChunk(id, webm, 0L)
            opener = { p -> NativeYt.openChunk(id, webm, p) }
        }
        val f = first ?: return null
        val h = HashMap<String, String>()
        h["Content-Length"] = f.total.toString()
        h["Cache-Control"] = "no-store"
        h["X-Muse-Src"] = src
        return WebResourceResponse(f.mime, null, 200, "OK", h, ChunkStream(f, opener))
    }

    private fun openLocal(id: String, rawQuery: String, pos: Long): NativeYt.Chunk? {
        val ctx = appCtx ?: return null
        if (!ensureRunning(ctx)) { lastErr = "không khởi động được (thiếu libmuse_audio.so hoặc hệ thống chặn chạy)"; return null }
        return try {
            val c = URL("http://127.0.0.1:$port$AUDIO$id?$rawQuery").openConnection() as HttpURLConnection
            c.connectTimeout = 3000
            c.readTimeout = 45000
            c.setRequestProperty("Range", "bytes=$pos-")
            val code = c.responseCode
            if (code != 200 && code != 206) {
                val msg = try { (c.errorStream?.bufferedReader()?.use { it.readText() } ?: "").take(400) } catch (e: Exception) { "" }
                lastErr = "HTTP $code $msg".trim()
                c.disconnect(); return null
            }
            val mime = c.contentType?.substringBefore(';')?.trim().takeUnless { it.isNullOrEmpty() } ?: "audio/mp4"
            val total = if (code == 206) (c.getHeaderField("Content-Range")?.substringAfterLast('/')?.trim()?.toLongOrNull() ?: -1L)
                        else c.getHeaderFieldLong("Content-Length", -1L)
            if (total <= 0L || (code == 200 && pos > 0L)) { c.disconnect(); return null }
            NativeYt.Chunk(c, total, mime)
        } catch (e: Exception) {
            lastErr = e.javaClass.simpleName + " " + (e.message ?: "")
            null
        }
    }

    fun handle(req: WebResourceRequest): WebResourceResponse? {
        if (req.method.equals("GET", true) && (req.url.encodedPath ?: "").startsWith(AUDIO) && req.url.getQueryParameter("full") == "1") {
            handleFull(req)?.let { return it }
        }
        handleLocal(req)?.let { return it }
        NativeYt.handle(req)?.let { return it }
        // Nhạc KHÔNG đi qua server: IP server (datacenter) luôn bị YouTube chặn nên quay về đó chỉ che mất lý do lỗi thật.
        // Trả lỗi 502 kèm lý do để giao diện hiện đúng nguyên nhân (tìm kiếm/trang chủ vẫn được phép nhờ server).
        if (req.method.equals("GET", true) && (req.url.encodedPath ?: "").startsWith("/api/yt/audio/")) {
            val msg = "Không lấy được nhạc trên điện thoại. Tiến trình trên máy: " + lastErr.ifEmpty { "(không có thông tin)" } +
                " | " + NativeYt.lastErr.ifEmpty { "bản Kotlin: (không chạy)" }
            val h = HashMap<String, String>()
            h["X-Muse-Src"] = "none"
            h["Cache-Control"] = "no-store"
            return WebResourceResponse("text/plain", "utf-8", 502, "Bad Gateway", h, ByteArrayInputStream(msg.toByteArray(Charsets.UTF_8)))
        }
        return null
    }

    private fun handleLocal(req: WebResourceRequest): WebResourceResponse? {
        if (!req.method.equals("GET", true)) return null
        val path = req.url.encodedPath ?: return null
        if (PREFIXES.none { path.startsWith(it) }) return null
        val ctx = appCtx ?: return null
        if (!ensureRunning(ctx)) { lastErr = "không khởi động được (thiếu libmuse_audio.so hoặc hệ thống chặn chạy)"; return null }
        val q = req.url.encodedQuery?.let { "?$it" } ?: ""
        return try {
            val c = URL("http://127.0.0.1:$port$path$q").openConnection() as HttpURLConnection
            c.connectTimeout = 3000
            c.readTimeout = 45000
            req.requestHeaders["Range"]?.let { c.setRequestProperty("Range", it) }
            req.requestHeaders["Authorization"]?.let { c.setRequestProperty("Authorization", it) }
            val code = c.responseCode
            // Cục bộ không lấy được (YouTube chặn, không có mạng...): để server thử, đừng báo lỗi cho trình phát.
            if (code != 200 && code != 206) {
                val msg = try { (c.errorStream?.bufferedReader()?.use { it.readText() } ?: "").take(400) } catch (e: Exception) { "" }
                lastErr = "HTTP $code $msg".trim()
                c.disconnect(); return null
            }
            val mime = c.contentType?.substringBefore(';')?.trim().takeUnless { it.isNullOrEmpty() } ?: "audio/mp4"
            val headers = HashMap<String, String>()
            headers["X-Muse-Src"] = "local"
            for (k in listOf("Content-Range", "Accept-Ranges", "Content-Length", "Cache-Control")) {
                c.getHeaderField(k)?.let { headers[k] = it }
            }
            val reason = if (code == 206) "Partial Content" else "OK"
            WebResourceResponse(mime, null, code, reason, headers, c.inputStream)
        } catch (e: Exception) {
            lastErr = e.javaClass.simpleName + " " + (e.message ?: "")
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

    // ------------------------------------------------------------------ tải ảnh song song (nhanh)

    private val imgPool = Executors.newCachedThreadPool { r -> Thread(r, "muse-img").apply { isDaemon = true } }
    @Volatile private var directFail = 0 // số lần liên tiếp tải trực tiếp i.ytimg.com thất bại: mạng chặn thì khỏi chờ nó nữa

    /**
     * Không chờ từng nguồn hết hạn rồi mới thử nguồn kế (trước đây mỗi ảnh có thể mất 6-20 giây khi mạng chặn i.ytimg.com).
     * Bật nguồn đầu ngay; sau 0,9 giây chưa có ảnh (hoặc nguồn đó vừa lỗi) thì bật thêm nguồn kế tiếp. Nguồn nào xong trước thì dùng.
     * Khi tải trực tiếp đã hỏng liên tiếp thì chuyển nó xuống sau các dịch vụ ảnh.
     */
    private fun raceImage(url: String, ua: String?, serverBase: String): Pair<String, ByteArray>? {
        val enc = URLEncoder.encode(url, "UTF-8")
        val direct = Callable<Pair<String, ByteArray>?> {
            val r = download(url, ua, 5000)
            if (r != null) directFail = 0 else if (!Thread.currentThread().isInterrupted) directFail++
            r
        }
        val srcs = ArrayList<Callable<Pair<String, ByteArray>?>>()
        srcs.add(direct)
        srcs.add(Callable { download("https://wsrv.nl/?url=$enc&w=480", ua, 6000) })
        srcs.add(Callable { download("https://external-content.duckduckgo.com/iu/?u=$enc&f=1", ua, 6000) })
        srcs.add(Callable { download("https://images.weserv.nl/?url=$enc&w=480", ua, 6000) })
        if (serverBase.isNotBlank()) srcs.add(Callable { download("$serverBase/api/img?u=$enc", ua, 12000) })
        if (directFail >= 2 && srcs.size > 2) { srcs.removeAt(0); srcs.add(1, direct) }

        val cs = ExecutorCompletionService<Pair<String, ByteArray>?>(imgPool)
        val futures = ArrayList<Future<Pair<String, ByteArray>?>>()
        var started = 0
        var finished = 0
        fun startNext() { if (started < srcs.size) { futures.add(cs.submit(srcs[started])); started++ } }
        val deadline = System.currentTimeMillis() + 16000
        var result: Pair<String, ByteArray>? = null
        startNext()
        while (finished < started) {
            val left = deadline - System.currentTimeMillis()
            if (left <= 0) break
            val f = cs.poll(minOf(900L, left), TimeUnit.MILLISECONDS)
            if (f == null) { startNext(); continue }
            finished++
            val r = try { f.get() } catch (e: Exception) { null }
            if (r != null) { result = r; break }
            startNext()
        }
        for (f in futures) f.cancel(true)
        return result
    }

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
        val got = raceImage(url, ua, serverBase) ?: return null
        runCatching {
            DataOutputStream(f.outputStream().buffered()).use { o -> o.writeUTF(got.first); o.write(got.second) }
            val all = dir.listFiles()
            if (all != null && all.size > 800) all.sortedBy { it.lastModified() }.take(200).forEach { it.delete() }
        }
        return WebResourceResponse(got.first, null, 200, "OK", hdr, ByteArrayInputStream(got.second))
    }
}

/** Ghép các đoạn tải tuần tự thành một luồng liền mạch. Mỗi đoạn hết thì mở đoạn kế từ vị trí đã đọc; đoạn rỗng liên tiếp thì báo lỗi. */
private class ChunkStream(first: NativeYt.Chunk, private val open: (Long) -> NativeYt.Chunk?) : java.io.InputStream() {
    private val total = first.total
    private var cur: NativeYt.Chunk? = first
    private var ins: java.io.InputStream? = first.conn.inputStream
    private var pos = 0L
    private var empties = 0

    override fun read(): Int {
        val b = ByteArray(1)
        val n = read(b, 0, 1)
        return if (n <= 0) -1 else b[0].toInt() and 0xff
    }

    override fun read(buf: ByteArray, off: Int, len: Int): Int {
        while (true) {
            if (pos >= total) return -1
            if (ins == null) {
                val c = open(pos) ?: throw java.io.IOException("không tải tiếp được từ byte $pos")
                cur = c
                ins = c.conn.inputStream
            }
            val n = ins!!.read(buf, off, len)
            if (n > 0) { pos += n; empties = 0; return n }
            closeCur()
            if (++empties >= 3) throw java.io.IOException("đoạn dữ liệu rỗng liên tiếp")
        }
    }

    private fun closeCur() {
        try { ins?.close() } catch (e: Exception) { }
        try { cur?.conn?.disconnect() } catch (e: Exception) { }
        ins = null
        cur = null
    }

    override fun close() = closeCur()
}
