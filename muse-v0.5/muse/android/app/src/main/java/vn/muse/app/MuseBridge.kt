package vn.muse.app

import android.content.ContentValues
import android.content.Context
import android.content.Intent
import android.net.Uri
import android.os.Build
import android.os.Environment
import android.os.Handler
import android.os.Looper
import android.provider.MediaStore
import android.util.Base64
import android.webkit.JavascriptInterface
import java.io.File
import java.io.FileOutputStream
import java.io.OutputStream
import androidx.core.content.ContextCompat
import org.json.JSONObject
import kotlin.concurrent.thread

class MuseBridge(private val ctx: Context) {
    private val ui = Handler(Looper.getMainLooper())

    // Bài nào đang tải dở bằng nguồn nào ("local" = tiến trình cục bộ, "native" = NativeYt). Hai nguồn có thể chọn định dạng/bitrate
    // khác nhau, nên các đoạn của MỘT file phải đến từ cùng một nguồn, nếu không file ghép ra sẽ hỏng.
    private val dlSrc = java.util.concurrent.ConcurrentHashMap<String, String>()
    // Tổng dung lượng file của lần tải dở: các đoạn sau phải khớp, không thì link mới là file khác (định dạng/bitrate khác) → không ghép.
    private val dlTotal = java.util.concurrent.ConcurrentHashMap<String, Long>()
    // Nguồn cục bộ đã làm đứt tải bài này: lần tải lại từ đầu bỏ qua nguồn đó.
    private val dlLocalBad = java.util.concurrent.ConcurrentHashMap.newKeySet<String>()

    /**
     * Tải một đoạn nhạc YouTube ngay trong app (IP điện thoại), không qua WebView fetch. Chạy nền, xong gọi lại
     * window.museChunk(reqId, mime, total, base64, err). Không lấy được thì mime = null và err là lý do thật
     * (từng client YouTube lỗi gì) để giao diện hiện ra thay vì lỗi của server.
     */
    @JavascriptInterface
    fun audioChunkAsync(reqId: String, id: String, fmt: String, start: Long) {
        thread(isDaemon = true, name = "muse-dl") {
            val webm = fmt == "webm"
            var ch: AudioChunk? = null
            var localNote = ""
            if (start <= 0L) {
                dlSrc.remove(id); dlTotal.remove(id)
                // Bản Kotlin (NativeYt) trước: mỗi link đều được thử tải ở sâu trong file nên loại được link chết sau ~1MB.
                // Tiến trình cục bộ chỉ là dự phòng (và bị bỏ qua nếu lần trước nó làm đứt tải bài này).
                ch = runCatching { NativeYt.fetchChunk(id, webm, 0L) }.getOrNull()
                if (ch != null) dlSrc[id] = "native"
                else if (id !in dlLocalBad) {
                    ch = runCatching { LocalAudio.fetchChunk(id, fmt, 0L) }.getOrNull()
                    if (ch != null) dlSrc[id] = "local" else localNote = "tiến trình cục bộ: không lấy được"
                }
                if (ch != null) dlTotal[id] = ch.total
            } else {
                val src = dlSrc[id]
                ch = when (src) {
                    "local" -> runCatching { LocalAudio.fetchChunk(id, fmt, start) }.getOrNull()
                    "native" -> runCatching { NativeYt.fetchChunk(id, webm, start) }.getOrNull()
                    else -> null
                }
                val want = dlTotal[id]
                if (ch != null && want != null && ch.total != want) {
                    NativeYt.setError(id, "link mới có dung lượng khác (${ch.total} ≠ $want), không ghép được")
                    ch = null
                }
                if (ch == null) {
                    if (src == "local") dlLocalBad.add(id) // lần tải lại sau sẽ bỏ qua nguồn cục bộ
                    localNote = "nguồn \"${src ?: "?"}\" đứt ở byte $start"
                    dlSrc.remove(id)
                }
            }
            val rid = reqId.filter { it.isLetterOrDigit() }
            val js = if (ch == null) {
                val err = listOf(NativeYt.lastError(id), localNote).filter { it.isNotEmpty() }.joinToString(" • ")
                    .ifEmpty { "không lấy được luồng âm thanh trên máy" }
                "window.museChunk&&museChunk('$rid',null,0,'',${JSONObject.quote(err)})"
            } else {
                "window.museChunk&&museChunk('$rid','${ch.mime.filter { it.isLetterOrDigit() || it == '/' || it == '-' || it == '.' || it == '+' }}',${ch.total},'${Base64.encodeToString(ch.bytes, Base64.NO_WRAP)}')"
            }
            ui.post { Playback.sendToWeb?.invoke(js) }
        }
    }

    /** Lý do lần lấy nhạc gần nhất của bài [id] thất bại trên máy (rỗng nếu không có). Web gọi khi không phát được. */
    @JavascriptInterface
    fun audioError(id: String): String = NativeYt.lastError(id.filter { it.isLetterOrDigit() || it == '-' || it == '_' })

    @JavascriptInterface
    fun update(title: String, artist: String, art: String, playing: Boolean, posMs: Long, durMs: Long, hasTrack: Boolean) {
        val trackChanged = title != Playback.title || artist != Playback.artist || art != Playback.art
        val playChanged = playing != Playback.playing
        Playback.title = title; Playback.artist = artist; Playback.art = art
        Playback.playing = playing; Playback.posMs = posMs; Playback.durMs = durMs; Playback.hasTrack = hasTrack
        if (hasTrack && playing) {
            runCatching { ContextCompat.startForegroundService(ctx, Intent(ctx, KeepAliveService::class.java)) }
        }
        if (trackChanged || playChanged || hasTrack) Playback.onChanged?.invoke()
    }

    private var saveOut: OutputStream? = null
    private var saveUri: Uri? = null
    private var saveFile: File? = null

    @JavascriptInterface
    @Synchronized
    fun saveBegin(name: String, mime: String): String {
        saveEnd(false)
        return try {
            val safe = name.filter { it >= ' ' }.replace(Regex("[\\/:*?\"<>|]"), "_").take(120).ifBlank { "bai-hat" }
            if (Build.VERSION.SDK_INT >= 29) {
                val v = ContentValues().apply {
                    put(MediaStore.Audio.Media.DISPLAY_NAME, safe)
                    put(MediaStore.Audio.Media.MIME_TYPE, mime.ifBlank { "audio/mp4" })
                    put(MediaStore.Audio.Media.RELATIVE_PATH, "Music/BBeat")
                    put(MediaStore.Audio.Media.IS_PENDING, 1)
                }
                val uri = ctx.contentResolver.insert(MediaStore.Audio.Media.EXTERNAL_CONTENT_URI, v)
                    ?: return "err:không tạo được file trong Music/BBeat"
                saveUri = uri
                saveOut = ctx.contentResolver.openOutputStream(uri) ?: return "err:không mở được file để ghi"
            } else {
                val dir = File(ctx.getExternalFilesDir(Environment.DIRECTORY_MUSIC), "BBeat")
                dir.mkdirs()
                val f = File(dir, safe)
                saveFile = f
                saveOut = FileOutputStream(f)
            }
            ""
        } catch (e: Exception) {
            saveEnd(false)
            "err:" + (e.message ?: "lỗi tạo file")
        }
    }

    @JavascriptInterface
    @Synchronized
    fun saveChunk(b64: String): String {
        val o = saveOut ?: return "err:chưa mở file để ghi"
        return try {
            o.write(Base64.decode(b64, Base64.DEFAULT))
            ""
        } catch (e: Exception) {
            "err:" + (e.message ?: "lỗi ghi file")
        }
    }

    @JavascriptInterface
    @Synchronized
    fun saveEnd(ok: Boolean) {
        try { saveOut?.flush(); saveOut?.close() } catch (_: Exception) {}
        saveOut = null
        val uri = saveUri
        val f = saveFile
        saveUri = null
        saveFile = null
        try {
            if (uri != null) {
                if (ok) {
                    ctx.contentResolver.update(uri, ContentValues().apply { put(MediaStore.Audio.Media.IS_PENDING, 0) }, null, null)
                } else {
                    ctx.contentResolver.delete(uri, null, null)
                }
            } else if (f != null && !ok) {
                f.delete()
            }
        } catch (_: Exception) {}
    }

    @JavascriptInterface
    fun share(text: String): Boolean = try {
        val send = Intent(Intent.ACTION_SEND).apply { type = "text/plain"; putExtra(Intent.EXTRA_TEXT, text) }
        val chooser = Intent.createChooser(send, null).apply { addFlags(Intent.FLAG_ACTIVITY_NEW_TASK) }
        ctx.startActivity(chooser)
        true
    } catch (e: Exception) { false }

    @JavascriptInterface
    fun sdkAvailable(): Boolean = MuseSdk.available

    @JavascriptInterface
    fun sdkStart(appId: String, token: String): String = MuseSdk.start(appId, token)

    @JavascriptInterface
    fun sdkStatus(): String = MuseSdk.status()

    @JavascriptInterface
    fun sdkUpdate(title: String, artist: String, image: String, startMs: Long, endMs: Long) =
        MuseSdk.update(title, artist, image, startMs, endMs)

    @JavascriptInterface
    fun sdkClear() = MuseSdk.clear()
}
