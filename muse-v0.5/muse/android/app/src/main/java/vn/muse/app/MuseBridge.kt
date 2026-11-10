package vn.muse.app

import android.content.ContentValues
import android.content.Context
import android.content.Intent
import android.net.Uri
import android.os.Build
import android.os.Environment
import android.provider.MediaStore
import android.util.Base64
import android.webkit.JavascriptInterface
import androidx.core.content.ContextCompat
import java.io.File
import java.io.FileOutputStream
import java.io.OutputStream

/** Cầu nối JS -> native. Trong web gọi: MuseNative.update(title, artist, art, playing, posMs, durMs, hasTrack). */
class MuseBridge(private val ctx: Context) {
    @JavascriptInterface
    fun update(title: String, artist: String, art: String, playing: Boolean, posMs: Long, durMs: Long, hasTrack: Boolean) {
        val trackChanged = title != Playback.title || artist != Playback.artist || art != Playback.art
        val playChanged = playing != Playback.playing
        Playback.title = title; Playback.artist = artist; Playback.art = art
        Playback.playing = playing; Playback.posMs = posMs; Playback.durMs = durMs; Playback.hasTrack = hasTrack
        if (hasTrack && playing) {
            // Chỉ khởi động service khi người dùng đang phát nhạc (app đang ở trước nền).
            runCatching { ContextCompat.startForegroundService(ctx, Intent(ctx, KeepAliveService::class.java)) }
        }
        if (trackChanged || playChanged || hasTrack) Playback.onChanged?.invoke()
    }

    // ---- Discord Social SDK (Rich Presence không cần app Discord) ----
    @JavascriptInterface
    fun sdkAvailable(): Boolean = MuseSdk.available

    /** Trả "" nếu bắt đầu được, ngược lại là thông báo lỗi. */
    @JavascriptInterface
    fun sdkStart(appId: String, token: String): String = MuseSdk.start(appId, token)

    @JavascriptInterface
    fun sdkStatus(): String = MuseSdk.status()

    @JavascriptInterface
    fun sdkUpdate(title: String, artist: String, image: String, startMs: Long, endMs: Long) =
        MuseSdk.update(title, artist, image, startMs, endMs)

    @JavascriptInterface
    fun sdkClear() = MuseSdk.clear()

    // ---- Tải nhạc về máy: WebView không tự lưu được file blob nên web gửi từng đoạn (base64) sang đây ----
    private var saveOut: OutputStream? = null
    private var saveUri: Uri? = null

    /** Mở file mới trong Music/BBeat. Trả "" nếu được, ngược lại là "err:...". */
    @JavascriptInterface
    @Synchronized
    fun saveBegin(name: String, mime: String): String {
        saveClose(false)
        return try {
            val safe = name.replace(Regex("[\\\\/:*?\"<>|]"), "_").ifBlank { "bai-hat.m4a" }
            if (Build.VERSION.SDK_INT >= 29) {
                val v = ContentValues().apply {
                    put(MediaStore.MediaColumns.DISPLAY_NAME, safe)
                    put(MediaStore.MediaColumns.MIME_TYPE, mime)
                    put(MediaStore.MediaColumns.RELATIVE_PATH, "Music/BBeat")
                    put(MediaStore.MediaColumns.IS_PENDING, 1)
                }
                val uri = ctx.contentResolver.insert(MediaStore.Audio.Media.EXTERNAL_CONTENT_URI, v)
                    ?: return "err:không tạo được file"
                saveUri = uri
                saveOut = ctx.contentResolver.openOutputStream(uri)
            } else {
                val dir = File(ctx.getExternalFilesDir(Environment.DIRECTORY_MUSIC), "BBeat")
                dir.mkdirs()
                saveOut = FileOutputStream(File(dir, safe))
            }
            if (saveOut == null) { saveClose(false); "err:không mở được file" } else ""
        } catch (e: Exception) {
            saveClose(false); "err:" + (e.message ?: "lỗi ghi file")
        }
    }

    @JavascriptInterface
    @Synchronized
    fun saveChunk(b64: String): String {
        val o = saveOut ?: return "err:chưa mở file"
        return try { o.write(Base64.decode(b64, Base64.DEFAULT)); "" } catch (e: Exception) { saveClose(false); "err:" + (e.message ?: "lỗi ghi file") }
    }

    @JavascriptInterface
    @Synchronized
    fun saveEnd(ok: Boolean): String { saveClose(ok); return "" }

    private fun saveClose(ok: Boolean) {
        runCatching { saveOut?.flush(); saveOut?.close() }
        saveOut = null
        val u = saveUri; saveUri = null
        if (u != null) runCatching {
            if (ok) ctx.contentResolver.update(u, ContentValues().apply { put(MediaStore.MediaColumns.IS_PENDING, 0) }, null, null)
            else ctx.contentResolver.delete(u, null, null)
        }
    }
}
