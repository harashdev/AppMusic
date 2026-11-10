package vn.muse.app

import android.content.Context
import android.content.Intent
import android.webkit.JavascriptInterface
import androidx.core.content.ContextCompat

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
}
