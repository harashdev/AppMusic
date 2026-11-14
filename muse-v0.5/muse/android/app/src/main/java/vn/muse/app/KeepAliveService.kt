package vn.muse.app

import android.app.Notification
import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.PendingIntent
import android.app.Service
import android.content.BroadcastReceiver
import android.content.Context
import android.content.Intent
import android.content.IntentFilter
import android.content.pm.ServiceInfo
import android.graphics.Bitmap
import android.graphics.BitmapFactory
import android.graphics.drawable.Icon
import android.media.AudioManager
import android.media.MediaMetadata
import android.media.session.MediaSession
import android.media.session.PlaybackState
import android.net.wifi.WifiManager
import android.os.Build
import android.os.Handler
import android.os.IBinder
import android.os.Looper
import android.os.PowerManager
import android.os.SystemClock
import androidx.core.content.ContextCompat
import java.io.File
import java.net.HttpURLConnection
import java.net.URL
import java.security.MessageDigest
import kotlin.concurrent.thread

class KeepAliveService : Service() {
    private lateinit var nm: NotificationManager
    private lateinit var session: MediaSession
    private val main = Handler(Looper.getMainLooper())

    private var wl: PowerManager.WakeLock? = null
    private var wifi: WifiManager.WifiLock? = null
    // Không tự giữ audio focus nữa: WebView (Chromium) đã tự xin focus cho thẻ <audio>, và hai bên giành nhau
    // khiến service nhận AUDIOFOCUS_LOSS "giả" rồi tự gửi lệnh pause (lúc được lúc không tuỳ bài tải nhanh hay chậm).
    private var wasPlaying = false
    private var lastSig = ""

    private var artUrl = ""
    private var artBmp: Bitmap? = null

    private val idleStop = Runnable { stopForeground(STOP_FOREGROUND_REMOVE); stopSelf() }

    private val noisy = object : BroadcastReceiver() {
        override fun onReceive(c: Context?, i: Intent?) { if (Playback.playing) Playback.command("pause") }
    }

    override fun onBind(intent: Intent?): IBinder? = null

    override fun onCreate() {
        super.onCreate()
        nm = getSystemService(NotificationManager::class.java)
        nm.createNotificationChannel(
            NotificationChannel(CH, "Đang phát nhạc", NotificationManager.IMPORTANCE_LOW).apply { setShowBadge(false) }
        )
        session = MediaSession(this, "Muse").apply {
            setCallback(object : MediaSession.Callback() {
                override fun onPlay() = Playback.command("play")
                override fun onPause() = Playback.command("pause")
                override fun onSkipToNext() = Playback.command("next")
                override fun onSkipToPrevious() = Playback.command("prev")
                override fun onSeekTo(pos: Long) {
                    Playback.posMs = pos
                    Playback.onChanged?.invoke()
                    Playback.command("seek", pos)
                }
            })
            isActive = true
        }
        val n = buildNotification()
        if (Build.VERSION.SDK_INT >= 29) startForeground(NID, n, ServiceInfo.FOREGROUND_SERVICE_TYPE_MEDIA_PLAYBACK)
        else startForeground(NID, n)

        ContextCompat.registerReceiver(this, noisy, IntentFilter(AudioManager.ACTION_AUDIO_BECOMING_NOISY), ContextCompat.RECEIVER_NOT_EXPORTED)
        Playback.onChanged = { main.post { refresh() } }
        refresh()
    }

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        when (intent?.action) {
            ACT_TOGGLE -> Playback.command(if (Playback.playing) "pause" else "play")
            ACT_NEXT -> Playback.command("next")
            ACT_PREV -> Playback.command("prev")
        }
        return START_NOT_STICKY
    }

    override fun onTaskRemoved(rootIntent: Intent?) { stopSelf() }

    override fun onDestroy() {
        main.removeCallbacks(idleStop)
        Playback.onChanged = null
        runCatching { unregisterReceiver(noisy) }
        releaseLocks()
        runCatching { session.isActive = false; session.release() }
        super.onDestroy()
    }

    private fun refresh() {
        val p = Playback
        loadArtIfNeeded(p.art)

        // Chỉ dựng lại metadata + thông báo khi có gì đổi (bài, trạng thái, ảnh bìa); vị trí phát cập nhật nhẹ qua PlaybackState.
        val sig = "${p.title}\u0001${p.artist}\u0001${p.playing}\u0001${p.hasTrack}\u0001${System.identityHashCode(artBmp)}\u0001${p.durMs / 1000}"
        val changed = sig != lastSig
        lastSig = sig
        if (changed) session.setMetadata(
            MediaMetadata.Builder()
                .putString(MediaMetadata.METADATA_KEY_TITLE, p.title)
                .putString(MediaMetadata.METADATA_KEY_ARTIST, p.artist)
                .putLong(MediaMetadata.METADATA_KEY_DURATION, if (p.durMs > 0) p.durMs else -1)
                .apply { artBmp?.let { putBitmap(MediaMetadata.METADATA_KEY_ALBUM_ART, it) } }
                .build()
        )
        session.setPlaybackState(
            PlaybackState.Builder()
                .setActions(
                    PlaybackState.ACTION_PLAY or PlaybackState.ACTION_PAUSE or PlaybackState.ACTION_PLAY_PAUSE or
                        PlaybackState.ACTION_SKIP_TO_NEXT or PlaybackState.ACTION_SKIP_TO_PREVIOUS or PlaybackState.ACTION_SEEK_TO
                )
                .setState(
                    if (p.playing) PlaybackState.STATE_PLAYING else PlaybackState.STATE_PAUSED,
                    p.posMs, if (p.playing) 1f else 0f, SystemClock.elapsedRealtime()
                )
                .build()
        )
        if (changed) nm.notify(NID, buildNotification())

        if (p.playing) {
            acquireLocks(); main.removeCallbacks(idleStop) // acquire đặt lại hạn 6 giờ mỗi lần, nghe liên tục vẫn không hết khoá
        } else if (wasPlaying) {
            releaseLocks()
            main.removeCallbacks(idleStop); main.postDelayed(idleStop, IDLE_MS)
        }
        wasPlaying = p.playing
    }

    private fun pi(action: String, code: Int): PendingIntent = PendingIntent.getService(
        this, code, Intent(this, KeepAliveService::class.java).setAction(action),
        PendingIntent.FLAG_IMMUTABLE or PendingIntent.FLAG_UPDATE_CURRENT
    )

    private fun act(icon: Int, label: String, action: String, code: Int) =
        Notification.Action.Builder(Icon.createWithResource(this, icon), label, pi(action, code)).build()

    private fun buildNotification(): Notification {
        val p = Playback
        val open = PendingIntent.getActivity(
            this, 0,
            Intent(this, MainActivity::class.java).addFlags(Intent.FLAG_ACTIVITY_SINGLE_TOP or Intent.FLAG_ACTIVITY_NEW_TASK),
            PendingIntent.FLAG_IMMUTABLE or PendingIntent.FLAG_UPDATE_CURRENT
        )
        return Notification.Builder(this, CH)
            .setSmallIcon(R.drawable.ic_stat_bbeat)
            .setContentTitle(p.title.ifBlank { "BBeat" })
            .setContentText(p.artist.ifBlank { "Sẵn sàng phát nhạc" })
            .setLargeIcon(artBmp)
            .setContentIntent(open)
            .setOngoing(p.playing)
            .setOnlyAlertOnce(true)
            .setShowWhen(false)
            .setVisibility(Notification.VISIBILITY_PUBLIC)
            .addAction(act(android.R.drawable.ic_media_previous, "Trước", ACT_PREV, 1))
            .addAction(
                act(if (p.playing) android.R.drawable.ic_media_pause else android.R.drawable.ic_media_play,
                    if (p.playing) "Tạm dừng" else "Phát", ACT_TOGGLE, 2)
            )
            .addAction(act(android.R.drawable.ic_media_next, "Sau", ACT_NEXT, 3))
            .setStyle(Notification.MediaStyle().setMediaSession(session.sessionToken).setShowActionsInCompactView(0, 1, 2))
            .build()
    }

    private fun serverUrl(): String =
        (getSharedPreferences("muse", MODE_PRIVATE).getString("server", "")?.takeIf { it.isNotBlank() } ?: BuildConfig.MUSE_URL).trim().trimEnd('/')

    private fun loadArtIfNeeded(raw: String) {
        if (raw == artUrl) return
        artUrl = raw; artBmp = null
        if (raw.isBlank()) return
        val url = if (raw.startsWith("/")) serverUrl() + raw else raw
        if (!url.startsWith("http")) return
        // Bản nhỏ (vài chục KB) tải trước để thông báo có ảnh gần như ngay; bản lớn tải song song rồi thay vào.
        val lo = lowResArt(url)
        thread(isDaemon = true, name = "muse-art-lo") { deliverArt(raw, fetchArtBitmap(lo, true), false) }
        if (lo != url) thread(isDaemon = true, name = "muse-art-hi") { deliverArt(raw, fetchArtBitmap(url, false), true) }
    }

    private fun deliverArt(raw: String, bmp: Bitmap?, hi: Boolean) {
        if (bmp == null) return
        main.post {
            if (artUrl != raw) return@post
            if (!hi && artBmp != null) return@post // bản lớn về trước thì bỏ bản nhỏ
            artBmp = bmp
            refresh()
        }
    }

    private fun lowResArt(u: String): String = when {
        u.contains("hq720") -> u.replace("hq720", "mqdefault")
        u.contains("googleusercontent.com") || u.contains("ggpht.com") ->
            u.replace(Regex("=w\\d+-h\\d+[^?#]*$"), "=w400-h400-l90-rj").replace(Regex("=s\\d+[^?#]*$"), "=s400")
        else -> u
    }

    private fun sha1(s: String) = MessageDigest.getInstance("SHA-1").digest(s.toByteArray()).joinToString("") { "%02x".format(it) }

    // Có cache trong máy: bài nghe lại / bài đã từng hiện thì ảnh lên ngay, không cần mạng.
    private fun fetchArtBitmap(url: String, useProxies: Boolean): Bitmap? {
        val dir = File(cacheDir, "art").apply { mkdirs() }
        val f = File(dir, sha1(url))
        runCatching {
            if (f.exists() && f.length() > 0) {
                decodeArt(f.readBytes())?.let { f.setLastModified(System.currentTimeMillis()); return it }
            }
        }
        val enc = java.net.URLEncoder.encode(url, "UTF-8")
        val bytes = fetchArtBytes(url, 3500)
            ?: (if (useProxies) fetchArtBytes("https://wsrv.nl/?url=$enc", 5000) else null)
            ?: (if (useProxies) fetchArtBytes("https://external-content.duckduckgo.com/iu/?u=$enc&f=1", 5000) else null)
            ?: return null
        val bmp = decodeArt(bytes) ?: return null
        runCatching {
            f.writeBytes(bytes)
            val all = dir.listFiles()
            if (all != null && all.size > 60) all.sortedBy { it.lastModified() }.take(20).forEach { it.delete() }
        }
        return bmp
    }

    private fun decodeArt(b: ByteArray): Bitmap? {
        val cap = artCap()
        val o = BitmapFactory.Options().apply { inJustDecodeBounds = true }
        BitmapFactory.decodeByteArray(b, 0, b.size, o)
        if (o.outWidth <= 0 || o.outHeight <= 0) return null
        var s = 1
        while (maxOf(o.outWidth, o.outHeight) / (s * 2) >= cap) s *= 2
        val bmp = BitmapFactory.decodeByteArray(b, 0, b.size, BitmapFactory.Options().apply { inSampleSize = s }) ?: return null
        val m = maxOf(bmp.width, bmp.height)
        return if (m > cap) Bitmap.createScaledBitmap(bmp, bmp.width * cap / m, bmp.height * cap / m, true) else bmp
    }

    private fun artCap(): Int {
        val dm = resources.displayMetrics
        val side = minOf(dm.widthPixels, dm.heightPixels).coerceIn(512, 1280)
        val low = (getSystemService(ACTIVITY_SERVICE) as android.app.ActivityManager).isLowRamDevice
        return if (low) minOf(side, 768) else side
    }

    private fun fetchArtBytes(url: String, timeoutMs: Int): ByteArray? = runCatching {
        val c = URL(url).openConnection() as HttpURLConnection
        c.connectTimeout = timeoutMs; c.readTimeout = timeoutMs + 2000
        try {
            if (c.responseCode !in 200..299) return@runCatching null
            c.inputStream.use { it.readBytes() }.takeIf { it.isNotEmpty() && it.size < 8 * 1024 * 1024 }
        } finally { c.disconnect() }
    }.getOrNull()

    private fun acquireLocks() {
        if (wl == null) wl = (getSystemService(POWER_SERVICE) as PowerManager)
            .newWakeLock(PowerManager.PARTIAL_WAKE_LOCK, "muse:playback").apply { setReferenceCounted(false) }
        wl?.acquire(6 * 60 * 60 * 1000L)
        if (wifi == null) wifi = (applicationContext.getSystemService(WIFI_SERVICE) as WifiManager)
            .createWifiLock(WifiManager.WIFI_MODE_FULL_HIGH_PERF, "muse:wifi").apply { setReferenceCounted(false) }
        if (wifi?.isHeld == false) wifi?.acquire()
    }

    private fun releaseLocks() {
        wl?.let { if (it.isHeld) it.release() }
        wifi?.let { if (it.isHeld) it.release() }
    }

    private companion object {
        const val CH = "muse_playback"
        const val NID = 1
        const val IDLE_MS = 15 * 60 * 1000L
        const val ACT_TOGGLE = "vn.muse.app.TOGGLE"
        const val ACT_NEXT = "vn.muse.app.NEXT"
        const val ACT_PREV = "vn.muse.app.PREV"
    }
}
