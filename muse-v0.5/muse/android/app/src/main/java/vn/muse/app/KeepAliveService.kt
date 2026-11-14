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
import android.media.AudioAttributes
import android.media.AudioFocusRequest
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
import java.net.HttpURLConnection
import java.net.URL
import kotlin.concurrent.thread

class KeepAliveService : Service() {
    private lateinit var nm: NotificationManager
    private lateinit var session: MediaSession
    private val main = Handler(Looper.getMainLooper())

    private var wl: PowerManager.WakeLock? = null
    private var wifi: WifiManager.WifiLock? = null
    private var focusReq: AudioFocusRequest? = null
    private var focusAt = 0L
    private var focusOff = false
    private var resumeOnFocus = false
    private var wasPlaying = false

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
        abandonFocus()
        releaseLocks()
        runCatching { session.isActive = false; session.release() }
        super.onDestroy()
    }

    private fun refresh() {
        val p = Playback
        loadArtIfNeeded(p.art)

        session.setMetadata(
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
        nm.notify(NID, buildNotification())

        if (p.playing != wasPlaying) {
            wasPlaying = p.playing
            if (p.playing) { acquireLocks(); requestFocus(); main.removeCallbacks(idleStop) }
            else {
                releaseLocks()
                if (!resumeOnFocus) abandonFocus()
                main.removeCallbacks(idleStop); main.postDelayed(idleStop, IDLE_MS)
            }
        }
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
        thread(isDaemon = true, name = "muse-art") {
            val enc = java.net.URLEncoder.encode(url.replace("hq720", "mqdefault"), "UTF-8")
            val cands = listOf(url, url.replace("hq720", "mqdefault"), "https://wsrv.nl/?url=$enc", "https://external-content.duckduckgo.com/iu/?u=$enc&f=1")
            val bmp = cands.distinct().firstNotNullOfOrNull { fetchArt(it) }?.let { b ->
                val m = maxOf(b.width, b.height)
                val cap = artCap()
                if (m > cap) Bitmap.createScaledBitmap(b, b.width * cap / m, b.height * cap / m, true) else b
            }
            main.post { if (artUrl == raw && bmp != null) { artBmp = bmp; refresh() } }
        }
    }

    private fun artCap(): Int {
        val dm = resources.displayMetrics
        val side = minOf(dm.widthPixels, dm.heightPixels).coerceIn(512, 1280)
        val low = (getSystemService(ACTIVITY_SERVICE) as android.app.ActivityManager).isLowRamDevice
        return if (low) minOf(side, 768) else side
    }

    private fun fetchArt(url: String): Bitmap? = runCatching {
        val c = URL(url).openConnection() as HttpURLConnection
        c.connectTimeout = 6000; c.readTimeout = 8000
        if (c.responseCode !in 200..299) return@runCatching null
        c.inputStream.use { BitmapFactory.decodeStream(it) }
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

    private fun requestFocus() {
        if (focusOff) return
        val am = getSystemService(AudioManager::class.java)
        val req = focusReq ?: AudioFocusRequest.Builder(AudioManager.AUDIOFOCUS_GAIN)
            .setAudioAttributes(AudioAttributes.Builder().setUsage(AudioAttributes.USAGE_MEDIA).setContentType(AudioAttributes.CONTENT_TYPE_MUSIC).build())
            .setOnAudioFocusChangeListener { change -> main.post { onFocus(change) } }
            .build().also { focusReq = it }
        focusAt = SystemClock.elapsedRealtime()
        am.requestAudioFocus(req)
    }

    private fun abandonFocus() {
        focusReq?.let { runCatching { getSystemService(AudioManager::class.java).abandonAudioFocusRequest(it) } }
        resumeOnFocus = false
    }

    private fun onFocus(change: Int) {
        if ((change == AudioManager.AUDIOFOCUS_LOSS || change == AudioManager.AUDIOFOCUS_LOSS_TRANSIENT) &&
            SystemClock.elapsedRealtime() - focusAt < 2000
        ) {
            focusOff = true; abandonFocus(); Playback.command("resume"); return
        }
        when (change) {
            AudioManager.AUDIOFOCUS_LOSS -> { resumeOnFocus = false; if (Playback.playing) Playback.command("pause") }
            AudioManager.AUDIOFOCUS_LOSS_TRANSIENT -> if (Playback.playing) { resumeOnFocus = true; Playback.command("pause") }
            AudioManager.AUDIOFOCUS_GAIN -> if (resumeOnFocus) { resumeOnFocus = false; Playback.command("play") }
        }
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
