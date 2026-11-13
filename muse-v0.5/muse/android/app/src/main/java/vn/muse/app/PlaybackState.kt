package vn.muse.app

import android.os.Handler
import android.os.Looper

object Playback {
    @Volatile var title: String = ""
    @Volatile var artist: String = ""
    @Volatile var art: String = ""
    @Volatile var playing: Boolean = false
    @Volatile var posMs: Long = 0
    @Volatile var durMs: Long = 0
    @Volatile var hasTrack: Boolean = false

    @Volatile var sendToWeb: ((String) -> Unit)? = null
    @Volatile var onChanged: (() -> Unit)? = null

    private val main = Handler(Looper.getMainLooper())

    fun command(cmd: String, arg: Long = 0) {
        val js = if (cmd == "seek") "window.museCmd&&museCmd('seek',$arg)" else "window.museCmd&&museCmd('$cmd')"
        main.post { sendToWeb?.invoke(js) }
    }
}
