package vn.muse.app

import android.content.Intent

// Link thẻ chia sẻ (https://<server>/u/<tên> hoặc bbeat://u/<tên>): lưu tên người dùng chờ giao diện web lấy qua MuseNative.takePendingUser().
object Deeplink {
    @Volatile var pending: String = ""

    private val nameRe = Regex("^[A-Za-z0-9_.\\-]{1,64}$")

    fun userFrom(i: Intent?): String? {
        val u = i?.data ?: return null
        val seg = u.pathSegments
        val name = when {
            u.scheme == "bbeat" && u.host == "u" -> seg.firstOrNull()
            seg.size >= 2 && seg[0] == "u" -> seg[1]
            else -> null
        } ?: return null
        return name.takeIf { nameRe.matches(it) }
    }
}
