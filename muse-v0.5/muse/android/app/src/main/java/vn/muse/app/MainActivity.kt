package vn.muse.app

import android.Manifest
import android.annotation.SuppressLint
import android.app.AlertDialog
import android.content.ActivityNotFoundException
import android.content.Intent
import android.content.pm.PackageManager
import android.graphics.Color
import android.net.Uri
import android.os.Build
import android.os.Bundle
import android.webkit.RenderProcessGoneDetail
import android.text.InputType
import android.view.Gravity
import android.view.View
import android.view.ViewGroup
import android.webkit.CookieManager
import android.webkit.PermissionRequest
import android.webkit.ValueCallback
import android.webkit.WebChromeClient
import android.webkit.WebResourceError
import android.webkit.WebResourceRequest
import android.webkit.WebSettings
import android.webkit.WebView
import android.webkit.WebViewClient
import android.widget.Button
import android.widget.EditText
import android.widget.FrameLayout
import android.widget.LinearLayout
import android.widget.TextView
import androidx.activity.ComponentActivity
import androidx.activity.OnBackPressedCallback
import androidx.activity.result.contract.ActivityResultContracts
import androidx.core.content.ContextCompat
import java.io.File
import java.net.HttpURLConnection
import java.net.URL
import kotlin.concurrent.thread

class MainActivity : ComponentActivity() {
    private val bg = Color.parseColor("#07070B")
    private lateinit var root: FrameLayout
    private lateinit var web: WebView
    private lateinit var errorBox: LinearLayout
    private lateinit var errorText: TextView
    private val prefs by lazy { getSharedPreferences("muse", MODE_PRIVATE) }

    private var fileCb: ValueCallback<Array<Uri>>? = null
    private var customView: View? = null
    private var customCb: WebChromeClient.CustomViewCallback? = null

    private val askNotif = registerForActivityResult(ActivityResultContracts.RequestPermission()) {}

    // Camera cho WebView (quét QR): web xin -> xin quyền Android -> cấp lại cho web.
    private var camReq: PermissionRequest? = null
    private val askCam = registerForActivityResult(ActivityResultContracts.RequestPermission()) { ok ->
        val r = camReq; camReq = null
        if (r == null) return@registerForActivityResult
        if (ok) r.grant(arrayOf(PermissionRequest.RESOURCE_VIDEO_CAPTURE)) else r.deny()
    }
    private val pickFile = registerForActivityResult(ActivityResultContracts.StartActivityForResult()) { r ->
        fileCb?.onReceiveValue(WebChromeClient.FileChooserParams.parseResult(r.resultCode, r.data))
        fileCb = null
    }

    private val shellFile by lazy { File(filesDir, "shell.html") }
    private var lastShellFetch = 0L

    private fun cacheShell() {
        val now = System.currentTimeMillis()
        if (now - lastShellFetch < 6 * 3600_000L && shellFile.exists()) return
        val base = serverUrl()
        if (base.isBlank()) return
        lastShellFetch = now
        thread(name = "muse-shell") {
            try {
                val c = URL("$base/").openConnection() as HttpURLConnection
                c.connectTimeout = 8000; c.readTimeout = 8000
                c.setRequestProperty("Accept", "text/html")
                if (c.responseCode == 200) {
                    val html = c.inputStream.bufferedReader(Charsets.UTF_8).use { it.readText() }
                    if (html.contains("<title>BBeat")) shellFile.writeText(html, Charsets.UTF_8)
                }
                c.disconnect()
            } catch (e: Exception) { lastShellFetch = 0L /* thử lại lần sau */ }
        }
    }

    private fun loadCachedShell(): Boolean {
        if (!shellFile.exists()) return false
        return try {
            val base = serverUrl() + "/"
            errorBox.visibility = View.GONE
            web.visibility = View.VISIBLE
            web.loadDataWithBaseURL(base, shellFile.readText(Charsets.UTF_8), "text/html", "UTF-8", base)
            true
        } catch (e: Exception) { false }
    }

    private fun serverUrl(): String =
        (prefs.getString("server", "")?.takeIf { it.isNotBlank() } ?: BuildConfig.MUSE_URL).trim().trimEnd('/')

    @SuppressLint("SetJavaScriptEnabled")
    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        window.statusBarColor = bg
        window.navigationBarColor = bg

        root = FrameLayout(this).apply { setBackgroundColor(bg) }
        web = WebView(this).apply {
            setBackgroundColor(bg)
            layoutParams = FrameLayout.LayoutParams(ViewGroup.LayoutParams.MATCH_PARENT, ViewGroup.LayoutParams.MATCH_PARENT)
        }
        root.addView(web)
        buildErrorBox()
        root.addView(errorBox)
        setContentView(root)
        requestHighRefreshRate()

        // Service worker của web (lưu ảnh bìa) tự gọi mạng, không đi qua WebViewClient: cho nó đi qua LocalAudio.handleImage luôn.
        android.webkit.ServiceWorkerController.getInstance().setServiceWorkerClient(object : android.webkit.ServiceWorkerClient() {
            override fun shouldInterceptRequest(request: WebResourceRequest): android.webkit.WebResourceResponse? =
                LocalAudio.handleImage(request, serverUrl())
        })
        LocalAudio.start(this, serverUrl()) // nhạc YouTube lấy ngay trên máy (nếu APK có kèm libmuse_audio.so), không thì vẫn qua server
        setupWebView()
        onBackPressedDispatcher.addCallback(this, object : OnBackPressedCallback(true) {
            override fun handleOnBackPressed() {
                when {
                    customView != null -> chrome.onHideCustomView()
                    web.canGoBack() -> web.goBack()
                    else -> moveTaskToBack(true)
                }
            }
        })

        if (Build.VERSION.SDK_INT >= 33 &&
            ContextCompat.checkSelfPermission(this, Manifest.permission.POST_NOTIFICATIONS) != PackageManager.PERMISSION_GRANTED
        ) askNotif.launch(Manifest.permission.POST_NOTIFICATIONS)
        Playback.sendToWeb = { js -> web.evaluateJavascript(js, null) }

        Deeplink.userFrom(intent)?.let { Deeplink.pending = it } // mở app từ link thẻ: web lấy tên này sau khi vào app
        loadServer()
    }

    // App đang chạy mà bấm link thẻ: hiện thẻ ngay.
    override fun onNewIntent(intent: Intent) {
        super.onNewIntent(intent)
        setIntent(intent)
        val name = Deeplink.userFrom(intent) ?: return
        Deeplink.pending = ""
        web.evaluateJavascript("window.museOpenUser&&museOpenUser('$name')", null)
    }

    private val chrome = object : WebChromeClient() {
        override fun onPermissionRequest(request: PermissionRequest) {
            runOnUiThread {
                // Chỉ cho phép camera (không micro); các quyền khác từ chối.
                if (!request.resources.contains(PermissionRequest.RESOURCE_VIDEO_CAPTURE)) { request.deny(); return@runOnUiThread }
                if (ContextCompat.checkSelfPermission(this@MainActivity, Manifest.permission.CAMERA) == PackageManager.PERMISSION_GRANTED) {
                    request.grant(arrayOf(PermissionRequest.RESOURCE_VIDEO_CAPTURE))
                } else {
                    camReq?.deny()
                    camReq = request
                    askCam.launch(Manifest.permission.CAMERA)
                }
            }
        }

        override fun onPermissionRequestCanceled(request: PermissionRequest) {
            if (camReq === request) camReq = null
        }

        override fun onShowFileChooser(
            view: WebView, cb: ValueCallback<Array<Uri>>, params: WebChromeClient.FileChooserParams,
        ): Boolean {
            fileCb?.onReceiveValue(null)
            fileCb = cb
            return try {
                pickFile.launch(params.createIntent()); true
            } catch (e: Exception) {
                fileCb = null; cb.onReceiveValue(null); false
            }
        }

        override fun onShowCustomView(view: View, callback: WebChromeClient.CustomViewCallback) {
            if (customView != null) { callback.onCustomViewHidden(); return }
            customView = view; customCb = callback
            root.addView(view, FrameLayout.LayoutParams(ViewGroup.LayoutParams.MATCH_PARENT, ViewGroup.LayoutParams.MATCH_PARENT))
            web.visibility = View.GONE
        }

        override fun onHideCustomView() {
            customView?.let { root.removeView(it) }
            customView = null
            customCb?.onCustomViewHidden()
            customCb = null
            web.visibility = View.VISIBLE
        }
    }

    @SuppressLint("SetJavaScriptEnabled")
    private fun setupWebView() {
        val s = web.settings
        s.javaScriptEnabled = true
        s.domStorageEnabled = true
        s.databaseEnabled = true
        s.mediaPlaybackRequiresUserGesture = false
        s.mixedContentMode = WebSettings.MIXED_CONTENT_COMPATIBILITY_MODE
        s.loadWithOverviewMode = true
        s.useWideViewPort = true
        s.userAgentString = s.userAgentString.replace("; wv", "") + " MuseAndroid"
        val am = getSystemService(ACTIVITY_SERVICE) as android.app.ActivityManager
        s.setOffscreenPreRaster(!am.isLowRamDevice && am.memoryClass >= 256)
        s.cacheMode = WebSettings.LOAD_DEFAULT
        s.allowFileAccess = false
        s.setGeolocationEnabled(false)
        s.setSupportZoom(true)
        s.builtInZoomControls = true
        s.displayZoomControls = false
        s.textZoom = 100
        CookieManager.getInstance().apply {
            setAcceptCookie(true)
            setAcceptThirdPartyCookies(web, true)
        }
        WebView.setWebContentsDebuggingEnabled(BuildConfig.DEBUG)
        MuseSdk.init(this)
        web.addJavascriptInterface(MuseBridge(applicationContext), "MuseNative")
        web.setLayerType(View.LAYER_TYPE_NONE, null)
        web.setRendererPriorityPolicy(WebView.RENDERER_PRIORITY_IMPORTANT, false)
        web.webChromeClient = chrome
        web.webViewClient = object : WebViewClient() {
            // Chỉ /api/yt/audio/* được chuyển sang tiến trình cục bộ; null = WebView gọi server như cũ.
            override fun shouldInterceptRequest(view: WebView, request: WebResourceRequest): android.webkit.WebResourceResponse? =
                LocalAudio.handle(request) ?: LocalAudio.handleImage(request, serverUrl()) ?: super.shouldInterceptRequest(view, request)

            override fun shouldOverrideUrlLoading(view: WebView, request: WebResourceRequest): Boolean {
                if (!request.isForMainFrame) return false
                val u = request.url
                if (u.host != null && u.host == Uri.parse(serverUrl()).host) return false
                val h = u.host?.lowercase().orEmpty()
                if (h == "discord.com" || h.endsWith(".discord.com") || h == "discordapp.com" || h.endsWith(".discordapp.com")) return false
                try { startActivity(Intent(Intent.ACTION_VIEW, u)) } catch (e: ActivityNotFoundException) { /* không có app mở link */ }
                return true
            }

            override fun onReceivedError(view: WebView, request: WebResourceRequest, error: WebResourceError) {
                if (request.isForMainFrame) {
                    if (loadCachedShell()) return
                    showError("Không kết nối được tới\n${serverUrl()}\n\n(${error.description})")
                }
            }

            override fun onRenderProcessGone(view: WebView, detail: RenderProcessGoneDetail): Boolean {
                runCatching { root.removeView(web); web.destroy() }
                web = WebView(this@MainActivity).apply {
                    setBackgroundColor(bg)
                    layoutParams = FrameLayout.LayoutParams(ViewGroup.LayoutParams.MATCH_PARENT, ViewGroup.LayoutParams.MATCH_PARENT)
                }
                root.addView(web, 0)
                setupWebView(); loadServer()
                return true
            }

            override fun onPageFinished(view: WebView, url: String?) {
                if (errorBox.visibility != View.VISIBLE) web.visibility = View.VISIBLE
            }
        }
    }

    private fun buildErrorBox() {
        val dp = resources.displayMetrics.density
        errorText = TextView(this).apply {
            setTextColor(Color.parseColor("#EDEDF2")); textSize = 16f; gravity = Gravity.CENTER
        }
        val hint = TextView(this).apply {
            text = "Hãy bật Tailscale (nếu dùng) và chắc chắn server Muse đang chạy."
            setTextColor(Color.parseColor("#A6A6B4")); textSize = 13f; gravity = Gravity.CENTER
            setPadding(0, (12 * dp).toInt(), 0, (20 * dp).toInt())
        }
        val retry = Button(this).apply { text = "Thử lại"; setOnClickListener { loadServer() } }
        val change = Button(this).apply { text = "Đổi địa chỉ server"; setOnClickListener { askServer() } }
        errorBox = LinearLayout(this).apply {
            orientation = LinearLayout.VERTICAL
            gravity = Gravity.CENTER
            setPadding((28 * dp).toInt(), 0, (28 * dp).toInt(), 0)
            setBackgroundColor(bg)
            visibility = View.GONE
            addView(errorText); addView(hint); addView(retry); addView(change)
            layoutParams = FrameLayout.LayoutParams(ViewGroup.LayoutParams.MATCH_PARENT, ViewGroup.LayoutParams.MATCH_PARENT)
            isClickable = true
        }
    }

    // Hiện lỗi kết nối nhưng tự thử lại mỗi 4 giây; hết lỗi là tự vào app, không cần thoát ra mở lại.
    private val autoRetry = object : Runnable {
        override fun run() {
            if (errorBox.visibility != View.VISIBLE) return
            loadServer()
            root.postDelayed(this, 4000)
        }
    }

    private fun showError(msg: String) {
        errorText.text = msg
        errorBox.visibility = View.VISIBLE
        web.visibility = View.INVISIBLE
        root.removeCallbacks(autoRetry)
        root.postDelayed(autoRetry, 4000)
    }

    // Giao diện nằm sẵn trong APK: mở app là hiện ngay, không cần chờ server. Gọi API vẫn đi tới server vì baseURL là địa chỉ server.
    private fun loadEmbedded(): Boolean = try {
        val html = assets.open("shell.html").bufferedReader(Charsets.UTF_8).use { it.readText() }
        val base = serverUrl() + "/"
        errorBox.visibility = View.GONE
        web.visibility = View.VISIBLE
        web.loadDataWithBaseURL(base, html, "text/html", "UTF-8", base)
        true
    } catch (e: Exception) { false }

    private fun loadServer() {
        val url = serverUrl()
        if (url.isBlank()) { askServer(); return }
        if (loadEmbedded()) return
        errorBox.visibility = View.GONE
        web.visibility = View.VISIBLE
        web.loadUrl(url)
    }

    private fun askServer() {
        val input = EditText(this).apply {
            inputType = InputType.TYPE_TEXT_VARIATION_URI
            hint = "https://appmusic-ur3z.onrender.com"
            setText(serverUrl())
        }
        AlertDialog.Builder(this)
            .setTitle("Địa chỉ server Muse")
            .setView(input)
            .setPositiveButton("Lưu") { _, _ ->
                var v = input.text.toString().trim().trimEnd('/')
                if (v.isNotEmpty() && !v.contains("://")) {
                    val local = Regex("^(localhost|\\d{1,3}(\\.\\d{1,3}){3})(:\\d+)?$").matches(v)
                    v = (if (local) "http://" else "https://") + v
                }
                prefs.edit().putString("server", v).apply()
                loadServer()
            }
            .setNegativeButton("Hủy", null)
            .show()
    }

    // Xin Android chạy app ở tần số quét cao nhất của màn hình (90/120/144Hz). Mặc định nhiều máy chỉ cho app WebView chạy 60Hz,
    // nên vuốt trông "khựng" dù màn hình hỗ trợ 120Hz. Máy chỉ có 60Hz thì hàm này không làm gì.
    private fun requestHighRefreshRate() {
        try {
            @Suppress("DEPRECATION")
            val d = windowManager.defaultDisplay
            val cur = d.mode
            val best = d.supportedModes
                .filter { it.physicalWidth == cur.physicalWidth && it.physicalHeight == cur.physicalHeight }
                .maxByOrNull { it.refreshRate } ?: return
            if (best.refreshRate > cur.refreshRate + 1f || window.attributes.preferredDisplayModeId != best.modeId) {
                val lp = window.attributes
                lp.preferredDisplayModeId = best.modeId
                lp.preferredRefreshRate = best.refreshRate
                window.attributes = lp
            }
        } catch (_: Throwable) {}
    }

    override fun onResume() {
        super.onResume()
        requestHighRefreshRate()
        web.onResume(); web.resumeTimers()
        if (errorBox.visibility == View.VISIBLE) loadServer()
    }

    // Không để WebView tự dừng audio khi activity vào background / màn hình tắt.
    // WebView.onPause() gửi "visibilitychange=hidden" vào JS và trình duyệt Chromium
    // sẽ throttle/pause <audio> — đây là nguyên nhân nhạc dừng khi tắt màn hình.
    override fun onPause() {
        super.onPause()
        // Luôn giữ WebView ở trạng thái resume khi có bài đang phát hoặc đang nạp.
        if (Playback.hasTrack || Playback.playing) {
            web.onResume()
            web.resumeTimers()
        }
    }

    override fun onStop() {
        super.onStop()
        // Có bài đang nạp (kể cả đang tạm dừng) thì giữ WebView chạy nền, để nút Phát/Tiếp trên thông báo còn tác dụng.
        if (Playback.hasTrack || Playback.playing) { web.onResume(); web.resumeTimers() }
    }

    override fun onDestroy() {
        root.removeCallbacks(autoRetry)
        Playback.sendToWeb = null
        if (isFinishing) {
            stopService(Intent(this, KeepAliveService::class.java))
            LocalAudio.stop()
            web.destroy()
        }
        super.onDestroy()
    }
}
