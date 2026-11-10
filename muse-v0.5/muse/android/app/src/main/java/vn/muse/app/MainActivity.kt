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
import android.os.PowerManager
import android.provider.Settings
import android.webkit.RenderProcessGoneDetail
import android.text.InputType
import android.view.Gravity
import android.view.View
import android.view.ViewGroup
import android.webkit.CookieManager
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

/**
 * Muse cho Android = đúng giao diện web của server (giống app máy tính), đặt trong WebView.
 * Địa chỉ server gắn sẵn lúc build (MUSE_URL) nên không phải nhập IP.
 */
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
    private val pickFile = registerForActivityResult(ActivityResultContracts.StartActivityForResult()) { r ->
        fileCb?.onReceiveValue(WebChromeClient.FileChooserParams.parseResult(r.resultCode, r.data))
        fileCb = null
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

        setupWebView()
        onBackPressedDispatcher.addCallback(this, object : OnBackPressedCallback(true) {
            override fun handleOnBackPressed() {
                when {
                    customView != null -> chrome.onHideCustomView()
                    web.canGoBack() -> web.goBack()
                    else -> moveTaskToBack(true) // thoát ra nhưng nhạc vẫn chạy nền
                }
            }
        })

        if (Build.VERSION.SDK_INT >= 33 &&
            ContextCompat.checkSelfPermission(this, Manifest.permission.POST_NOTIFICATIONS) != PackageManager.PERMISSION_GRANTED
        ) askNotif.launch(Manifest.permission.POST_NOTIFICATIONS)
        // Dịch vụ nền được MuseBridge bật khi bắt đầu phát nhạc, không bật ngay lúc mở app.
        Playback.sendToWeb = { js -> web.evaluateJavascript(js, null) }
        askBatteryOnce()

        loadServer()
    }

    private val chrome = object : WebChromeClient() {
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
        s.userAgentString = s.userAgentString.replace("; wv", "") + " MuseAndroid" // YouTube coi như Chrome; web nhận ra app để dùng giao diện máy tính
        s.setOffscreenPreRaster(true)           // vẽ sẵn phần ngoài màn hình -> cuộn mượt hơn
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
        web.setLayerType(View.LAYER_TYPE_HARDWARE, null)
        web.webChromeClient = chrome
        web.webViewClient = object : WebViewClient() {
            override fun shouldOverrideUrlLoading(view: WebView, request: WebResourceRequest): Boolean {
                if (!request.isForMainFrame) return false
                val u = request.url
                if (u.host != null && u.host == Uri.parse(serverUrl()).host) return false
                // Link ra ngoài (Discord, v.v.) mở bằng trình duyệt.
                try { startActivity(Intent(Intent.ACTION_VIEW, u)) } catch (e: ActivityNotFoundException) { /* không có app mở link */ }
                return true
            }

            override fun onReceivedError(view: WebView, request: WebResourceRequest, error: WebResourceError) {
                if (request.isForMainFrame) showError("Không kết nối được tới\n${serverUrl()}\n\n(${error.description})")
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

    private fun showError(msg: String) {
        errorText.text = msg
        errorBox.visibility = View.VISIBLE
        web.visibility = View.INVISIBLE
    }

    private fun loadServer() {
        val url = serverUrl()
        if (url.isBlank()) { askServer(); return }
        errorBox.visibility = View.GONE
        web.visibility = View.VISIBLE
        web.loadUrl(url)
    }

    /** Chỉ hỏi khi build không gắn sẵn địa chỉ, hoặc khi bấm "Đổi địa chỉ server". */
    private fun askServer() {
        val input = EditText(this).apply {
            inputType = InputType.TYPE_TEXT_VARIATION_URI
            hint = "https://ten-may.xxxx.ts.net"
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

    override fun onResume() {
        super.onResume()
        web.onResume(); web.resumeTimers()
    }

    /** Hỏi một lần: cho Muse không bị tối ưu pin để Android không cắt nhạc khi tắt màn hình lâu. */
    private fun askBatteryOnce() {
        if (prefs.getBoolean("battery_asked", false)) return
        val pm = getSystemService(PowerManager::class.java)
        if (pm.isIgnoringBatteryOptimizations(packageName)) return
        prefs.edit().putBoolean("battery_asked", true).apply()
        AlertDialog.Builder(this)
            .setTitle("Giữ nhạc chạy khi tắt màn hình")
            .setMessage("Để Android không tự ngắt nhạc khi để lâu, hãy cho Muse chạy không bị hạn chế pin.")
            .setPositiveButton("Cho phép") { _, _ ->
                runCatching {
                    startActivity(Intent(Settings.ACTION_REQUEST_IGNORE_BATTERY_OPTIMIZATIONS, Uri.parse("package:$packageName")))
                }
            }
            .setNegativeButton("Để sau", null)
            .show()
    }

    override fun onDestroy() {
        Playback.sendToWeb = null
        if (isFinishing) {
            stopService(Intent(this, KeepAliveService::class.java))
            web.destroy()
        }
        super.onDestroy()
    }
}
