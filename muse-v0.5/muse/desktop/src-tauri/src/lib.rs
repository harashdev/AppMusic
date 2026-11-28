use tauri::{WebviewUrl, WebviewWindowBuilder};
#[cfg(desktop)]
use serde_json::{json, Value};
#[cfg(desktop)]
use std::{
    io::{Read, Write},
    sync::Mutex,
    time::{SystemTime, UNIX_EPOCH},
};
#[cfg(desktop)]
use tauri::State;

// Biến MUSE_URL để trống (vd. GitHub Actions chưa đặt vars.MUSE_URL) vẫn là Some("") → phải coi như chưa đặt.
const SERVER_URL: &str = match option_env!("MUSE_URL") {
    Some(u) if !u.is_empty() => u,
    _ => "https://appmusic-ur3z.onrender.com",
};

#[cfg(desktop)]
mod discord_rpc {
    use super::*;
    trait Conn: Read + Write + Send {}
    impl<T: Read + Write + Send> Conn for T {}

    #[derive(Default)]
    pub struct Discord {
        conn: Option<Box<dyn Conn>>,
        app_id: String,
        user: String,
    }
    pub struct DiscordState(pub Mutex<Discord>);

    fn open_ipc() -> Option<Box<dyn Conn>> {
        for i in 0..10 {
            #[cfg(windows)]
            {
                let p = format!(r"\\.\pipe\discord-ipc-{i}");
                if let Ok(f) = std::fs::OpenOptions::new().read(true).write(true).open(&p) {
                    return Some(Box::new(f));
                }
            }
            #[cfg(unix)]
            {
                let mut dirs: Vec<String> = ["XDG_RUNTIME_DIR", "TMPDIR", "TMP", "TEMP"]
                    .iter()
                    .filter_map(|k| std::env::var(k).ok())
                    .collect();
                dirs.push("/tmp".into());
                for d in dirs {
                    for sub in ["", "snap.discord/", "app/com.discordapp.Discord/"] {
                        let path = format!("{}/{}discord-ipc-{}", d.trim_end_matches('/'), sub, i);
                        if let Ok(s) = std::os::unix::net::UnixStream::connect(&path) {
                            return Some(Box::new(s));
                        }
                    }
                }
            }
        }
        None
    }

    fn send_frame(c: &mut dyn Conn, op: u32, v: &Value) -> std::io::Result<()> {
        let data = v.to_string().into_bytes();
        let mut buf = Vec::with_capacity(8 + data.len());
        buf.extend_from_slice(&op.to_le_bytes());
        buf.extend_from_slice(&(data.len() as u32).to_le_bytes());
        buf.extend_from_slice(&data);
        c.write_all(&buf)?;
        c.flush()
    }

    fn read_frame(c: &mut dyn Conn) -> std::io::Result<(u32, Value)> {
        let mut h = [0u8; 8];
        c.read_exact(&mut h)?;
        let op = u32::from_le_bytes([h[0], h[1], h[2], h[3]]);
        let len = u32::from_le_bytes([h[4], h[5], h[6], h[7]]) as usize;
        if len > 1 << 20 {
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "khung quá lớn"));
        }
        let mut body = vec![0u8; len];
        c.read_exact(&mut body)?;
        Ok((op, serde_json::from_slice(&body).unwrap_or(Value::Null)))
    }

    fn handshake(d: &mut Discord) -> Result<String, String> {
        let mut c = open_ipc().ok_or("Không tìm thấy Discord đang chạy")?;
        send_frame(&mut *c, 0, &json!({ "v": 1, "client_id": d.app_id })).map_err(|e| e.to_string())?;
        let (_, v) = read_frame(&mut *c).map_err(|e| e.to_string())?;
        if v.get("evt").and_then(|x| x.as_str()) != Some("READY") {
            return Err(v
                .pointer("/data/message")
                .and_then(|x| x.as_str())
                .unwrap_or("Discord từ chối kết nối")
                .to_string());
        }
        let user = v
            .pointer("/data/user/global_name")
            .and_then(|x| x.as_str())
            .filter(|s| !s.is_empty())
            .or_else(|| v.pointer("/data/user/username").and_then(|x| x.as_str()))
            .unwrap_or("Discord")
            .to_string();
        d.conn = Some(c);
        d.user = user.clone();
        Ok(user)
    }

    fn now_ms() -> u64 {
        SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0)
    }

    fn set_activity(d: &mut Discord, activity: Value) -> Result<(), String> {
        let nonce = now_ms().to_string();
        let payload = json!({
            "cmd": "SET_ACTIVITY",
            "args": { "pid": std::process::id(), "activity": activity },
            "nonce": nonce
        });
        let c = d.conn.as_mut().ok_or("Chưa kết nối Discord")?;
        let res = send_frame(&mut **c, 1, &payload).and_then(|_| read_frame(&mut **c).map(|_| ()));
        if let Err(e) = res {
            d.conn = None;
            d.user.clear();
            return Err(e.to_string());
        }
        Ok(())
    }

    #[tauri::command]
    pub fn discord_connect(state: State<'_, DiscordState>, app_id: String) -> Result<String, String> {
        let mut d = state.0.lock().map_err(|e| e.to_string())?;
        if d.conn.is_some() && d.app_id == app_id {
            return Ok(d.user.clone());
        }
        d.conn = None;
        d.app_id = app_id;
        handshake(&mut d)
    }

    #[tauri::command]
    pub fn discord_activity(
        state: State<'_, DiscordState>,
        title: String,
        artist: String,
        image: String,
        start_ms: u64,
        end_ms: u64,
    ) -> Result<(), String> {
        let mut d = state.0.lock().map_err(|e| e.to_string())?;
        let mut a = json!({
            "type": 2,
            "details": title.chars().take(120).collect::<String>(),
            "state": artist.chars().take(120).collect::<String>(),
        });
        if !artist.is_empty() {
            a["state"] = json!(artist.chars().take(120).collect::<String>());
        }
        let mut ts = json!({});
        if start_ms > 0 {
            ts["start"] = json!(start_ms);
        }
        if end_ms > 0 {
            ts["end"] = json!(end_ms);
        }
        a["timestamps"] = ts;
        if image.starts_with("http") {
            a["assets"] = json!({ "large_image": image, "large_text": title });
        }
        set_activity(&mut d, a)
    }

    // Không phát nhạc: hiện vị trí đang đứng trong app (kiểu "Playing", không có thời gian/ảnh bìa).
    #[tauri::command]
    pub fn discord_page(state: State<'_, DiscordState>, label: String) -> Result<(), String> {
        let mut d = state.0.lock().map_err(|e| e.to_string())?;
        let a = json!({
            "type": 0,
            "details": label.chars().take(120).collect::<String>(),
        });
        set_activity(&mut d, a)
    }

    #[tauri::command]
    pub fn discord_clear(state: State<'_, DiscordState>) -> Result<(), String> {
        let mut d = state.0.lock().map_err(|e| e.to_string())?;
        if d.conn.is_none() {
            return Ok(());
        }
        set_activity(&mut d, Value::Null)
    }
}

#[cfg(all(desktop, muse_sdk))]
mod sdk {
    use std::ffi::{c_char, c_int, CStr, CString};

    extern "C" {
        fn muse_pres_start(app_id: u64, token: *const c_char) -> c_int;
        fn muse_pres_update(title: *const c_char, artist: *const c_char, image: *const c_char, start_s: i64, end_s: i64);
        fn muse_pres_page(label: *const c_char);
        fn muse_pres_clear();
        fn muse_pres_status() -> c_int;
        fn muse_pres_error() -> *const c_char;
    }
    extern "system" {
        fn LoadLibraryA(name: *const c_char) -> *mut core::ffi::c_void;
    }

    fn c(s: &str) -> CString {
        CString::new(s.replace('\0', "")).unwrap_or_default()
    }

    #[tauri::command]
    pub fn sdk_available() -> bool {
        unsafe { !LoadLibraryA(c"discord_partner_sdk.dll".as_ptr()).is_null() }
    }

    #[tauri::command]
    pub fn sdk_presence_start(app_id: String, token: String) -> Result<(), String> {
        let id: u64 = app_id.parse().map_err(|_| "app_id không hợp lệ".to_string())?;
        let r = unsafe { muse_pres_start(id, c(&token).as_ptr()) };
        if r == 0 { Ok(()) } else { Err("Không khởi động được Discord SDK".into()) }
    }

    #[tauri::command]
    pub fn sdk_presence_status() -> Result<String, String> {
        unsafe {
            match muse_pres_status() {
                2 => Ok("ready".into()),
                3 => {
                    let p = muse_pres_error();
                    let e = if p.is_null() { String::new() } else { CStr::from_ptr(p).to_string_lossy().into_owned() };
                    Err(if e.is_empty() { "lỗi Discord".into() } else { e })
                }
                1 => Ok("connecting".into()),
                _ => Ok("idle".into()),
            }
        }
    }

    #[tauri::command]
    pub fn sdk_presence_update(title: String, artist: String, image: String, start_ms: i64, end_ms: i64) -> Result<(), String> {
        unsafe { muse_pres_update(c(&title).as_ptr(), c(&artist).as_ptr(), c(&image).as_ptr(), start_ms / 1000, end_ms / 1000) };
        Ok(())
    }

    #[tauri::command]
    pub fn sdk_presence_page(label: String) -> Result<(), String> {
        unsafe { muse_pres_page(c(&label).as_ptr()) };
        Ok(())
    }

    #[tauri::command]
    pub fn sdk_presence_clear() -> Result<(), String> {
        unsafe { muse_pres_clear() };
        Ok(())
    }
}

#[cfg(all(desktop, not(muse_sdk)))]
mod sdk {
    #[tauri::command]
    pub fn sdk_available() -> bool {
        false
    }
    #[tauri::command]
    pub fn sdk_presence_start(_app_id: String, _token: String) -> Result<(), String> {
        Err("Bản app này không có Discord SDK".into())
    }
    #[tauri::command]
    pub fn sdk_presence_status() -> Result<String, String> {
        Ok("idle".into())
    }
    #[tauri::command]
    pub fn sdk_presence_update(_title: String, _artist: String, _image: String, _start_ms: i64, _end_ms: i64) -> Result<(), String> {
        Ok(())
    }
    #[tauri::command]
    pub fn sdk_presence_page(_label: String) -> Result<(), String> {
        Ok(())
    }
    #[tauri::command]
    pub fn sdk_presence_clear() -> Result<(), String> {
        Ok(())
    }
}

// Cổng nhà: app tự chạy `muse-server` (đi kèm ngay cạnh file .exe) ở chế độ GATEWAY_REMOTE=<MUSE_URL> trên 127.0.0.1.
// Nhạc/album/tìm kiếm YouTube được lấy bằng IP nhà của máy này; tài khoản, bạn bè, avatar, phòng nghe chung vẫn nằm trên server chính.
// Cửa sổ mở http://127.0.0.1:PORT (cùng một nguồn gốc) nên giao diện, API và WebSocket đều đi qua cổng nhà.
// Không chạy được (thiếu file, cổng bận...) thì app mở thẳng server chính như trước. Đặt MUSE_DIRECT=1 để cố ý bỏ qua cổng nhà.
#[cfg(desktop)]
mod gateway {
    use std::{
        net::{SocketAddr, TcpStream},
        process::{Child, Command, Stdio},
        sync::Mutex,
        time::{Duration, Instant},
    };

    pub const PORT: u16 = 38417;
    pub struct GatewayChild(pub Mutex<Option<Child>>);

    // Provider PO token (bgutil-pot, tuỳ chọn): có file `bgutil-pot(.exe)` cạnh app thì tự bật ở 127.0.0.1:4416 để yt-dlp
    // (khi các client YouTube viết tay bị chặn) có token và khỏi bị 403/"not a bot". Thiếu file thì bỏ qua, không ảnh hưởng gì.
    // yt-dlp cần thêm plugin bgutil (thư mục `yt-dlp-plugins` cạnh yt-dlp.exe) thì mới dùng được token này.
    pub const POT_PORT: u16 = 4416;
    static POT_CHILD: Mutex<Option<Child>> = Mutex::new(None);

    fn pot_up() -> bool {
        TcpStream::connect_timeout(&SocketAddr::from(([127, 0, 0, 1], POT_PORT)), Duration::from_millis(250)).is_ok()
    }

    /// Trả về true nếu provider đang chạy (do app bật hoặc đã có sẵn).
    fn start_pot(dir: &std::path::Path) -> bool {
        if pot_up() {
            return true;
        }
        let exe = dir.join(if cfg!(windows) { "bgutil-pot.exe" } else { "bgutil-pot" });
        if !exe.is_file() {
            return false;
        }
        // Tuỳ bản bgutil-pot mà cờ --host có hay không: thử có --host (chỉ nghe 127.0.0.1) trước, không chạy được thì thử chỉ --port.
        let mk = |with_host: bool| {
            let mut cmd = Command::new(&exe);
            cmd.arg("server");
            if with_host {
                cmd.arg("--host").arg("127.0.0.1");
            }
            cmd.arg("--port")
                .arg(POT_PORT.to_string())
                .current_dir(dir)
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null());
            #[cfg(windows)]
            {
                use std::os::windows::process::CommandExt;
                cmd.creation_flags(0x0800_0000);
            }
            cmd
        };
        let mut cmd = mk(true);
        let Ok(mut child) = cmd.spawn() else { return false };
        // Chờ tối đa ~2s cho provider mở cổng (không chặn lâu: nhạc vẫn phát bằng các client khác).
        let t0 = Instant::now();
        let mut retried = false;
        while t0.elapsed() < Duration::from_secs(3) {
            if pot_up() {
                break;
            }
            if !retried && matches!(child.try_wait(), Ok(Some(_))) {
                // thoát ngay = có thể không hiểu --host → chạy lại chỉ với --port
                retried = true;
                match mk(false).spawn() {
                    Ok(c) => child = c,
                    Err(_) => return false,
                }
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        let ok = pot_up();
        if ok {
            if let Ok(mut g) = POT_CHILD.lock() {
                *g = Some(child);
            }
        } else {
            let _ = child.kill();
        }
        ok
    }

    pub fn stop_pot() {
        if let Ok(mut g) = POT_CHILD.lock() {
            if let Some(mut c) = g.take() {
                let _ = c.kill();
            }
        }
    }

    fn up() -> bool {
        TcpStream::connect_timeout(&SocketAddr::from(([127, 0, 0, 1], PORT)), Duration::from_millis(250)).is_ok()
    }

    /// Trả về (địa chỉ cổng nhà nếu dùng được, tiến trình do app tự bật để tắt khi thoát).
    pub fn start(remote: &str) -> (Option<String>, Option<Child>) {
        let url = format!("http://127.0.0.1:{PORT}");
        if up() {
            // Đã có cổng nhà chạy sẵn (app mở lần 2, hoặc bạn tự chạy start-gateway.bat): dùng luôn.
            return (Some(url), None);
        }
        let Some(dir) = std::env::current_exe().ok().and_then(|p| p.parent().map(|d| d.to_path_buf())) else {
            return (None, None);
        };
        let exe = dir.join(if cfg!(windows) { "muse-server.exe" } else { "muse-server" });
        if !exe.exists() {
            eprintln!("Không thấy {} - mở thẳng server chính", exe.display());
            return (None, None);
        }
        // yt-dlp + deno đi kèm app (cạnh muse-server): lấy nhạc bằng IP máy này khi các client YouTube viết tay bị chặn / đòi PO token.
        let ytdlp = dir.join(if cfg!(windows) { "yt-dlp.exe" } else { "yt-dlp" });
        if ytdlp.is_file() {
            // Tự cập nhật yt-dlp ngầm (YouTube đổi liên tục): chạy nền, lỗi mạng/không ghi được thì bỏ qua.
            let up = ytdlp.clone();
            std::thread::spawn(move || {
                let mut c = Command::new(up);
                c.arg("-U").stdin(Stdio::null()).stdout(Stdio::null()).stderr(Stdio::null());
                #[cfg(windows)]
                {
                    use std::os::windows::process::CommandExt;
                    c.creation_flags(0x0800_0000);
                }
                let _ = c.status();
            });
        }
        let mut cmd = Command::new(&exe);
        if ytdlp.is_file() {
            cmd.env("YTDLP_PATH", &ytdlp);
            // Bật dịch vụ PO token ở NỀN: không được làm chậm việc mở cổng nhà (cổng nhà không lên kịp thì app quay về server Render).
            let pot_dir = dir.clone();
            std::thread::spawn(move || {
                let _ = start_pot(&pot_dir);
            });
            if dir.join(if cfg!(windows) { "bgutil-pot.exe" } else { "bgutil-pot" }).is_file() {
                cmd.env("YT_POT_URL", format!("http://127.0.0.1:{POT_PORT}"));
            }
        }
        // Đăng nhập YouTube cho yt-dlp để khỏi bị "Sign in to confirm you're not a bot" (mỗi bài lỗi tốn ~22 giây).
        // Ưu tiên: biến môi trường YT_COOKIES / YT_BROWSER do người dùng tự đặt > file cookies.txt cạnh app
        // > file yt-browser.txt (ghi tên trình duyệt: firefox / edge / chrome / brave) > tự tìm Firefox trên máy.
        if std::env::var("YT_COOKIES").map(|v| v.trim().is_empty()).unwrap_or(true)
            && std::env::var("YT_BROWSER").map(|v| v.trim().is_empty()).unwrap_or(true)
        {
            let ck = dir.join("cookies.txt");
            if ck.is_file() {
                cmd.env("YT_COOKIES", &ck);
            } else if let Some(b) = std::fs::read_to_string(dir.join("yt-browser.txt")).ok().map(|x| x.trim().to_string()).filter(|x| !x.is_empty()) {
                cmd.env("YT_BROWSER", b);
            } else {
                let fx = std::env::var_os("APPDATA").map(|a| std::path::PathBuf::from(a).join("Mozilla").join("Firefox").join("Profiles"));
                if fx.map(|d| d.is_dir()).unwrap_or(false) {
                    cmd.env("YT_BROWSER", "firefox");
                }
            }
        }
        cmd.current_dir(&dir)
            .env("GATEWAY_REMOTE", remote)
            .env("PORT", PORT.to_string())
            .env("BIND", "127.0.0.1")
            .env("GATEWAY_LOCAL_UI", "1")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        #[cfg(windows)]
        {
            use std::os::windows::process::CommandExt;
            const CREATE_NO_WINDOW: u32 = 0x0800_0000;
            cmd.creation_flags(CREATE_NO_WINDOW);
        }
        let mut child = match cmd.spawn() {
            Ok(c) => c,
            Err(e) => {
                eprintln!("Không chạy được cổng nhà: {e}");
                return (None, None);
            }
        };
        let t0 = Instant::now();
        // Lần mở đầu sau khi cài, Windows có thể quét file mới khá lâu → cho chờ tới 25 giây (trước là 8).
        while t0.elapsed() < Duration::from_secs(25) {
            if up() {
                return (Some(url), Some(child));
            }
            if matches!(child.try_wait(), Ok(Some(_))) {
                return (None, None);
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        let _ = child.kill();
        (None, None)
    }
}

#[cfg(desktop)]
#[tauri::command]
fn open_url(url: String) -> Result<(), String> {
    let url = url.trim().to_string();
    if !(url.starts_with("http://") || url.starts_with("https://")) {
        return Err("URL không hợp lệ".into());
    }
    #[cfg(windows)]
    let r = {
        use std::os::windows::process::CommandExt;
        const CREATE_NO_WINDOW: u32 = 0x0800_0000;
        std::process::Command::new("rundll32")
            .args(["url.dll,FileProtocolHandler", &url])
            .creation_flags(CREATE_NO_WINDOW)
            .spawn()
            .or_else(|_| std::process::Command::new("explorer.exe").arg(&url).spawn())
    };
    #[cfg(target_os = "macos")]
    let r = std::process::Command::new("open").arg(&url).spawn();
    #[cfg(all(unix, not(target_os = "macos")))]
    let r = std::process::Command::new("xdg-open").arg(&url).spawn();
    r.map(|_| ()).map_err(|e| e.to_string())
}

// Phóng to / thu nhỏ và toàn màn hình (F11) cho cửa sổ app, gọi từ giao diện.
#[cfg(desktop)]
#[tauri::command]
fn win_toggle_max(window: tauri::WebviewWindow) -> Result<(), String> {
    let maxed = window.is_maximized().map_err(|e| e.to_string())?;
    let r = if maxed { window.unmaximize() } else { window.maximize() };
    r.map_err(|e| e.to_string())
}

#[cfg(desktop)]
#[tauri::command]
fn win_toggle_fullscreen(window: tauri::WebviewWindow) -> Result<(), String> {
    let full = window.is_fullscreen().map_err(|e| e.to_string())?;
    window.set_fullscreen(!full).map_err(|e| e.to_string())
}

// ---------- Link thẻ hồ sơ: bbeat://u/<tên> (trang /u/<tên> của server mở app bằng link này) ----------
#[cfg(desktop)]
fn deep_user(urls: &[tauri::Url]) -> Option<String> {
    for u in urls {
        if !u.scheme().eq_ignore_ascii_case("bbeat") {
            continue;
        }
        // bbeat://u/<tên> → host = "u", path = "/<tên>" (một số hệ điều hành bỏ host → "u/<tên>" nằm trong path)
        let mut parts: Vec<&str> = Vec::new();
        if let Some(h) = u.host_str() {
            parts.push(h);
        }
        parts.extend(u.path().split('/').filter(|s| !s.is_empty()));
        if parts.len() >= 2 && parts[0].eq_ignore_ascii_case("u") {
            let n = parts[1];
            if !n.is_empty() && n.len() <= 64 && n.chars().all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.' || c == '-') {
                return Some(n.to_string());
            }
        }
    }
    None
}

// Giao diện gọi khi có người bấm link thẻ trên web: đưa cửa sổ app lên trước.
#[cfg(desktop)]
#[tauri::command]
fn win_focus(app: tauri::AppHandle) {
    show_main(&app);
}

#[cfg(desktop)]
fn show_main(app: &tauri::AppHandle) {
    use tauri::Manager;
    if let Some(w) = app.get_webview_window("main") {
        let _ = w.unminimize();
        let _ = w.show();
        let _ = w.set_focus();
    }
}

// App đang chạy: đưa cửa sổ lên trước rồi mở thẻ hồ sơ của người đó (web có sẵn window.museOpenUser; chưa có thì tải lại kèm ?u=)
#[cfg(desktop)]
fn open_user(app: &tauri::AppHandle, name: &str) {
    use tauri::Manager;
    show_main(app);
    if let Some(w) = app.get_webview_window("main") {
        // Trang có thể chưa nạp xong (app vừa bật / đang tải lại): chờ tối đa ~30 giây cho tới khi có window.museOpenUser rồi mới mở thẻ;
        // hết thời gian mà vẫn chưa có thì tải lại trang kèm ?u= để web tự hiện thẻ. Chống mở trùng khi 2 đường (single-instance + deep-link) cùng báo.
        let js = format!(
            "(function(n){{try{{if(window.__bbU===n&&Date.now()-(window.__bbT||0)<4000)return;window.__bbU=n;window.__bbT=Date.now();}}catch(e){{}}var t=0;(function f(){{try{{if(window.museOpenUser){{window.museOpenUser(n);return;}}}}catch(e){{}}if(++t<60){{setTimeout(f,500);}}else{{try{{location.href=location.origin+'/?u='+encodeURIComponent(n);}}catch(e){{}}}}}})();}})({});",
            json!(name)
        );
        let _ = w.eval(&js);
    }
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    let builder = tauri::Builder::default();

    // Plugin single-instance phải đứng đầu: bấm link bbeat:// khi app đang mở thì chỉ đưa app lên trước, không mở bản thứ hai.
    #[cfg(desktop)]
    let builder = builder
        .plugin(tauri_plugin_single_instance::init(|app, args, _cwd| {
            show_main(app);
            // Dự phòng: Windows truyền link bbeat://u/<tên> qua dòng lệnh của bản chạy thứ hai → mở thẻ ngay cả khi sự kiện deep-link không tới.
            if let Some(n) = args.iter().filter_map(|a| tauri::Url::parse(a).ok()).find_map(|u| deep_user(&[u])) {
                open_user(app, &n);
            }
        }))
        .plugin(tauri_plugin_deep_link::init());

    #[cfg(desktop)]
    let builder = builder
        .manage(discord_rpc::DiscordState(Mutex::new(Default::default())))
        .invoke_handler(tauri::generate_handler![
            open_url,
            win_toggle_max,
            win_toggle_fullscreen,
            win_focus,
            discord_rpc::discord_connect,
            discord_rpc::discord_activity,
            discord_rpc::discord_page,
            discord_rpc::discord_clear,
            sdk::sdk_available,
            sdk::sdk_presence_start,
            sdk::sdk_presence_status,
            sdk::sdk_presence_update,
            sdk::sdk_presence_page,
            sdk::sdk_presence_clear,
        ]);

    builder
        .setup(|app| {
            #[allow(unused_mut)]
            let mut start_url: String = SERVER_URL.to_string();
            #[allow(unused_mut)]
            let mut cold_user: Option<String> = None;
            #[cfg(desktop)]
            {
                use tauri_plugin_deep_link::DeepLinkExt;
                let dl = app.deep_link();
                // Bản không cài qua installer (portable/AppImage) vẫn tự đăng ký bbeat:// với hệ điều hành
                #[cfg(any(windows, target_os = "linux"))]
                {
                    let _ = dl.register_all();
                }
                // Mở app lúc đang tắt bằng link bbeat://u/<tên>: tải thẳng trang kèm ?u=<tên>, web tự hiện thẻ hồ sơ
                cold_user = dl
                    .get_current()
                    .ok()
                    .flatten()
                    .and_then(|u| deep_user(&u))
                    // Dự phòng: lấy link bbeat:// thẳng từ dòng lệnh lúc app được bật bằng link
                    .or_else(|| std::env::args().filter_map(|a| tauri::Url::parse(&a).ok()).find_map(|u| deep_user(&[u])));
                // Mở link khi app đang chạy
                let h = app.handle().clone();
                dl.on_open_url(move |ev| {
                    if let Some(n) = deep_user(&ev.urls()) {
                        open_user(&h, &n);
                    }
                });
            }
            #[cfg(desktop)]
            {
                use tauri::Manager;
                if std::env::var("MUSE_DIRECT").is_err() {
                    let (gw_url, child) = gateway::start(SERVER_URL.trim_end_matches('/'));
                    if let Some(u) = gw_url {
                        start_url = u;
                    }
                    app.manage(gateway::GatewayChild(Mutex::new(child)));
                }
            }
            let mut url: tauri::Url = start_url.parse().map_err(|e| format!("MUSE_URL không hợp lệ: {e}"))?;
            if let Some(n) = cold_user {
                url.query_pairs_mut().append_pair("u", &n);
            }
            WebviewWindowBuilder::new(app, "main", WebviewUrl::External(url))
                .title("BBeat")
                .inner_size(1280.0, 820.0)
                .min_inner_size(420.0, 600.0)
                .build()?;
            Ok(())
        })
        .build(tauri::generate_context!())
        .expect("lỗi khi chạy Muse")
        .run(|_app, _event| {
            // Thoát app thì tắt luôn cổng nhà do app đã tự bật.
            #[cfg(desktop)]
            if let tauri::RunEvent::Exit = _event {
                use tauri::Manager;
                gateway::stop_pot();
                if let Some(g) = _app.try_state::<gateway::GatewayChild>() {
                    if let Ok(mut c) = g.0.lock() {
                        if let Some(mut ch) = c.take() {
                            let _ = ch.kill();
                        }
                    }
                }
            }
        });
}
