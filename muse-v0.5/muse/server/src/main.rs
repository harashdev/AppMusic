use argon2::{
    password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString},
    Argon2,
};
use axum::{
    body::{Body, Bytes},
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        ConnectInfo, DefaultBodyLimit, Path, Query, State,
    },
    http::{header, HeaderMap, HeaderValue, Request, StatusCode, Uri},
    middleware::{self, Next},
    response::{IntoResponse, Redirect, Response},
    routing::{get, post},
    Json, Router,
};
use futures_util::{SinkExt, StreamExt};
use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::broadcast;
use tower_http::services::ServeDir;

mod pl_import; // nhập playlist từ Spotify / Apple Music / SoundCloud

struct Room {
    tx: broadcast::Sender<String>,
    last_state: Option<String>,
    // Số kết nối đang mở của từng người trong phòng. Dùng để báo "vào phòng"/"thoát phòng" đúng 1 lần,
    // dù mạng chập chờn khiến app tự kết nối lại nhiều lần.
    members: HashMap<i64, usize>,
}

#[derive(Clone)]
struct AppState {
    db: Arc<Mutex<Connection>>,
    rooms: Arc<Mutex<HashMap<String, Room>>>,
    // Kênh thông báo riêng cho từng người dùng đang online (lời mời nghe chung).
    notify: Arc<Mutex<HashMap<i64, broadcast::Sender<String>>>>,
    avatar_dir: PathBuf,
    bg_dir: PathBuf,
    banner_dir: PathBuf,
    card_dir: PathBuf, // ảnh thẻ chia sẻ hồ sơ (data/cards/<id>.png)
    app_dir: PathBuf,
    http: reqwest::Client,
    // Client đi qua proxy (biến môi trường YT_PROXY), dùng khi người dùng bật "Proxy" trong Cài đặt.
    http_proxy: Option<reqwest::Client>,
    // Cache địa chỉ luồng âm thanh YouTube: khoá -> (url, kích thước, mime, thời điểm tạo - giây)
    audio: Arc<Mutex<HashMap<String, (String, u64, String, u64, usize)>>>,
    // Giới hạn đăng nhập/đăng ký: khoá -> (số lần, thời điểm bắt đầu cửa sổ - giây). Chống dò mật khẩu.
    auth_limit: Arc<Mutex<HashMap<String, (u32, u64)>>>,
    // Vé WebSocket dùng một lần, sống 30 giây: vé -> (người dùng, hạn - giây). Tránh để token phiên nằm trong URL (log proxy).
    ws_tickets: Arc<Mutex<HashMap<String, (User, u64)>>>,
    // Chế độ "cổng nhà" (GATEWAY_REMOTE): địa chỉ server chính để chuyển tiếp mọi thứ không phải YouTube.
    gateway: Option<Arc<Gateway>>,
    // Mã ghép thiết bị dùng một lần (đăng nhập nhanh không cần gõ mật khẩu): mã -> (user_id admin, hạn - giây).
    pair_codes: Arc<Mutex<HashMap<String, (i64, u64)>>>,
}

// Cổng nhà: chạy trên máy ở nhà (IP dân cư) để YouTube không chặn như khi chạy trên VPS/Render.
struct Gateway {
    remote: String, // vd. https://muse.example.com (không có dấu / cuối)
    client: reqwest::Client,
    // token đã băm -> (cho phép nội dung nhạy cảm, thời điểm lấy - giây)
    explicit: Mutex<HashMap<String, (bool, u64)>>,
}

// Giao diện nhúng sẵn (chỉ khi build với `--features embed-ui`, app desktop dùng): cổng nhà đặt GATEWAY_LOCAL_UI=1 thì tự phục vụ
// trang chính từ chính file chạy này thay vì tải từ server chính → giao diện nằm trong app, server chính chỉ còn lưu dữ liệu.
#[cfg(feature = "embed-ui")]
const EMBED_INDEX: &str = include_str!("../static/index.html");
#[cfg(not(feature = "embed-ui"))]
const EMBED_INDEX: &str = "";

fn gw_local_ui() -> bool {
    !EMBED_INDEX.is_empty() && std::env::var("GATEWAY_LOCAL_UI").map(|v| v.trim() == "1").unwrap_or(false)
}

// ===== Bảo mật: header phản hồi + giới hạn đăng nhập =====
// Chính sách nội dung (CSP): chỉ cho tải script/khung từ chính server và YouTube, chặn nhúng trang vào khung khác.
// Muốn tắt tạm khi gỡ lỗi: đặt biến môi trường CSP_OFF=1.
const CSP: &str = "default-src 'self'; \
script-src 'self' 'unsafe-inline' https://www.youtube.com https://s.ytimg.com; \
style-src 'self' 'unsafe-inline'; \
img-src 'self' data: blob: https:; \
media-src 'self' blob: https:; \
font-src 'self' data:; \
connect-src 'self' ws: wss: ipc: http://ipc.localhost https://i.ytimg.com https://*.googleusercontent.com https://*.ggpht.com https://www.youtube.com https://music.youtube.com https://returnyoutubedislikeapi.com https://api.github.com https://sponsor.ajay.app https://lrclib.net https://api-lyrics.simpmusic.org; \
frame-src https://www.youtube.com https://www.youtube-nocookie.com; \
worker-src 'self'; manifest-src 'self'; \
object-src 'none'; base-uri 'self'; form-action 'self'; frame-ancestors 'none'";

async fn security_headers(req: Request<Body>, next: Next) -> Response {
    let is_api = req.uri().path().starts_with("/api/");
    // Banner / avatar / nền tải lên có tên là UUID mới cho mỗi lần upload (không bao giờ đổi nội dung) → cho trình duyệt/WebView
    // giữ cả năm, lần mở sau hiện ngay không cần hỏi mạng.
    let is_upload = {
        let p = req.uri().path();
        p.starts_with("/banners/") || p.starts_with("/avatars/") || p.starts_with("/backgrounds/")
    };
    let mut res = next.run(req).await;
    let ok_status = res.status().is_success();
    let h = res.headers_mut();
    fn set(h: &mut HeaderMap, k: &'static str, v: &'static str) {
        h.insert(header::HeaderName::from_static(k), HeaderValue::from_static(v));
    }
    set(h, "x-content-type-options", "nosniff");
    set(h, "x-frame-options", "DENY");
    set(h, "referrer-policy", "no-referrer");
    set(h, "cross-origin-opener-policy", "same-origin");
    set(h, "cross-origin-resource-policy", "same-origin");
    set(h, "x-permitted-cross-domain-policies", "none");
    set(h, "permissions-policy", "camera=(), microphone=(), geolocation=(), payment=(), usb=(), serial=(), bluetooth=()");
    set(h, "strict-transport-security", "max-age=31536000");
    if std::env::var("CSP_OFF").ok().as_deref() != Some("1") {
        set(h, "content-security-policy", CSP);
    }
    if is_api && !h.contains_key(header::CACHE_CONTROL) {
        set(h, "cache-control", "no-store");
    } else if is_upload && ok_status && !h.contains_key(header::CACHE_CONTROL) {
        set(h, "cache-control", "public, max-age=31536000, immutable");
    }
    res
}

// Địa chỉ người gọi: ưu tiên X-Forwarded-For (khi đứng sau Caddy/Tailscale), không có thì dùng địa chỉ kết nối.
fn client_key(headers: &HeaderMap, addr: &std::net::SocketAddr) -> String {
    headers
        .get("x-forwarded-for")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.split(',').next())
        .map(|s| s.trim().chars().take(64).collect::<String>())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| addr.ip().to_string())
}

// true = đang bị khoá (đã vượt `max` lần trong `window` giây).
fn rl_blocked(st: &AppState, key: &str, max: u32, window: u64) -> bool {
    let mut g = st.auth_limit.lock().unwrap();
    let now = now_secs();
    if g.len() > 5000 {
        g.retain(|_, (_, t)| now.saturating_sub(*t) < 3600);
    }
    let cur: Option<(u32, u64)> = g.get(key).copied();
    match cur {
        Some((n, t)) if now.saturating_sub(t) < window => n >= max,
        Some(_) => {
            g.remove(key);
            false
        }
        None => false,
    }
}

fn rl_hit(st: &AppState, key: &str, window: u64) {
    let mut g = st.auth_limit.lock().unwrap();
    let now = now_secs();
    let e = g.entry(key.to_string()).or_insert((0, now));
    if now.saturating_sub(e.1) >= window {
        *e = (0, now);
    }
    e.0 += 1;
}

fn rl_clear(st: &AppState, key: &str) {
    st.auth_limit.lock().unwrap().remove(key);
}

fn too_many() -> ApiErr {
    (StatusCode::TOO_MANY_REQUESTS, "Thử quá nhiều lần. Vui lòng đợi vài phút rồi thử lại.".into())
}

#[derive(Serialize, Clone)]
struct User {
    id: i64,
    username: String,
    display_name: String,
    avatar: String,
    accent: String,
    bg: String,
    bg_url: String,
    code: String,
    is_admin: bool,
    banner: String,
    banner_color: String,
    status: String,
    crop: String,
    supporter: i64, // 0 = chưa ủng hộ, 1 = thành viên nhỏ, 2 = thành viên lớn (donate Ko-fi)
}

#[derive(Serialize)]
struct Friend {
    id: i64,
    display_name: String,
    avatar: String,
    online: bool,
    accent: String,
    banner: String,
    banner_color: String,
    status: String,
    crop: String,
    supporter: i64,
}

const USER_COLS: &str = "id, username, display_name, avatar, accent, bg, bg_url, code, is_admin, banner, banner_color, status, crop, supporter";

fn row_to_user(r: &rusqlite::Row) -> rusqlite::Result<User> {
    let is_admin = r.get::<_, i64>(8)? != 0;
    Ok(User {
        id: r.get(0)?,
        username: r.get(1)?,
        display_name: r.get(2)?,
        avatar: r.get(3)?,
        accent: r.get(4)?,
        bg: r.get(5)?,
        bg_url: r.get(6)?,
        code: r.get(7)?,
        is_admin,
        banner: r.get(9)?,
        banner_color: r.get(10)?,
        status: r.get(11)?,
        crop: r.get(12)?,
        supporter: if is_admin { 2 } else { r.get(13)? }, // admin: quyền cao nhất, luôn là thành viên lớn
    })
}

type ApiErr = (StatusCode, String);
type R<T> = Result<Json<T>, ApiErr>;
fn bad(m: &str) -> ApiErr { (StatusCode::BAD_REQUEST, m.to_string()) }
fn unauth() -> ApiErr { (StatusCode::UNAUTHORIZED, "Chưa đăng nhập hoặc phiên hết hạn".into()) }
fn e500() -> ApiErr { (StatusCode::INTERNAL_SERVER_ERROR, "Lỗi máy chủ".into()) }

fn hash_pw(pw: &str) -> Result<String, ()> {
    let salt = SaltString::encode_b64(uuid::Uuid::new_v4().as_bytes()).map_err(|_| ())?;
    Argon2::default().hash_password(pw.as_bytes(), &salt).map(|h| h.to_string()).map_err(|_| ())
}
fn verify_pw(pw: &str, hash: &str) -> bool {
    match PasswordHash::new(hash) {
        Ok(p) => Argon2::default().verify_password(pw.as_bytes(), &p).is_ok(),
        Err(_) => false,
    }
}
fn new_token() -> String {
    format!("{}{}", uuid::Uuid::new_v4().simple(), uuid::Uuid::new_v4().simple())
}

// Token phiên chỉ được lưu dưới dạng băm SHA-256 ("h1:" + hex): lộ file muse.db cũng không dùng lại được phiên đăng nhập.
// Người dùng giữ token gốc; mỗi lần gọi API server băm lại rồi so khớp.
fn tok_hash(t: &str) -> String {
    let d = Sha256::digest(t.as_bytes());
    let mut s = String::with_capacity(3 + d.len() * 2);
    s.push_str("h1:");
    for b in d.iter() {
        s.push_str(&format!("{:02x}", b));
    }
    s
}

// Băm giả để mất thời gian như khi kiểm tra mật khẩu thật: tên đăng nhập không tồn tại không trả lời nhanh hơn tên có thật.
static DUMMY_HASH: std::sync::OnceLock<String> = std::sync::OnceLock::new();

fn user_from_token(db: &Connection, token: &str) -> Option<User> {
    let sql = "SELECT u.id,u.username,u.display_name,u.avatar,u.accent,u.bg,u.bg_url,u.code,u.is_admin,u.banner,u.banner_color,u.status,u.crop,u.supporter \
               FROM users u JOIN sessions s ON s.user_id=u.id \
               WHERE s.token=?1 AND s.created_at > datetime('now','-30 days')";
    db.query_row(sql, [tok_hash(token)], row_to_user).ok()
}
fn bearer(h: &HeaderMap) -> Option<String> {
    h.get("authorization")?.to_str().ok()?.strip_prefix("Bearer ").map(|s| s.to_string())
}
fn require(st: &AppState, h: &HeaderMap) -> Result<User, ApiErr> {
    let token = bearer(h).ok_or_else(unauth)?;
    let db = st.db.lock().unwrap();
    let u = user_from_token(&db, &token);
    u.ok_or_else(unauth)
}

fn init_db(path: &std::path::Path) -> Connection {
    let db = Connection::open(path).expect("không mở được SQLite");
    db.execute_batch(
        "PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; PRAGMA foreign_keys=ON;
         CREATE TABLE IF NOT EXISTS users(
           id INTEGER PRIMARY KEY AUTOINCREMENT,
           username TEXT NOT NULL UNIQUE COLLATE NOCASE,
           pass_hash TEXT NOT NULL,
           display_name TEXT NOT NULL,
           avatar TEXT NOT NULL DEFAULT '🎧',
           accent TEXT NOT NULL DEFAULT '#31c27c',
           bg TEXT NOT NULL DEFAULT '',
           bg_url TEXT NOT NULL DEFAULT '',
           code TEXT NOT NULL UNIQUE,
           created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP);
         CREATE TABLE IF NOT EXISTS sessions(
           token TEXT PRIMARY KEY,
           user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
           created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP);
         CREATE TABLE IF NOT EXISTS friends(
           user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
           friend_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
           PRIMARY KEY(user_id, friend_id));
         CREATE TABLE IF NOT EXISTS friend_requests(
           from_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
           to_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
           created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
           PRIMARY KEY(from_id, to_id));
         CREATE TABLE IF NOT EXISTS favorites(
           user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
           track_key TEXT NOT NULL, track_json TEXT NOT NULL,
           PRIMARY KEY(user_id, track_key));
         CREATE TABLE IF NOT EXISTS history(
           user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
           track_key TEXT NOT NULL, track_json TEXT NOT NULL,
           PRIMARY KEY(user_id, track_key));
         CREATE TABLE IF NOT EXISTS comments(
           id INTEGER PRIMARY KEY AUTOINCREMENT,
           track_key TEXT NOT NULL,
           user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
           body TEXT NOT NULL,
           created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP);
         CREATE INDEX IF NOT EXISTS idx_comments_track ON comments(track_key, id);
         CREATE TABLE IF NOT EXISTS playlists(
           id INTEGER PRIMARY KEY AUTOINCREMENT,
           user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
           name TEXT NOT NULL);
         CREATE TABLE IF NOT EXISTS playlist_tracks(
           playlist_id INTEGER NOT NULL REFERENCES playlists(id) ON DELETE CASCADE,
           track_key TEXT NOT NULL, track_json TEXT NOT NULL,
           PRIMARY KEY(playlist_id, track_key));",
    )
    .expect("không tạo được bảng");
    // Nâng cấp DB cũ: thêm cột is_admin (lỗi "đã tồn tại" được bỏ qua).
    let _ = db.execute("ALTER TABLE users ADD COLUMN is_admin INTEGER NOT NULL DEFAULT 0", []);
    // Banner kiểu Discord: ảnh/GIF + màu dự phòng + dòng trạng thái.
    for ddl in [
        "ALTER TABLE users ADD COLUMN banner TEXT NOT NULL DEFAULT ''",
        "ALTER TABLE users ADD COLUMN banner_color TEXT NOT NULL DEFAULT ''",
        "ALTER TABLE users ADD COLUMN status TEXT NOT NULL DEFAULT ''",
        "ALTER TABLE users ADD COLUMN crop TEXT NOT NULL DEFAULT ''",
        "ALTER TABLE users ADD COLUMN settings TEXT NOT NULL DEFAULT '{}'",
        "ALTER TABLE users ADD COLUMN discord_id TEXT NOT NULL DEFAULT ''",
        "ALTER TABLE users ADD COLUMN discord_name TEXT NOT NULL DEFAULT ''",
        "ALTER TABLE users ADD COLUMN discord_avatar TEXT NOT NULL DEFAULT ''",
        "ALTER TABLE users ADD COLUMN discord_access TEXT NOT NULL DEFAULT ''",
        "ALTER TABLE users ADD COLUMN discord_refresh TEXT NOT NULL DEFAULT ''",
        "ALTER TABLE users ADD COLUMN discord_expires INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE users ADD COLUMN supporter INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE users ADD COLUMN listen_secs INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE users ADD COLUMN listen_at INTEGER NOT NULL DEFAULT 0",
    ] {
        let _ = db.execute(ddl, []);
    }
    // Lịch sử donate Ko-fi (webhook). user_id để trống nếu chưa khớp được tài khoản nào.
    let _ = db.execute_batch(
        "CREATE TABLE IF NOT EXISTS kofi_donations(
           txn_id TEXT PRIMARY KEY,
           user_id INTEGER,
           amount REAL NOT NULL DEFAULT 0,
           currency TEXT NOT NULL DEFAULT '',
           from_name TEXT NOT NULL DEFAULT '',
           message TEXT NOT NULL DEFAULT '',
           created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP);",
    );
    // Dữ liệu nghe đồng bộ theo TÀI KHOẢN (lượt phát, nghe hết/bỏ qua, không thích, theo dõi nghệ sĩ, tiếp tục nghe).
    // Mọi tài khoản (kể cả admin) dùng chung một cơ chế, khóa theo user_id.
    let _ = db.execute(
        "CREATE TABLE IF NOT EXISTS user_sync(
           user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
           k TEXT NOT NULL, v TEXT NOT NULL, ts INTEGER NOT NULL DEFAULT 0,
           PRIMARY KEY(user_id, k))",
        [],
    );
    // Trạng thái tạm (dùng 1 lần) cho luồng đăng nhập Discord OAuth2.
    let _ = db.execute(
        "CREATE TABLE IF NOT EXISTS discord_states(state TEXT PRIMARY KEY, user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE, created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)",
        [],
    );
    // Nâng cấp bảo mật: token phiên cũ (lưu thô) -> băm SHA-256, người dùng không bị đăng xuất.
    let mut olds: Vec<String> = Vec::new();
    if let Ok(mut stmt) = db.prepare("SELECT token FROM sessions WHERE token NOT LIKE 'h1:%'") {
        if let Ok(rows) = stmt.query_map([], |r| r.get::<_, String>(0)) {
            for t in rows.flatten() {
                olds.push(t);
            }
        };
    }
    for t in olds {
        let _ = db.execute("UPDATE sessions SET token=?1 WHERE token=?2", params![tok_hash(&t), t]);
    }
    // Dọn dữ liệu SoundCloud cũ (nguồn "sc" đã bị gỡ).
    for t in ["favorites", "history", "playlist_tracks"] {
        let _ = db.execute(&format!("DELETE FROM {t} WHERE track_key LIKE 'sc:%'"), []);
    }
    db
}


// Tên đặt trước: không ai (trừ admin) đăng ký làm tên đăng nhập hoặc đặt làm tên hiển thị được.
// Thêm tên khác qua biến môi trường RESERVED_NAMES (cách nhau bằng dấu phẩy). ADMIN_USER cũng tự được đặt trước.
const RESERVED_DEFAULT: &[&str] = &["saki_hanami"];
fn is_reserved_name(s: &str) -> bool {
    let n = norm_name(s);
    if n.is_empty() {
        return false;
    }
    let mut list: Vec<String> = RESERVED_DEFAULT.iter().map(|x| norm_name(x)).collect();
    if let Ok(a) = std::env::var("ADMIN_USER") {
        list.push(norm_name(&a));
    }
    if let Ok(r) = std::env::var("RESERVED_NAMES") {
        list.extend(r.split(',').map(norm_name));
    }
    list.iter().any(|x| !x.is_empty() && *x == n)
}

// Tạo/cấp quyền admin từ biến môi trường ADMIN_USER (+ ADMIN_PASS nếu tài khoản chưa tồn tại).
fn bootstrap_admin(db: &Connection) {
    let Ok(user) = std::env::var("ADMIN_USER") else { return };
    let user = user.trim().to_lowercase();
    if user.len() < 3 || user.len() > 24 || !user.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        eprintln!("ADMIN_USER không hợp lệ (3-24 ký tự: chữ, số, _)");
        return;
    }
    // Không xóa tài khoản của ai. Từ bản này tên admin được đặt trước (xem is_reserved_name) nên người khác không đăng ký trùng được.
    // Nếu tên đã có người dùng thường đăng ký từ trước thì không tự cấp admin cho họ, chỉ cảnh báo để chủ server xử lý thủ công.
    if let Ok(row) = db.query_row("SELECT is_admin FROM users WHERE username=?1", [&user], |r| r.get::<_, i64>(0)) {
        if row == 0 {
            eprintln!("Cảnh báo: tên admin '{user}' đã bị tài khoản thường dùng từ trước; không tạo/ghi đè admin.");
            return;
        }
    }
    let existing: Option<(i64, String)> = db
        .query_row("SELECT id, pass_hash FROM users WHERE username=?1", [&user], |r| Ok((r.get(0)?, r.get(1)?)))
        .ok();
    match existing {
        Some((id, _)) => {
            // Còn lại chỉ có thể là tài khoản đã là admin: giữ nguyên mật khẩu hiện tại, chỉ cập nhật tên hiển thị.
            if let Ok(name) = std::env::var("ADMIN_NAME") {
                let name: String = name.trim().chars().take(24).collect();
                if !name.is_empty() {
                    let _ = db.execute("UPDATE users SET display_name=?1 WHERE id=?2", params![name, id]);
                }
            }
        }
        None => {
            let Ok(pw) = std::env::var("ADMIN_PASS") else {
                eprintln!("Thiếu ADMIN_PASS để tạo tài khoản admin");
                return;
            };
            if pw.len() < 8 || pw.len() > 128 {
                eprintln!("ADMIN_PASS phải từ 8 đến 128 ký tự");
                return;
            }
            let Ok(hash) = hash_pw(&pw) else { return };
            let code = uuid::Uuid::new_v4().simple().to_string()[..8].to_uppercase();
            let display: String = std::env::var("ADMIN_NAME")
                .ok()
                .map(|n| n.trim().chars().take(24).collect::<String>())
                .filter(|n| !n.is_empty())
                .unwrap_or_else(|| "Admin".to_string());
            let res = db.execute(
                "INSERT INTO users(username,pass_hash,display_name,code,is_admin) VALUES(?1,?2,?3,?4,1)",
                params![user, hash, display, code],
            );
            match res {
                Ok(_) => {}
                Err(e) => eprintln!("Không tạo được admin: {e}"),
            }
        }
    }
}

// Chế độ AUDIO_ONLY=1 (dùng trong app Android): chỉ phục vụ /api/yt/audio/:id trên máy này (127.0.0.1) để điện thoại
// tự lấy nhạc từ YouTube bằng IP nhà. Không có tài khoản / playlist / phòng nghe chung (những thứ đó vẫn do server chính lo).
// DB là SQLite trong bộ nhớ (trống) nên mọi yêu cầu được coi là khách, đúng với cách yt_audio đã xử lý khách.
async fn run_audio_only() {
    let st = AppState {
        db: Arc::new(Mutex::new(init_db(std::path::Path::new(":memory:")))),
        rooms: Arc::new(Mutex::new(HashMap::new())),
        notify: Arc::new(Mutex::new(HashMap::new())),
        avatar_dir: PathBuf::new(),
        bg_dir: PathBuf::new(),
        banner_dir: PathBuf::new(),
        card_dir: PathBuf::new(),
        app_dir: PathBuf::new(),
        http: reqwest::Client::builder()
            .timeout(Duration::from_secs(15))
            .user_agent("Muse/0.3")
            // Giữ kết nối tới YouTube/googlevideo lâu hơn (mặc định 90s) và gửi gói nhỏ ngay: bài kế tiếp khỏi bắt tay TLS lại.
            .tcp_nodelay(true)
            .tcp_keepalive(Duration::from_secs(30))
            .pool_idle_timeout(Duration::from_secs(300))
            .build()
            .expect("không tạo được HTTP client"),
        http_proxy: None,
        audio: Arc::new(Mutex::new(HashMap::new())),
        auth_limit: Arc::new(Mutex::new(HashMap::new())),
        ws_tickets: Arc::new(Mutex::new(HashMap::new())),
        pair_codes: Arc::new(Mutex::new(HashMap::new())),
        gateway: gateway_from_env(),
    };
    let router = yt_routes().layer(middleware::from_fn(dbg_req_timing)).with_state(st);
    let port = std::env::var("PORT").unwrap_or_else(|_| "3000".into());
    // Chỉ nhận kết nối từ chính máy này, không bao giờ mở ra mạng.
    let listener = tokio::net::TcpListener::bind(format!("127.0.0.1:{port}")).await.unwrap();
    println!("Muse audio-only: http://127.0.0.1:{port}");
    dbg_log(&format!("[START] audio-only 127.0.0.1:{port} | file log: {}", dbg_log_path().display()));
    axum::serve(listener, router.into_make_service()).await.unwrap();
}

// Các route lấy dữ liệu từ YouTube (tìm kiếm, album, nghệ sĩ, luồng âm thanh, ảnh bìa...). Dùng chung cho server chính,
// chế độ AUDIO_ONLY (Android) và chế độ cổng nhà (desktop). Tất cả đều chạy được với tư cách khách (không cần tài khoản).
fn yt_routes() -> Router<AppState> {
    yt_meta_routes().merge(yt_media_routes())
}

// Nhóm "dữ liệu danh sách": tìm kiếm, trang chủ/khám phá, album, nghệ sĩ... (JSON nhỏ, cache được).
fn yt_meta_routes() -> Router<AppState> {
    Router::new()
        .route("/api/yt/search", get(yt_search))
        .route("/api/yt/browse", get(yt_browse_music))
        .route("/api/yt/similar", get(yt_similar))
        .route("/api/yt/fresh", get(yt_fresh))
        .route("/api/yt/artists", get(yt_artists))
        .route("/api/yt/best-artists", get(yt_best_artists))
        .route("/api/yt/this-is", get(yt_this_is))
}

// Nhóm "luồng nặng": âm thanh, video, ảnh bìa qua server. Server chính không phục vụ nhóm này (app tự lấy bằng IP máy).
fn yt_media_routes() -> Router<AppState> {
    Router::new()
        .route("/api/yt/audio/:id", get(yt_audio))
        .route("/api/img", get(img_proxy))
        .route("/api/yt/video/:id", get(yt_video))
}

// Server chính chỉ lưu dữ liệu người dùng (database): các route lấy nhạc / tìm kiếm / ảnh bìa từ YouTube bị tắt và báo rõ lý do.
// Nhạc do chính app tự lấy: app desktop qua cổng nhà (GATEWAY_REMOTE), app Android qua AUDIO_ONLY – hai chế độ đó vẫn dùng yt_routes().
// Mặc định (không đặt SERVE_MUSIC) server chính chạy chế độ "meta": xem media_off_routes() và meta_cache().
fn music_off_routes() -> Router<AppState> {
    Router::new()
        .route("/api/yt/*rest", axum::routing::any(music_off))
        .route("/api/img", axum::routing::any(music_off))
}

// Chế độ mặc định của server chính ("meta"): chỉ nhóm luồng nặng bị tắt (HTTP 410), phần danh sách vẫn chạy và được cache.
fn media_off_routes() -> Router<AppState> {
    Router::new()
        .route("/api/yt/audio/*rest", axum::routing::any(music_off))
        .route("/api/yt/video/*rest", axum::routing::any(music_off))
        .route("/api/img", axum::routing::any(music_off))
}

// ===== Cache dữ liệu danh sách (metadata) trên server chính =====
// Ý tưởng: server tải danh sách từ YouTube Music giống cổng nhà rồi LƯU XUỐNG ĐĨA (data/app/meta-cache/).
//  - Còn mới (< 1 giờ): trả thẳng từ cache, không hỏi YouTube.
//  - Hết hạn: hỏi YouTube, thành công thì cập nhật cache.
//  - YouTube lỗi / chặn IP server: vẫn trả bản cũ (tối đa 7 ngày) thay vì lỗi, để trang chủ không trống.
// Khoá cache gồm đường dẫn + query và cài đặt "nội dung nhạy cảm" của người gọi (kết quả khác nhau theo cài đặt đó).
// Kết quả rỗng không được lưu (tránh "ghi nhớ" lúc YouTube chặn). Header phản hồi `x-muse-cache`: hit | stale.
const META_FRESH_SECS: u64 = 3600;
const META_STALE_SECS: u64 = 7 * 86400;
const META_MAX_BYTES: usize = 2 * 1024 * 1024;
const META_MAX_FILES: usize = 3000;
static META_WRITES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

fn meta_cache_path(st: &AppState, key: &str) -> Option<PathBuf> {
    if st.app_dir.as_os_str().is_empty() {
        return None;
    }
    let d = Sha256::digest(key.as_bytes());
    let mut name = String::with_capacity(d.len() * 2 + 5);
    for b in d.iter() {
        name.push_str(&format!("{:02x}", b));
    }
    name.push_str(".json");
    Some(st.app_dir.join("meta-cache").join(name))
}

// File: dòng 1 = thời điểm lưu (giây), phần còn lại = nội dung JSON.
fn meta_cache_read(p: &std::path::Path) -> Option<(u64, String)> {
    let s = std::fs::read_to_string(p).ok()?;
    let (t, body) = s.split_once('\n')?;
    Some((t.trim().parse().ok()?, body.to_string()))
}

fn meta_cache_write(p: &std::path::Path, body: &str) {
    let Some(dir) = p.parent() else { return };
    if std::fs::create_dir_all(dir).is_err() {
        return;
    }
    // Ghi file tạm rồi đổi tên: không bao giờ đọc phải file ghi dở.
    let tmp = p.with_extension(format!("{}.tmp", uuid::Uuid::new_v4().simple()));
    if std::fs::write(&tmp, format!("{}\n{}", now_secs(), body)).is_ok() {
        if std::fs::rename(&tmp, p).is_err() {
            let _ = std::fs::remove_file(&tmp);
        }
    }
    if META_WRITES.fetch_add(1, std::sync::atomic::Ordering::Relaxed) % 50 == 49 {
        meta_cache_prune(dir);
    }
}

// Xoá file quá 7 ngày; nếu vẫn quá nhiều file thì xoá các file cũ nhất (người dùng tìm bừa không làm đầy ổ đĩa).
fn meta_cache_prune(dir: &std::path::Path) {
    let Ok(rd) = std::fs::read_dir(dir) else { return };
    let now = SystemTime::now();
    let mut files: Vec<(SystemTime, PathBuf)> = Vec::new();
    for e in rd.flatten() {
        let p = e.path();
        let Ok(m) = e.metadata() else { continue };
        let mt = m.modified().unwrap_or(UNIX_EPOCH);
        if now.duration_since(mt).map(|d| d.as_secs() > META_STALE_SECS).unwrap_or(false) {
            let _ = std::fs::remove_file(&p);
            continue;
        }
        files.push((mt, p));
    }
    if files.len() > META_MAX_FILES {
        files.sort_by_key(|f| f.0);
        let extra = files.len() - META_MAX_FILES;
        for (_, p) in files.into_iter().take(extra) {
            let _ = std::fs::remove_file(p);
        }
    }
}

// Kết quả "rỗng": mảng rỗng, hoặc object mà mọi trường dạng mảng đều rỗng (vd. {"tracks":[]}).
fn meta_is_empty(v: &Value) -> bool {
    match v {
        Value::Null => true,
        Value::Array(a) => a.is_empty(),
        Value::Object(o) => {
            let arrs: Vec<&Vec<Value>> = o.values().filter_map(|x| x.as_array()).collect();
            !arrs.is_empty() && arrs.iter().all(|a| a.is_empty())
        }
        _ => false,
    }
}

fn meta_json(body: String, state: &'static str) -> Response {
    ([("content-type", "application/json"), ("x-muse-cache", state)], body).into_response()
}

async fn meta_cache(State(st): State<AppState>, req: Request<Body>, next: Next) -> Response {
    if req.method() != axum::http::Method::GET {
        return next.run(req).await;
    }
    let pq = req.uri().path_and_query().map(|p| p.as_str().to_string()).unwrap_or_default();
    if pq.len() > 300 {
        return next.run(req).await;
    }
    let (explicit, _) = net_prefs(&st, req.headers()).await;
    let key = format!("{}|{}", explicit as u8, pq);
    let path = meta_cache_path(&st, &key);
    let cached = path.as_deref().and_then(meta_cache_read);
    if let Some((t, body)) = &cached {
        if now_secs().saturating_sub(*t) < META_FRESH_SECS {
            return meta_json(body.clone(), "hit");
        }
    }
    let res = next.run(req).await;
    if res.status().is_success() {
        let (parts, body) = res.into_parts();
        let bytes = match axum::body::to_bytes(body, META_MAX_BYTES).await {
            Ok(b) => b,
            Err(_) => return (StatusCode::BAD_GATEWAY, "Dữ liệu từ YouTube quá lớn").into_response(),
        };
        let empty = serde_json::from_slice::<Value>(&bytes).map(|v| meta_is_empty(&v)).unwrap_or(false);
        if empty {
            // YouTube trả rỗng (thường là bị chặn): có bản cũ thì dùng bản cũ.
            if let Some((_, b)) = &cached {
                return meta_json(b.clone(), "stale");
            }
        } else if let (Some(p), Ok(txt)) = (path.as_deref(), std::str::from_utf8(&bytes)) {
            meta_cache_write(p, txt);
        }
        return Response::from_parts(parts, Body::from(bytes));
    }
    // YouTube lỗi: ưu tiên bản cũ (còn trong 7 ngày) hơn là báo lỗi.
    if let Some((t, b)) = &cached {
        if now_secs().saturating_sub(*t) < META_STALE_SECS {
            return meta_json(b.clone(), "stale");
        }
    }
    res
}

async fn music_off() -> Response {
    (StatusCode::GONE, "Server này chỉ lưu dữ liệu người dùng; nhạc do app tự lấy (đặt SERVE_MUSIC=1 nếu muốn bật lại nguồn nhạc trên server)").into_response()
}

// ===== LOG CHẨN ĐOÁN TẠM (xoá được: tìm "dbg_" trong file này) =====
// App desktop chạy server với stdout/stderr tắt, nên ghi thêm ra file. Mặc định: <thư mục tạm>/bbeat-debug.log (đặt BBEAT_LOG=đường_dẫn để đổi).
fn dbg_log_path() -> PathBuf {
    std::env::var("BBEAT_LOG").ok().filter(|x| !x.trim().is_empty()).map(PathBuf::from).unwrap_or_else(|| std::env::temp_dir().join("bbeat-debug.log"))
}
fn dbg_log(line: &str) {
    use std::io::Write;
    let ms = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis()).unwrap_or(0);
    let txt = format!("{}.{:03} {line}", ms / 1000, ms % 1000);
    eprintln!("{txt}");
    let path = dbg_log_path();
    if std::fs::metadata(&path).map(|m| m.len() > 2_000_000).unwrap_or(false) {
        let _ = std::fs::remove_file(&path);
    }
    if let Ok(mut f) = std::fs::OpenOptions::new().create(true).append(true).open(&path) {
        let _ = writeln!(f, "{txt}");
    }
}
// Đo thời gian mọi yêu cầu qua cổng nhà / audio-only: thời gian tới khi có header phản hồi (luồng nhạc thì là lúc bắt đầu phát).
// Các yêu cầu bị chuyển tiếp lên server chính (đăng nhập, giao diện, avatar...) cũng được đo ở đây → thấy Render chậm hay YouTube chậm.
async fn dbg_req_timing(req: Request<Body>, next: Next) -> Response {
    let path = req.uri().path().to_string();
    if path.starts_with("/ws") {
        return next.run(req).await;
    }
    let method = req.method().clone();
    let t = std::time::Instant::now();
    let res = next.run(req).await;
    let ms = t.elapsed().as_millis();
    let kind = if path.starts_with("/api/yt/") || path == "/api/img" { "yt" } else { "fwd" };
    dbg_log(&format!("[REQ] {kind} {method} {path} -> {} ttfb={ms}ms{}", res.status().as_u16(), if ms >= 1500 { "  <<< CHẬM" } else { "" }));
    res
}

fn gateway_client() -> reqwest::Client {
    reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(Duration::from_secs(15))
        .timeout(Duration::from_secs(240))
        .user_agent("Muse-Gateway/0.5")
        .build()
        .expect("không tạo được HTTP client cho cổng nhà")
}

// AUDIO_ONLY (Android) có thể kèm GATEWAY_REMOTE chỉ để hỏi server chính xem người dùng có bật "nội dung nhạy cảm" không.
fn gateway_from_env() -> Option<Arc<Gateway>> {
    let remote = std::env::var("GATEWAY_REMOTE").ok()?.trim().trim_end_matches('/').to_string();
    if remote.is_empty() {
        return None;
    }
    Some(Arc::new(Gateway { remote, client: gateway_client(), explicit: Mutex::new(HashMap::new()) }))
}

// Cài đặt "nội dung nhạy cảm" nằm ở tài khoản trên server chính. KHÔNG được để tìm kiếm/phát nhạc đứng chờ server chính
// (Render gói free hay ngủ đông, một lần hỏi có thể mất vài chục giây): trả ngay giá trị đã nhớ, làm mới ngầm ở nền.
const EXPLICIT_FRESH_SECS: u64 = 600;
async fn gateway_fetch_explicit(g: &Gateway, tok: &str, timeout: Duration) -> Option<bool> {
    let r = g
        .client
        .get(format!("{}/api/settings", g.remote))
        .header("authorization", format!("Bearer {tok}"))
        .timeout(timeout)
        .send()
        .await
        .ok()?;
    if !r.status().is_success() {
        return None;
    }
    let j = r.json::<Value>().await.ok()?;
    Some(j.get("explicit").and_then(|x| x.as_bool()).unwrap_or(false))
}

async fn gateway_explicit(g: &Arc<Gateway>, headers: &HeaderMap) -> bool {
    let Some(tok) = bearer(headers) else { return false };
    let key = tok_hash(&tok);
    let now = now_secs();
    let cached = g.explicit.lock().unwrap().get(&key).copied();
    if let Some((v, t)) = cached {
        if now.saturating_sub(t) >= EXPLICIT_FRESH_SECS {
            // Hết hạn: dùng giá trị cũ ngay, đánh dấu lại để 1 lần làm mới ở nền (thất bại thì 60s sau thử lại).
            g.explicit.lock().unwrap().insert(key.clone(), (v, now.saturating_sub(EXPLICIT_FRESH_SECS - 60)));
            let g2 = g.clone();
            tokio::spawn(async move {
                if let Some(nv) = gateway_fetch_explicit(&g2, &tok, Duration::from_secs(20)).await {
                    g2.explicit.lock().unwrap().insert(key, (nv, now_secs()));
                }
            });
        }
        return v;
    }
    // Lần đầu chưa có gì trong bộ nhớ: chỉ chờ tối đa 2,5s; không được thì tạm coi là false và thử lại sau 60s.
    let val = gateway_fetch_explicit(g, &tok, Duration::from_millis(2500)).await;
    let mut c = g.explicit.lock().unwrap();
    if c.len() > 500 {
        c.clear();
    }
    match val {
        Some(v) => {
            c.insert(key, (v, now));
            v
        }
        None => {
            c.insert(key, (false, now.saturating_sub(EXPLICIT_FRESH_SECS - 60)));
            false
        }
    }
}

// (cho phép nội dung nhạy cảm, dùng proxy) cho các route YouTube: server chính đọc từ DB, cổng nhà hỏi server chính.
async fn net_prefs(st: &AppState, headers: &HeaderMap) -> (bool, bool) {
    if let Some(g) = &st.gateway {
        return (gateway_explicit(g, headers).await, false);
    }
    match require(st, headers) {
        Ok(user) => user_net_prefs(st, user.id),
        Err(_) => (false, false),
    }
}

// ===== Cổng nhà =====
// GATEWAY_REMOTE=https://server-chinh  ->  chương trình này chạy trên máy ở nhà và làm 2 việc:
//   1) tự xử lý mọi route YouTube (tìm kiếm, album, nghệ sĩ, nhạc, ảnh bìa) bằng IP nhà;
//   2) chuyển tiếp mọi thứ còn lại (giao diện, đăng nhập, bạn bè, avatar/banner, phòng nghe chung, WebSocket) lên server chính.
// Giao diện được mở ở http://127.0.0.1:PORT (cùng một nguồn gốc) nên không dính CORS/CSP. Server chính chỉ còn lưu dữ liệu người dùng.
async fn run_gateway(remote: String) {
    let st = AppState {
        db: Arc::new(Mutex::new(init_db(std::path::Path::new(":memory:")))),
        rooms: Arc::new(Mutex::new(HashMap::new())),
        notify: Arc::new(Mutex::new(HashMap::new())),
        avatar_dir: PathBuf::new(),
        bg_dir: PathBuf::new(),
        banner_dir: PathBuf::new(),
        card_dir: PathBuf::new(),
        app_dir: PathBuf::new(),
        http: reqwest::Client::builder()
            .timeout(Duration::from_secs(15))
            .user_agent("Muse/0.3")
            // Giữ kết nối tới YouTube/googlevideo lâu hơn (mặc định 90s) và gửi gói nhỏ ngay: bài kế tiếp khỏi bắt tay TLS lại.
            .tcp_nodelay(true)
            .tcp_keepalive(Duration::from_secs(30))
            .pool_idle_timeout(Duration::from_secs(300))
            .build()
            .expect("không tạo được HTTP client"),
        http_proxy: None,
        audio: Arc::new(Mutex::new(HashMap::new())),
        auth_limit: Arc::new(Mutex::new(HashMap::new())),
        ws_tickets: Arc::new(Mutex::new(HashMap::new())),
        pair_codes: Arc::new(Mutex::new(HashMap::new())),
        gateway: Some(Arc::new(Gateway { remote: remote.clone(), client: gateway_client(), explicit: Mutex::new(HashMap::new()) })),
    };
    // Làm nóng kết nối tới YouTube ngay khi cổng nhà lên (DNS + TLS), để bài đầu tiên không phải trả thêm vài lượt bắt tay.
    {
        let c = st.http.clone();
        tokio::spawn(async move { warm_youtube(&c).await });
    }
    let router = yt_routes()
        .route("/api/local/open", post(local_open))
        .route("/api/local/list", get(local_list))
        .route("/api/local/log", get(local_log))
        .route("/api/local/file", get(local_file))
        .route("/ws/:room", get(gw_ws))
        .route("/ws-user", get(gw_ws))
        .fallback(gw_http)
        .layer(middleware::from_fn(dbg_req_timing))
        .with_state(st);
    let port = std::env::var("PORT").unwrap_or_else(|_| "38417".into());
    let bind = std::env::var("BIND").ok().map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).unwrap_or_else(|| "127.0.0.1".into());
    let listener = match tokio::net::TcpListener::bind(format!("{bind}:{port}")).await {
        Ok(l) => l,
        Err(e) => {
            eprintln!("Không mở được cổng {bind}:{port}: {e} (có thể cổng nhà đã chạy sẵn?)");
            return;
        }
    };
    println!("Muse cổng nhà: http://{bind}:{port}  ->  server chính {remote}");
    dbg_log(&format!("[START] cổng nhà {bind}:{port} -> {remote} | file log: {}", dbg_log_path().display()));
    axum::serve(listener, router.into_make_service_with_connect_info::<std::net::SocketAddr>()).await.unwrap();
}

// Mở sẵn kết nối tới các máy chủ YouTube dùng khi lấy nhạc; kết quả bỏ qua (lỗi mạng cũng không sao).
async fn warm_youtube(c: &reqwest::Client) {
    let one = |u: &'static str| async move {
        let _ = c.get(u).timeout(Duration::from_secs(6)).send().await;
    };
    tokio::join!(one("https://youtubei.googleapis.com/generate_204"), one("https://www.youtube.com/generate_204"));
}

// ===== Thư mục nhạc của app (chỉ có ở cổng nhà chạy trên máy người dùng) =====
// "Thêm nhạc" mở thư mục này trong trình quản lý file; người dùng thả file nhạc vào, giao diện tự quét và thêm vào Tải xuống.
// Mặc định: <Nhạc của người dùng>\BBeat (đặt MUSE_MUSIC_DIR để đổi). Chỉ nhận yêu cầu gọi tới 127.0.0.1/localhost.
const LOCAL_AUDIO_EXT: &[&str] = &["mp3", "m4a", "aac", "flac", "wav", "ogg", "oga", "opus", "webm", "wma", "mka", "mp4"];

fn local_music_dir() -> PathBuf {
    if let Ok(d) = std::env::var("MUSE_MUSIC_DIR") {
        if !d.trim().is_empty() {
            return PathBuf::from(d.trim());
        }
    }
    let home = std::env::var("USERPROFILE").or_else(|_| std::env::var("HOME")).unwrap_or_else(|_| ".".into());
    PathBuf::from(home).join("Music").join("BBeat")
}

fn local_host_ok(h: &HeaderMap) -> bool {
    let host = h.get("host").and_then(|v| v.to_str().ok()).unwrap_or("");
    ["127.0.0.1", "localhost", "[::1]"].iter().any(|p| host.strip_prefix(p).map(|r| r.is_empty() || r.starts_with(':')).unwrap_or(false))
}

fn local_ext_of(name: &str) -> Option<String> {
    let e = name.rsplit_once('.')?.1.to_lowercase();
    if LOCAL_AUDIO_EXT.contains(&e.as_str()) {
        Some(e)
    } else {
        None
    }
}

async fn local_open(headers: HeaderMap) -> Response {
    if !local_host_ok(&headers) {
        return StatusCode::FORBIDDEN.into_response();
    }
    let dir = local_music_dir();
    if let Err(e) = std::fs::create_dir_all(&dir) {
        return (StatusCode::INTERNAL_SERVER_ERROR, format!("Không tạo được thư mục nhạc: {e}")).into_response();
    }
    #[cfg(windows)]
    let r = std::process::Command::new("explorer.exe").arg(&dir).spawn();
    #[cfg(target_os = "macos")]
    let r = std::process::Command::new("open").arg(&dir).spawn();
    #[cfg(all(unix, not(target_os = "macos")))]
    let r = std::process::Command::new("xdg-open").arg(&dir).spawn();
    match r {
        Ok(_) => Json(json!({ "dir": dir.to_string_lossy() })).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, format!("Không mở được thư mục: {e}")).into_response(),
    }
}

// Danh sách file nhạc trong thư mục (đi sâu tối đa 3 cấp thư mục con): [{name, size, mtime(ms)}], name là đường dẫn tương đối dùng dấu /.
// Xem log chẩn đoán ngay trong trình duyệt: mở http://127.0.0.1:38417/api/local/log (chỉ máy này; trả 40KB cuối của file log).
// Mở được trang này = đang chạy đúng bản server có log; nếu trang báo lỗi / 404 thì app vẫn đang chạy bản server cũ.
async fn local_log(headers: HeaderMap) -> Response {
    if !local_host_ok(&headers) {
        return StatusCode::FORBIDDEN.into_response();
    }
    let path = dbg_log_path();
    let txt = match std::fs::read(&path) {
        Ok(b) => {
            let from = b.len().saturating_sub(40_000);
            format!("# file: {}\n# {} byte, hiện phần cuối\n{}", path.display(), b.len(), String::from_utf8_lossy(&b[from..]))
        }
        Err(e) => format!("# chưa có log tại {} ({e})\n# server đang chạy đúng bản mới; hãy dùng app vài thao tác rồi tải lại trang này.", path.display()),
    };
    ([(header::CONTENT_TYPE, "text/plain; charset=utf-8"), (header::CACHE_CONTROL, "no-store")], txt).into_response()
}

async fn local_list(headers: HeaderMap) -> Response {
    if !local_host_ok(&headers) {
        return StatusCode::FORBIDDEN.into_response();
    }
    let root = local_music_dir();
    let out = tokio::task::spawn_blocking(move || {
        let mut files: Vec<Value> = Vec::new();
        let mut stack: Vec<(PathBuf, String, u8)> = vec![(root, String::new(), 0)];
        while let Some((dir, rel, depth)) = stack.pop() {
            let Ok(rd) = std::fs::read_dir(&dir) else { continue };
            for ent in rd.flatten() {
                let name = ent.file_name().to_string_lossy().into_owned();
                let Ok(md) = ent.metadata() else { continue };
                let relname = if rel.is_empty() { name.clone() } else { format!("{rel}/{name}") };
                if md.is_dir() {
                    if depth < 3 {
                        stack.push((ent.path(), relname, depth + 1));
                    }
                } else if md.is_file() && local_ext_of(&name).is_some() {
                    let mtime = md.modified().ok().and_then(|t| t.duration_since(UNIX_EPOCH).ok()).map(|d| d.as_millis() as u64).unwrap_or(0);
                    files.push(json!({ "name": relname, "size": md.len(), "mtime": mtime }));
                }
            }
        }
        files.truncate(5000);
        files
    })
    .await
    .unwrap_or_default();
    Json(Value::Array(out)).into_response()
}

#[derive(Deserialize)]
struct LocalQ {
    name: Option<String>,
}

async fn local_file(headers: HeaderMap, Query(q): Query<LocalQ>) -> Response {
    if !local_host_ok(&headers) {
        return StatusCode::FORBIDDEN.into_response();
    }
    let name = q.name.unwrap_or_default();
    let Some(ext) = local_ext_of(&name) else { return StatusCode::BAD_REQUEST.into_response() };
    if name.contains('\\') || name.contains(':') || name.split('/').any(|seg| seg.is_empty() || seg == "." || seg == "..") {
        return StatusCode::BAD_REQUEST.into_response();
    }
    let root = local_music_dir();
    let (Ok(root_c), Ok(path_c)) = (root.canonicalize(), root.join(&name).canonicalize()) else { return StatusCode::NOT_FOUND.into_response() };
    if !path_c.starts_with(&root_c) || !path_c.is_file() {
        return StatusCode::NOT_FOUND.into_response();
    }
    let Ok(bytes) = tokio::fs::read(&path_c).await else { return StatusCode::NOT_FOUND.into_response() };
    let mime = match ext.as_str() {
        "mp3" => "audio/mpeg",
        "m4a" | "mp4" => "audio/mp4",
        "aac" => "audio/aac",
        "flac" => "audio/flac",
        "wav" => "audio/wav",
        "ogg" | "oga" | "opus" => "audio/ogg",
        "webm" => "audio/webm",
        "wma" => "audio/x-ms-wma",
        _ => "audio/x-matroska",
    };
    ([(header::CONTENT_TYPE, mime), (header::CACHE_CONTROL, "no-store")], bytes).into_response()
}

fn gw_hop(name: &str) -> bool {
    matches!(
        name,
        "host" | "connection" | "keep-alive" | "proxy-authenticate" | "proxy-authorization" | "te" | "trailer" | "transfer-encoding" | "upgrade" | "content-length"
    )
}

// Trang chờ: khi server chính chưa sẵn sàng (Render đang ngủ/khởi động, mất mạng...) thì cửa sổ app không kẹt ở trang lỗi,
// mà tự nạp lại mỗi 3 giây cho tới khi vào được.
const GW_RETRY_HTML: &str = r#"<!doctype html><html lang="vi"><head><meta charset="utf-8"><title>BBeat</title><meta name="viewport" content="width=device-width,initial-scale=1"><style>html,body{height:100%;margin:0;background:#07070b;color:#ededf2;font-family:system-ui,sans-serif}body{display:flex;flex-direction:column;align-items:center;justify-content:center;gap:14px;text-align:center}.s{width:34px;height:34px;border:3px solid #333;border-top-color:#8ecbff;border-radius:50%;animation:r 1s linear infinite}@keyframes r{to{transform:rotate(360deg)}}p{margin:0;color:#a6a6b4;font-size:14px}</style></head><body><div class="s"></div><b>Đang kết nối tới máy chủ…</b><p>Máy chủ có thể đang khởi động, app sẽ tự vào khi sẵn sàng.</p><script>setTimeout(function(){location.reload()},3000)</script></body></html>"#;

fn gw_retry_page() -> Response {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        [("content-type", "text/html; charset=utf-8"), ("cache-control", "no-store")],
        GW_RETRY_HTML,
    )
        .into_response()
}

// Chuyển tiếp nguyên một yêu cầu HTTP lên server chính rồi trả lại phản hồi (không giải nén, không tự theo chuyển hướng).
async fn gw_http(State(st): State<AppState>, ConnectInfo(addr): ConnectInfo<std::net::SocketAddr>, req: Request<Body>) -> Response {
    let Some(g) = st.gateway.clone() else { return StatusCode::NOT_FOUND.into_response() };
    let (parts, body) = req.into_parts();
    // Mã ghép admin: chỉ máy mở cổng nhà (127.0.0.1) mới xin được; cổng nhà gắn ADMIN_PAIR_KEY (env của máy này) khi chuyển lên server chính.
    let is_pair = parts.method == axum::http::Method::POST && parts.uri.path() == "/api/admin/pair-code";
    if is_pair && !is_local_request(&addr, &parts.headers) {
        return (StatusCode::FORBIDDEN, "Chỉ tạo được mã ghép từ chính máy chạy cổng nhà (mở http://127.0.0.1)").into_response();
    }
    if parts.method == axum::http::Method::GET && matches!(parts.uri.path(), "/" | "/index.html") && gw_local_ui() {
        return (
            [("content-type", "text/html; charset=utf-8"), ("cache-control", "no-cache")],
            EMBED_INDEX,
        )
            .into_response();
    }
    let pq = parts.uri.path_and_query().map(|p| p.as_str()).unwrap_or("/");
    let url = format!("{}{}", g.remote, pq);
    let is_nav = parts.method == axum::http::Method::GET
        && parts
            .headers
            .get("accept")
            .and_then(|v| v.to_str().ok())
            .map(|a| a.contains("text/html"))
            .unwrap_or(false);
    let bytes = match axum::body::to_bytes(body, 16 * 1024 * 1024).await {
        Ok(b) => b,
        Err(_) => return StatusCode::PAYLOAD_TOO_LARGE.into_response(),
    };
    let mut rb = g.client.request(parts.method.clone(), &url);
    for (k, v) in parts.headers.iter() {
        let n = k.as_str();
        if gw_hop(n) || n == "origin" || n == "referer" || n == "x-pair-key" {
            continue;
        }
        rb = rb.header(k, v);
    }
    if is_pair {
        if let Ok(k) = std::env::var("ADMIN_PAIR_KEY") {
            let k = k.trim().to_string();
            if !k.is_empty() {
                rb = rb.header("x-pair-key", k);
            }
        }
    }
    if !bytes.is_empty() {
        rb = rb.body(bytes);
    }
    match rb.send().await {
        Ok(r) => {
            let status = r.status();
            if is_nav && matches!(status.as_u16(), 502 | 503 | 504) {
                return gw_retry_page();
            }
            let headers = r.headers().clone();
            match r.bytes().await {
                Ok(b) => {
                    let mut out = Response::new(Body::from(b));
                    *out.status_mut() = status;
                    for (k, v) in headers.iter() {
                        if gw_hop(k.as_str()) {
                            continue;
                        }
                        out.headers_mut().append(k.clone(), v.clone());
                    }
                    out
                }
                Err(e) => {
                    if is_nav {
                        return gw_retry_page();
                    }
                    (StatusCode::BAD_GATEWAY, format!("Cổng nhà: lỗi đọc phản hồi từ server chính ({e})")).into_response()
                }
            }
        }
        Err(e) => {
            if is_nav {
                return gw_retry_page();
            }
            {
                let kind = if e.is_timeout() { "quá thời gian chờ" } else if e.is_connect() { "không kết nối được (server Render đang ngủ/tắt?)" } else if e.is_request() { "lỗi khi gửi dữ liệu lên" } else { "lỗi khác" };
                let mut chain = String::new();
                let mut src = std::error::Error::source(&e);
                while let Some(s) = src { chain.push_str(&format!(" → {s}")); src = s.source(); }
                (StatusCode::BAD_GATEWAY, format!("Cổng nhà: không nối được server chính {} [{kind}] ({e}){chain}", g.remote)).into_response()
            }
        }
    }
}

// Chuyển tiếp WebSocket (phòng nghe chung, thông báo mời) lên server chính, giữ nguyên đường dẫn + vé (?ticket=).
async fn gw_ws(State(st): State<AppState>, ws: WebSocketUpgrade, uri: Uri) -> Response {
    use tokio_tungstenite::tungstenite::Message as TM;
    let Some(g) = st.gateway.clone() else { return StatusCode::NOT_FOUND.into_response() };
    let pq = uri.path_and_query().map(|p| p.as_str()).unwrap_or("/").to_string();
    let base = g.remote.replacen("https://", "wss://", 1).replacen("http://", "ws://", 1);
    let url = format!("{base}{pq}");
    ws.on_upgrade(move |client: WebSocket| async move {
        let Ok((up, _)) = tokio_tungstenite::connect_async(url.as_str()).await else { return };
        let (mut up_tx, mut up_rx) = up.split();
        let (mut c_tx, mut c_rx) = client.split();
        let to_up = async {
            while let Some(Ok(m)) = c_rx.next().await {
                let t = match m {
                    Message::Text(s) => TM::Text(s),
                    Message::Binary(b) => TM::Binary(b),
                    Message::Close(_) => break,
                    _ => continue,
                };
                if up_tx.send(t).await.is_err() {
                    break;
                }
            }
        };
        let to_client = async {
            while let Some(Ok(m)) = up_rx.next().await {
                let t = match m {
                    TM::Text(s) => Message::Text(s),
                    TM::Binary(b) => Message::Binary(b),
                    TM::Close(_) => break,
                    _ => continue,
                };
                if c_tx.send(t).await.is_err() {
                    break;
                }
            }
        };
        tokio::select! { _ = to_up => {}, _ = to_client => {} }
    })
}

#[tokio::main]
async fn main() {
    if std::env::var("AUDIO_ONLY").map(|v| v.trim() == "1").unwrap_or(false) {
        run_audio_only().await;
        return;
    }
    if let Some(remote) = std::env::var("GATEWAY_REMOTE").ok().map(|v| v.trim().trim_end_matches('/').to_string()).filter(|v| !v.is_empty()) {
        run_gateway(remote).await;
        return;
    }
    let data_dir = PathBuf::from(std::env::var("DATA_DIR").unwrap_or_else(|_| "data".into()));
    let avatar_dir = data_dir.join("avatars");
    let bg_dir = data_dir.join("backgrounds");
    let banner_dir = data_dir.join("banners");
    let card_dir = data_dir.join("cards");
    let app_dir = data_dir.join("app");
    std::fs::create_dir_all(&avatar_dir).unwrap();
    std::fs::create_dir_all(&bg_dir).unwrap();
    std::fs::create_dir_all(&banner_dir).unwrap();
    std::fs::create_dir_all(&card_dir).unwrap();
    std::fs::create_dir_all(&app_dir).unwrap();
    let static_dir = std::env::var("STATIC_DIR").unwrap_or_else(|_| "static".into());

    let db_conn = init_db(&data_dir.join("muse.db"));
    bootstrap_admin(&db_conn);
    let st = AppState {
        db: Arc::new(Mutex::new(db_conn)),
        rooms: Arc::new(Mutex::new(HashMap::new())),
        notify: Arc::new(Mutex::new(HashMap::new())),
        avatar_dir: avatar_dir.clone(),
        bg_dir: bg_dir.clone(),
        banner_dir: banner_dir.clone(),
        card_dir: card_dir.clone(),
        app_dir: app_dir.clone(),
        http: reqwest::Client::builder()
            .timeout(Duration::from_secs(15))
            .user_agent("Muse/0.3")
            // Giữ kết nối tới YouTube/googlevideo lâu hơn (mặc định 90s) và gửi gói nhỏ ngay: bài kế tiếp khỏi bắt tay TLS lại.
            .tcp_nodelay(true)
            .tcp_keepalive(Duration::from_secs(30))
            .pool_idle_timeout(Duration::from_secs(300))
            .build()
            .expect("không tạo được HTTP client"),
        http_proxy: std::env::var("YT_PROXY")
            .ok()
            .map(|p| p.trim().to_string())
            .filter(|p| !p.is_empty())
            .and_then(|p| reqwest::Proxy::all(p).ok())
            .and_then(|px| {
                reqwest::Client::builder()
                    .proxy(px)
                    .timeout(Duration::from_secs(20))
                    .user_agent("Muse/0.3")
                    .build()
                    .ok()
            }),
        audio: Arc::new(Mutex::new(HashMap::new())),
        auth_limit: Arc::new(Mutex::new(HashMap::new())),
        ws_tickets: Arc::new(Mutex::new(HashMap::new())),
        pair_codes: Arc::new(Mutex::new(HashMap::new())),
        gateway: None,
    };

    // SERVE_MUSIC: "1" = server chính phục vụ cả nhạc như cổng nhà; "0" = tắt hết (410);
    // để trống (mặc định) = chế độ "meta": danh sách/tìm kiếm có cache, còn luồng âm thanh/video/ảnh bìa vẫn do app tự lấy.
    let music_mode = match std::env::var("SERVE_MUSIC").unwrap_or_default().trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "on" | "yes" => "full",
        "0" | "false" | "off" | "no" => "off",
        _ => "meta",
    };
    let music_router = match music_mode {
        "full" => yt_routes(),
        "off" => music_off_routes(),
        _ => yt_meta_routes().layer(middleware::from_fn_with_state(st.clone(), meta_cache)).merge(media_off_routes()),
    };
    let router = Router::new()
        .route("/api/config", get(config))
        .route("/api/register", post(register))
        .route("/api/login", post(login))
        .route("/api/logout", post(logout))
        .route("/api/ws-ticket", post(ws_ticket))
        .route("/api/admin/pair-code", post(admin_pair_code))
        .route("/api/kofi/webhook", post(kofi_webhook))
        .route("/api/qr/new", post(qr_new))
        .route("/api/qr/poll/:id", get(qr_poll))
        .route("/api/qr/approve", post(qr_approve))
        .route("/api/admin/supporter", post(admin_set_supporter))
        .route("/api/admin/users/reset-avatar", post(admin_reset_avatar))
        .route("/api/pair/redeem", post(pair_redeem))
        .route("/api/pair/create", post(pair_create))
        .route("/api/me", get(me))
        .route("/api/profile", post(update_profile))
        .route("/api/account/name", post(account_name))
        .route("/api/account/password", post(account_password))
        .route("/api/account", axum::routing::delete(account_delete))
        .route("/api/settings", get(settings_get).post(settings_set))
        .route("/api/discord/status", get(discord_status))
        .route("/api/discord/link", post(discord_link).delete(discord_unlink))
        .route("/api/discord/callback", get(discord_callback))
        .route("/api/discord/token", get(discord_token))
        .route("/api/yt-import/url", post(yt_import_url))
        .route("/api/pl-import/url", post(pl_import::pl_import_url))
        .route("/api/backup", get(backup_export))
        .route("/api/restore", post(backup_restore).layer(DefaultBodyLimit::max(10 * 1024 * 1024)))
        .route("/api/import", post(import_playlists).layer(DefaultBodyLimit::max(10 * 1024 * 1024)))
        .route("/api/avatar", post(upload_avatar))
        .route("/api/background", post(upload_bg).layer(DefaultBodyLimit::max(10_500_000)))
        .route("/api/banner", post(upload_banner).delete(remove_banner).layer(DefaultBodyLimit::max(10 * 1024 * 1024)))
        .route("/api/admin/icon", post(admin_icon).delete(admin_icon_reset))
        .route("/app-icon.png", get(serve_icon))
        .route("/api/card", post(upload_card).layer(DefaultBodyLimit::max(3 * 1024 * 1024)))
        .route("/api/u/:username", get(card_user))
        .route("/api/fans/:id", get(fans_get))
        .route("/api/listen", post(listen_post))
        .route("/api/lv/:id", get(level_get))
        .route("/u/:username", get(card_page))
        .route("/.well-known/assetlinks.json", get(assetlinks))
        .merge(music_router)
        .route("/api/friends", get(list_friends).post(add_friend))
        .route("/api/friends/requests", get(list_requests))
        .route("/api/friends/respond", post(respond_request))
        .route("/api/friends/:id", axum::routing::delete(remove_friend))
        .route("/api/favorites", get(fav_list).post(fav_toggle))
        .route("/api/history", get(hist_list).post(hist_add).delete(hist_clear))
        .route("/api/comments", get(comments_list).post(comments_add))
        .route("/api/comments/:id", axum::routing::delete(comments_delete))
        .route("/api/sync", get(sync_get).post(sync_post))
        .route("/api/recommend", get(recommend))
        .route("/api/blend/:fid", get(blend))
        .route("/api/playlists", get(pl_list).post(pl_create))
        .route("/api/playlists/:id", get(pl_get).delete(pl_delete))
        .route("/api/playlists/:id/tracks", post(pl_add))
        .route("/api/playlists/:id/tracks/:key", axum::routing::delete(pl_remove))
        .route("/api/time", get(time_now))
        .route("/api/invite", post(invite))
        .route("/ws-user", get(ws_user))
        .route("/ws/:room", get(ws_handler))
        .nest_service("/avatars", ServeDir::new(avatar_dir))
        .nest_service("/backgrounds", ServeDir::new(bg_dir))
        .nest_service("/banners", ServeDir::new(banner_dir))
        .nest_service("/cards", ServeDir::new(card_dir))
        .fallback_service(ServeDir::new(static_dir))
        .layer(DefaultBodyLimit::max(10 * 1024 * 1024))
        .layer(middleware::from_fn(security_headers))
        .with_state(st);

    let port = std::env::var("PORT").unwrap_or_else(|_| "3000".into());
    // BIND=127.0.0.1 -> chỉ nhận kết nối từ chính máy này (dùng khi đã có Caddy / Tailscale Serve / Funnel phía trước),
    // nên IP mạng LAN và cổng không bị lộ ra ngoài. Mặc định 0.0.0.0 để không phá cách chạy hiện có (Docker, điện thoại cùng Wi-Fi).
    let bind = std::env::var("BIND").ok().map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).unwrap_or_else(|| "0.0.0.0".into());
    let listener = tokio::net::TcpListener::bind(format!("{bind}:{port}")).await.unwrap();
    println!("Muse server: http://localhost:{port} (lắng nghe {bind})");
    match music_mode {
        "off" => println!("Nguồn nhạc (YouTube) đã tắt trên server này (SERVE_MUSIC=0): chỉ lưu dữ liệu người dùng."),
        "meta" => println!("Server chính ở chế độ meta: danh sách/tìm kiếm YouTube có cache trên đĩa; luồng âm thanh/video do app tự lấy (SERVE_MUSIC=1 để phục vụ cả nhạc)."),
        _ => {}
    }
    axum::serve(listener, router.into_make_service_with_connect_info::<std::net::SocketAddr>()).await.unwrap();
}

// Ảnh bìa YouTube/Google qua server: dự phòng cho app khi mạng điện thoại chặn i.ytimg.com. Chỉ nhận https và đúng các tên miền ảnh này.
#[derive(Deserialize)]
struct ImgQ {
    u: String,
}

// ===== Ảnh bìa: cache RAM + đĩa, nguồn gốc chạy trước, các nguồn dự phòng chạy đua (giống bản Android) =====
struct ImgRam {
    map: HashMap<String, (String, Bytes)>,
    order: std::collections::VecDeque<String>,
    bytes: usize,
}
const IMG_RAM_MAX: usize = 8 * 1024 * 1024;

fn img_ram() -> &'static Mutex<ImgRam> {
    static R: std::sync::OnceLock<Mutex<ImgRam>> = std::sync::OnceLock::new();
    R.get_or_init(|| Mutex::new(ImgRam { map: HashMap::new(), order: Default::default(), bytes: 0 }))
}

fn img_ram_get(key: &str) -> Option<(String, Bytes)> {
    img_ram().lock().ok()?.map.get(key).cloned()
}

fn img_ram_put(key: &str, ct: &str, b: &Bytes) {
    let Ok(mut r) = img_ram().lock() else { return };
    if r.map.contains_key(key) {
        return;
    }
    r.bytes += b.len() + 64;
    r.map.insert(key.to_string(), (ct.to_string(), b.clone()));
    r.order.push_back(key.to_string());
    while r.bytes > IMG_RAM_MAX {
        let Some(old) = r.order.pop_front() else { break };
        if let Some((_, ob)) = r.map.remove(&old) {
            r.bytes = r.bytes.saturating_sub(ob.len() + 64);
        }
    }
}

// Thư mục cache ảnh trên đĩa: MUSE_CACHE_DIR, không có thì thư mục cache của hệ điều hành (không ghi cạnh file .exe vì có thể bị khoá quyền).
fn img_cache_dir() -> Option<PathBuf> {
    static D: std::sync::OnceLock<Option<PathBuf>> = std::sync::OnceLock::new();
    D.get_or_init(|| {
        let base = std::env::var_os("MUSE_CACHE_DIR")
            .map(PathBuf::from)
            .or_else(|| std::env::var_os("LOCALAPPDATA").map(|p| PathBuf::from(p).join("BBeat")))
            .or_else(|| std::env::var_os("XDG_CACHE_HOME").map(|p| PathBuf::from(p).join("bbeat")))
            .or_else(|| std::env::var_os("HOME").map(|p| PathBuf::from(p).join(".cache").join("bbeat")))
            .unwrap_or_else(|| std::env::temp_dir().join("bbeat"));
        let d = base.join("img");
        std::fs::create_dir_all(&d).ok()?;
        Some(d)
    })
    .clone()
}

fn img_key(url: &str) -> String {
    Sha256::digest(url.as_bytes()).iter().map(|b| format!("{b:02x}")).collect()
}

async fn img_disk_get(key: &str) -> Option<(String, Bytes)> {
    let f = img_cache_dir()?.join(key);
    let raw = tokio::fs::read(&f).await.ok()?;
    let nl = raw.iter().position(|&c| c == b'\n')?;
    let ct = String::from_utf8(raw[..nl].to_vec()).ok()?;
    let body = &raw[nl + 1..];
    if !ct.starts_with("image/") || body.is_empty() {
        let _ = tokio::fs::remove_file(&f).await;
        return None;
    }
    Some((ct, Bytes::copy_from_slice(body)))
}

async fn img_disk_put(key: &str, ct: &str, b: &Bytes) {
    static N: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    let Some(dir) = img_cache_dir() else { return };
    let mut out = Vec::with_capacity(ct.len() + 1 + b.len());
    out.extend_from_slice(ct.as_bytes());
    out.push(b'\n');
    out.extend_from_slice(b);
    if tokio::fs::write(dir.join(key), out).await.is_err() {
        return;
    }
    // Giữ thư mục gọn: cứ 50 ảnh mới thì kiểm tra, quá 800 file thì xoá 200 file cũ nhất.
    if N.fetch_add(1, std::sync::atomic::Ordering::Relaxed) % 50 == 49 {
        if let Ok(mut rd) = tokio::fs::read_dir(&dir).await {
            let mut all: Vec<(SystemTime, PathBuf)> = Vec::new();
            while let Ok(Some(e)) = rd.next_entry().await {
                if let Ok(m) = e.metadata().await {
                    all.push((m.modified().unwrap_or(UNIX_EPOCH), e.path()));
                }
            }
            if all.len() > 800 {
                all.sort();
                for (_, f) in all.into_iter().take(200) {
                    let _ = tokio::fs::remove_file(f).await;
                }
            }
        }
    }
}

const IMG_UA: &str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36";

// Tải một URL ảnh: Ok((content-type, bytes)) nếu là ảnh hợp lệ, Err(ghi chú) nếu không.
async fn img_fetch(client: &reqwest::Client, url: &str, timeout: Duration) -> Result<(String, Bytes), String> {
    let r = client
        .get(url)
        .header("user-agent", IMG_UA)
        .header("accept", "image/avif,image/webp,image/apng,image/*,*/*;q=0.8")
        .timeout(timeout)
        .send()
        .await
        .map_err(|e| format!("[{}: lỗi kết nối ({e})] ", reqwest::Url::parse(url).ok().and_then(|u| u.host_str().map(|h| h.to_string())).unwrap_or_default()))?;
    let host = r.url().host_str().unwrap_or("?").to_string();
    if !r.status().is_success() {
        return Err(format!("[{host}: HTTP {}] ", r.status().as_u16()));
    }
    let ct = r.headers().get(header::CONTENT_TYPE).and_then(|v| v.to_str().ok()).unwrap_or("").split(';').next().unwrap_or("").trim().to_string();
    if !ct.starts_with("image/") {
        return Err(format!("[{host}: không phải ảnh ({ct})] "));
    }
    match r.bytes().await {
        Ok(b) if !b.is_empty() && b.len() < 8 * 1024 * 1024 => Ok((ct, b)),
        _ => Err(format!("[{host}: ảnh rỗng/quá lớn] ")),
    }
}

fn img_resp(ct: &str, b: Bytes) -> Response {
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, ct)
        .header(header::CACHE_CONTROL, "public, max-age=86400")
        .body(Body::from(b))
        .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
}

async fn img_proxy(State(st): State<AppState>, Query(q): Query<ImgQ>) -> Response {
    let parsed = match reqwest::Url::parse(&q.u).ok().filter(|p| p.scheme() == "https") {
        Some(p) => p,
        None => return StatusCode::BAD_REQUEST.into_response(),
    };
    let host = parsed.host_str().map(|h| h.to_ascii_lowercase()).unwrap_or_default();
    if !(host == "i.ytimg.com" || host.ends_with(".ggpht.com") || host.ends_with(".googleusercontent.com")) {
        return StatusCode::BAD_REQUEST.into_response();
    }
    // 1) RAM rồi đĩa: ảnh đã xem rồi hiện ngay, không hỏi mạng.
    let key = img_key(&q.u);
    if let Some((ct, b)) = img_ram_get(&key) {
        return img_resp(&ct, b);
    }
    if let Some((ct, b)) = img_disk_get(&key).await {
        img_ram_put(&key, &ct, &b);
        return img_resp(&ct, b);
    }
    // 2) Nguồn gốc (IP của máy này): link gốc, rồi các tên miền ảnh khác của YouTube, rồi hạ dần chất lượng (hq720/maxres không phải video nào cũng có).
    let mut cands: Vec<String> = vec![q.u.clone()];
    if host == "i.ytimg.com" {
        for alt in ["i1.ytimg.com", "i9.ytimg.com", "img.youtube.com"] {
            let mut u = parsed.clone();
            if u.set_host(Some(alt)).is_ok() {
                cands.push(u.to_string());
            }
        }
        if let Some(pos) = q.u.rfind('/') {
            let (base, tail) = q.u.split_at(pos + 1);
            if base.contains("/vi/") && tail.ends_with(".jpg") {
                for n in ["hqdefault.jpg", "mqdefault.jpg", "default.jpg"] {
                    if tail != n {
                        cands.push(format!("{base}{n}"));
                    }
                }
            }
        }
    }
    // 3) Dự phòng (wsrv.nl, DuckDuckGo, weserv.nl) chỉ bắt đầu SAU KHI nguồn gốc xong mà lỗi (hoặc sau 1,5 giây), chạy song song, cái nào xong trước thắng.
    let enc: String = reqwest::Url::parse_with_params("https://x/", &[("u", q.u.as_str())])
        .ok()
        .and_then(|u| u.query().map(|s| s.trim_start_matches("u=").to_string()))
        .unwrap_or_default();
    let fallbacks = [
        format!("https://wsrv.nl/?url={enc}"),
        format!("https://external-content.duckduckgo.com/iu/?u={enc}&f=1"),
        format!("https://images.weserv.nl/?url={enc}"),
    ];
    type Msg = Result<(String, Bytes), String>;
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Msg>();
    let direct_done = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut tasks: Vec<tokio::task::JoinHandle<()>> = Vec::new();
    {
        let (tx, client, done) = (tx.clone(), st.http.clone(), direct_done.clone());
        tasks.push(tokio::spawn(async move {
            let mut notes = String::new();
            for (i, u) in cands.iter().enumerate() {
                match img_fetch(&client, u, Duration::from_secs(if i == 0 { 5 } else { 8 })).await {
                    Ok(x) => {
                        done.store(true, std::sync::atomic::Ordering::Relaxed);
                        let _ = tx.send(Ok(x));
                        return;
                    }
                    Err(n) => notes.push_str(&n),
                }
            }
            done.store(true, std::sync::atomic::Ordering::Relaxed);
            let _ = tx.send(Err(notes));
        }));
    }
    for (i, u) in fallbacks.into_iter().enumerate() {
        let (tx, client, done) = (tx.clone(), st.http.clone(), direct_done.clone());
        tasks.push(tokio::spawn(async move {
            let t0 = std::time::Instant::now();
            while !done.load(std::sync::atomic::Ordering::Relaxed) && t0.elapsed() < Duration::from_millis(1500) {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            tokio::time::sleep(Duration::from_millis(150 * i as u64)).await;
            let _ = tx.send(img_fetch(&client, &u, Duration::from_secs(8)).await);
        }));
    }
    drop(tx);
    let mut notes = String::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    let mut got: Option<(String, Bytes)> = None;
    while let Ok(Some(m)) = tokio::time::timeout_at(deadline, rx.recv()).await {
        match m {
            Ok(x) => {
                got = Some(x);
                break;
            }
            Err(n) => notes.push_str(&n),
        }
    }
    for t in tasks {
        t.abort();
    }
    match got {
        Some((ct, b)) => {
            img_ram_put(&key, &ct, &b);
            img_disk_put(&key, &ct, &b).await;
            img_resp(&ct, b)
        }
        None => (StatusCode::BAD_GATEWAY, format!("Không tải được ảnh: {notes}")).into_response(),
    }
}

async fn config(State(st): State<AppState>) -> Json<serde_json::Value> {
    let icon = std::fs::metadata(st.app_dir.join("icon.png"))
        .and_then(|m| m.modified())
        .ok()
        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map(|d| format!("/app-icon.png?v={}", d.as_secs()))
        .unwrap_or_default();
    Json(json!({
        "donate_url": std::env::var("DONATE_URL").unwrap_or_default(),
        "icon_url": icon,
        "version": env!("CARGO_PKG_VERSION"),
        "proxy_available": st.http_proxy.is_some(),
        "discord_app_id": std::env::var("DISCORD_CLIENT_ID").unwrap_or_default(),
        "discord_login": discord_cfg().is_some(),
        "artist_aliases": alias_map(),
    }))
}

#[derive(Deserialize)]
struct AuthReq {
    username: String,
    password: String,
    display_name: Option<String>,
}
#[derive(Serialize)]
struct AuthResp {
    token: String,
    user: User,
}

async fn register(State(st): State<AppState>, ConnectInfo(addr): ConnectInfo<std::net::SocketAddr>, headers: HeaderMap, Json(r): Json<AuthReq>) -> R<AuthResp> {
    // Tối đa 10 lần đăng ký / giờ / địa chỉ, tránh tạo tài khoản hàng loạt.
    let rkey = format!("reg:{}", client_key(&headers, &addr));
    if rl_blocked(&st, &rkey, 10, 3600) {
        return Err(too_many());
    }
    rl_hit(&st, &rkey, 3600);
    let username = r.username.trim().to_lowercase();
    if username.len() < 3 || username.len() > 24 || !username.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        return Err(bad("Tên đăng nhập 3-24 ký tự: chữ, số hoặc _"));
    }
    // Chỉ tên ĐĂNG NHẬP mới bị giữ chỗ / phải duy nhất; tên hiển thị thì ai cũng đặt trùng được.
    if is_reserved_name(&username) {
        return Err((StatusCode::CONFLICT, "Tên đăng nhập đã tồn tại".into()));
    }
    if r.password.len() < 8 || r.password.len() > 128 {
        return Err(bad("Mật khẩu từ 8 đến 128 ký tự"));
    }
    let pw = r.password.clone();
    let hash = tokio::task::spawn_blocking(move || hash_pw(&pw)).await.map_err(|_| e500())?.map_err(|_| e500())?;
    let display: String = r
        .display_name
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| username.clone())
        .trim()
        .chars()
        .take(24)
        .collect();
    let code = uuid::Uuid::new_v4().simple().to_string()[..8].to_uppercase();

    let db = st.db.lock().unwrap();
    let res = db.execute(
        "INSERT INTO users(username,pass_hash,display_name,code) VALUES(?1,?2,?3,?4)",
        params![username, hash, display, code],
    );
    match res {
        Ok(_) => {}
        Err(rusqlite::Error::SqliteFailure(e, _)) if e.code == rusqlite::ErrorCode::ConstraintViolation => {
            return Err((StatusCode::CONFLICT, "Tên đăng nhập đã tồn tại".into()));
        }
        Err(_) => return Err(e500()),
    }
    let id = db.last_insert_rowid();
    let token = new_token();
    db.execute("INSERT INTO sessions(token,user_id) VALUES(?1,?2)", params![tok_hash(&token), id]).map_err(|_| e500())?;
    let user = db
        .query_row(&format!("SELECT {USER_COLS} FROM users WHERE id=?1"), [id], row_to_user)
        .map_err(|_| e500())?;
    Ok(Json(AuthResp { token, user }))
}

async fn login(State(st): State<AppState>, ConnectInfo(addr): ConnectInfo<std::net::SocketAddr>, headers: HeaderMap, Json(r): Json<AuthReq>) -> R<AuthResp> {
    let username = r.username.trim().to_lowercase();
    // Chống dò mật khẩu: 5 lần sai / 10 phút cho mỗi tên đăng nhập, 30 lần sai / 10 phút cho mỗi địa chỉ.
    let ukey = format!("u:{}", username.chars().take(40).collect::<String>());
    let ikey = format!("ip:{}", client_key(&headers, &addr));
    if rl_blocked(&st, &ukey, 5, 600) || rl_blocked(&st, &ikey, 30, 600) {
        return Err(too_many());
    }
    let row = {
        let db = st.db.lock().unwrap();
        let x = db
            .query_row("SELECT id, pass_hash FROM users WHERE username=?1", [&username], |r| {
                Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?))
            })
            .ok();
        x
    };
    let Some((id, hash)) = row else {
        let dummy = DUMMY_HASH.get_or_init(|| hash_pw("dummy-password-for-timing").unwrap_or_default()).clone();
        let pw = r.password.clone();
        let _ = tokio::task::spawn_blocking(move || verify_pw(&pw, &dummy)).await;
        rl_hit(&st, &ukey, 600);
        rl_hit(&st, &ikey, 600);
        return Err(unauth_login());
    };
    let pw = r.password.clone();
    let ok = tokio::task::spawn_blocking(move || verify_pw(&pw, &hash)).await.unwrap_or(false);
    if !ok {
        rl_hit(&st, &ukey, 600);
        rl_hit(&st, &ikey, 600);
        return Err(unauth_login());
    }
    rl_clear(&st, &ukey);
    let db = st.db.lock().unwrap();
    let _ = db.execute("DELETE FROM sessions WHERE created_at < datetime('now','-30 days')", []);
    let token = new_token();
    db.execute("INSERT INTO sessions(token,user_id) VALUES(?1,?2)", params![tok_hash(&token), id]).map_err(|_| e500())?;
    let user = db
        .query_row(&format!("SELECT {USER_COLS} FROM users WHERE id=?1"), [id], row_to_user)
        .map_err(|_| e500())?;
    Ok(Json(AuthResp { token, user }))
}
fn unauth_login() -> ApiErr { (StatusCode::UNAUTHORIZED, "Sai tên đăng nhập hoặc mật khẩu".into()) }

async fn logout(State(st): State<AppState>, headers: HeaderMap) -> StatusCode {
    if let Some(t) = bearer(&headers) {
        let db = st.db.lock().unwrap();
        let _ = db.execute("DELETE FROM sessions WHERE token=?1", [tok_hash(&t)]);
    }
    StatusCode::OK
}

async fn me(State(st): State<AppState>, headers: HeaderMap) -> R<User> {
    Ok(Json(require(&st, &headers)?))
}

#[derive(Deserialize)]
struct ProfileReq {
    display_name: String,
    accent: String,
    bg: String,
    bg_url: String,
    avatar_emoji: Option<String>,
    banner_color: Option<String>,
    status: Option<String>,
    crop: Option<Value>,
}

async fn update_profile(State(st): State<AppState>, headers: HeaderMap, Json(r): Json<ProfileReq>) -> R<User> {
    let user = require(&st, &headers)?;
    let name: String = r.display_name.trim().chars().take(24).collect();
    if name.is_empty() {
        return Err(bad("Tên hiển thị không được trống"));
    }
    let a = &r.accent;
    if a.len() != 7 || !a.starts_with('#') || !a[1..].chars().all(|c| c.is_ascii_hexdigit()) {
        return Err(bad("Màu không hợp lệ"));
    }
    if !["", "dark", "ocean", "sunset", "forest", "custom", "glass"].contains(&r.bg.as_str()) {
        return Err(bad("Nền không hợp lệ"));
    }
    let u = &r.bg_url;
    let local_ok = u
        .strip_prefix("/backgrounds/")
        .map(|f| !f.is_empty() && !f.contains('/') && !f.contains(".."))
        .unwrap_or(false);
    if !u.is_empty()
        && (!(u.starts_with("https://") || local_ok) || u.len() > 500 || u.chars().any(|c| c.is_whitespace() || "\"'\\()".contains(c)))
    {
        return Err(bad("Link ảnh nền phải là https:// (hoặc ảnh đã tải lên) và không chứa ký tự đặc biệt"));
    }
    let db = st.db.lock().unwrap();
    // Gộp mọi lệnh ghi vào một giao dịch: chỉ 1 lần commit/fsync thay vì mỗi trường một lần (nhanh hơn rõ rệt trên ổ đĩa mạng),
    // và lỗi giữa chừng sẽ hoàn tác toàn bộ thay vì lưu dở.
    let tx = db.unchecked_transaction().map_err(|_| e500())?;
    tx.execute(
        "UPDATE users SET display_name=?1, accent=?2, bg=?3, bg_url=?4 WHERE id=?5",
        params![name, r.accent, r.bg, r.bg_url, user.id],
    )
    .map_err(|_| e500())?;
    if let Some(c) = r.banner_color.as_ref() {
        if !(c.is_empty() || (c.len() == 7 && c.starts_with('#') && c[1..].chars().all(|x| x.is_ascii_hexdigit()))) {
            return Err(bad("Màu banner không hợp lệ"));
        }
        tx.execute("UPDATE users SET banner_color=?1 WHERE id=?2", params![c, user.id]).map_err(|_| e500())?;
    }
    // Tâm ảnh (x, y: 0-100 % khung), zoom (0.1-4, 1 = phủ kín khung) và tỉ lệ ảnh (rộng/cao) của avatar (a*) và banner (b*).
    if let Some(c) = r.crop.as_ref() {
        let mut out = serde_json::Map::new();
        for (k, lo, hi, d) in [
            ("bx", 0.0, 100.0, 50.0),
            ("by", 0.0, 100.0, 50.0),
            ("bz", 0.1, 4.0, 1.0),
            ("ba", 0.0, 20.0, 0.0),
            ("ax", 0.0, 100.0, 50.0),
            ("ay", 0.0, 100.0, 50.0),
            ("az", 0.1, 4.0, 1.0),
            ("aa", 0.0, 20.0, 0.0),
        ] {
            let v: f64 = c.get(k).and_then(|x| x.as_f64()).filter(|x| x.is_finite()).unwrap_or(d).clamp(lo, hi);
            out.insert(k.to_string(), json!((v * 100.0).round() / 100.0));
        }
        tx.execute("UPDATE users SET crop=?1 WHERE id=?2", params![Value::Object(out).to_string(), user.id]).map_err(|_| e500())?;
    }
    if let Some(stt) = r.status.as_ref() {
        let clean: String = stt.chars().filter(|c| !c.is_control()).collect::<String>().trim().chars().take(60).collect();
        tx.execute("UPDATE users SET status=?1 WHERE id=?2", params![clean, user.id]).map_err(|_| e500())?;
    }
    if let Some(em) = r.avatar_emoji.filter(|e| !e.trim().is_empty() && e.chars().count() <= 4) {
        tx.execute("UPDATE users SET avatar=?1 WHERE id=?2", params![em.trim(), user.id]).map_err(|_| e500())?;
    }
    let out = tx
        .query_row(&format!("SELECT {USER_COLS} FROM users WHERE id=?1"), [user.id], row_to_user)
        .map_err(|_| e500())?;
    tx.commit().map_err(|_| e500())?;
    Ok(Json(out))
}

fn sniff_image(b: &[u8]) -> Option<&'static str> {
    if b.len() > 12 && (b.starts_with(b"GIF87a") || b.starts_with(b"GIF89a")) { Some("gif") }
    else if b.starts_with(&[0x89, b'P', b'N', b'G']) { Some("png") }
    else if b.starts_with(&[0xFF, 0xD8, 0xFF]) { Some("jpg") }
    else if b.len() > 12 && &b[0..4] == b"RIFF" && &b[8..12] == b"WEBP" { Some("webp") }
    else { None }
}

// ---------- Tối ưu GIF khi tải lên: gifsicle (nén) → gif2webp (WebP động, giải mã nhẹ hơn GIF) ----------
// Thiếu công cụ hoặc có lỗi → giữ nguyên file gốc, việc tải ảnh lên không bao giờ bị hỏng.
// Đường dẫn tuỳ chọn: biến môi trường GIFSICLE_PATH / GIF2WEBP_PATH (mặc định tìm trong PATH).
async fn run_tool(prog: &str, args: &[&str]) -> bool {
    let mut cmd = tokio::process::Command::new(prog);
    cmd.args(args)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true);
    match tokio::time::timeout(Duration::from_secs(90), cmd.status()).await {
        Ok(Ok(st)) => st.success(),
        _ => false,
    }
}

fn tool_path(env: &str, default: &str) -> String {
    std::env::var(env).ok().filter(|p| !p.trim().is_empty()).unwrap_or_else(|| default.to_string())
}

async fn optimize_gif_files(
    body: &[u8],
    src: &std::path::Path,
    opt: &std::path::Path,
    web: &std::path::Path,
    vid: &std::path::Path,
    max_px: u32,
    want_video: bool,
) -> Option<(Vec<u8>, &'static str, Option<Vec<u8>>)> {
    tokio::fs::write(src, body).await.ok()?;
    let (s, o, w, vp) = (src.to_str()?, opt.to_str()?, web.to_str()?, vid.to_str()?);
    let gifsicle = tool_path("GIFSICLE_PATH", "gifsicle");
    let gif2webp = tool_path("GIF2WEBP_PATH", "gif2webp");
    let ffmpeg = tool_path("FFMPEG_PATH", "ffmpeg");

    // 1) gifsicle: thu nhỏ nếu ảnh to hơn max_px (chỉ thu nhỏ, không phóng to) rồi nén nhẹ (lossy=40, gần như không thấy artifact).
    //    Số điểm ảnh mỗi khung là thứ làm điện thoại lag nhiều nhất, nên đây là bước quan trọng nhất. Không hỗ trợ lossy thì nén không mất dữ liệu.
    let fit = format!("{max_px}x{max_px}");
    let mut best_gif: Vec<u8> = body.to_vec();
    let mut webp_input = s;
    let packed = run_tool(&gifsicle, &["-w", "--resize-fit", fit.as_str(), "-O2", "--lossy=40", s, "-o", o]).await
        || run_tool(&gifsicle, &["-w", "--resize-fit", fit.as_str(), "-O2", s, "-o", o]).await;
    if packed {
        if let Ok(b) = tokio::fs::read(opt).await {
            if matches!(sniff_image(&b), Some("gif")) && b.len() < best_gif.len() {
                best_gif = b;
                webp_input = o;
            }
        }
    }

    // 2) + 3) Video MP4 (ffmpeg) và WebP động (gif2webp) chỉ cùng đọc webp_input, không phụ thuộc nhau → chạy SONG SONG
    //    thay vì nối đuôi (trước đây tổng thời gian = cộng hai bước, giờ chỉ còn bước lâu hơn).
    //    Video: MP4 (H.264) tắt tiếng, tối đa 25 khung/giây, điện thoại giải mã bằng phần cứng nên nhẹ hơn ảnh động nhiều.
    //    Nền trong suốt của GIF sẽ thành màu đen. Thiếu ffmpeg thì bỏ qua bước này.
    //    WebP: ảnh dự phòng khi video không phát được, và là ảnh dùng ở các chỗ không dùng video (danh sách, nền).
    let make_video = async {
        if !want_video {
            return None;
        }
        let made = run_tool(
            &ffmpeg,
            &[
                "-v", "error", "-y", "-i", webp_input, "-an", "-vf", "fps=25,scale=trunc(iw/2)*2:trunc(ih/2)*2",
                "-c:v", "libx264", "-profile:v", "main", "-pix_fmt", "yuv420p", "-preset", "veryfast", "-crf", "26",
                "-movflags", "+faststart", vp,
            ],
        )
        .await;
        if !made {
            return None;
        }
        match tokio::fs::read(vid).await {
            Ok(b) if b.len() > 12 && &b[4..8] == b"ftyp" => Some(b),
            _ => None,
        }
    };
    let make_webp = async {
        // -m 2 (thay vì 4): nén nhanh hơn nhiều, file chỉ nhỉnh hơn một chút.
        let converted = run_tool(
            &gif2webp,
            &["-quiet", "-mixed", "-q", "80", "-m", "2", "-mt", "-kmin", "3", "-kmax", "5", webp_input, "-o", w],
        )
        .await;
        if !converted {
            return None;
        }
        match tokio::fs::read(web).await {
            // Chỉ dùng WebP khi hợp lệ và không to hơn GIF đã nén quá 1,5 lần
            Ok(b) if matches!(sniff_image(&b), Some("webp")) && b.len() <= best_gif.len() + best_gif.len() / 2 => Some((b, "webp")),
            _ => None,
        }
    };
    let (video, webp) = tokio::join!(make_video, make_webp);
    let mut image: Option<(Vec<u8>, &'static str)> = webp;
    if image.is_none() && best_gif.len() < body.len() {
        image = Some((best_gif, "gif"));
    }
    match (image, video) {
        (Some((b, e)), v) => Some((b, e, v)),
        (None, Some(v)) => Some((body.to_vec(), "gif", Some(v))),
        (None, None) => None,
    }
}

// max_px: cạnh dài tối đa (điểm ảnh) cho GIF; want_video: có tạo thêm video MP4 hay không. PNG/JPG/WEBP giữ nguyên.
async fn optimize_upload(body: Bytes, ext: &'static str, max_px: u32, want_video: bool) -> (Vec<u8>, &'static str, Option<Vec<u8>>) {
    if ext != "gif" {
        return (body.to_vec(), ext, None);
    }
    let id = uuid::Uuid::new_v4().simple().to_string();
    let dir = std::env::temp_dir();
    let src = dir.join(format!("muse-{id}-in.gif"));
    let opt = dir.join(format!("muse-{id}-opt.gif"));
    let web = dir.join(format!("muse-{id}-out.webp"));
    let vid = dir.join(format!("muse-{id}-out.mp4"));
    // Giới hạn tổng 40 giây cho việc nén (gifsicle + gif2webp có thể mỗi bước 90 giây trên Render yếu → vượt thời gian chờ của cổng nhà
    // → 502). Quá 40 giây thì bỏ nén, lưu nguyên GIF gốc (vẫn chuyển động); kill_on_drop tự dừng các tiến trình nén còn dang dở.
    let result = tokio::time::timeout(Duration::from_secs(40), optimize_gif_files(&body, &src, &opt, &web, &vid, max_px, want_video))
        .await
        .unwrap_or(None);
    for p in [&src, &opt, &web, &vid] {
        let _ = tokio::fs::remove_file(p).await;
    }
    result.unwrap_or_else(|| (body.to_vec(), ext, None))
}

// Lưu ảnh tải lên. Có video đi kèm thì ảnh đặt tên {stem}.v.{ext} và video là {stem}.mp4
// (giao diện dựa vào đuôi ".v." để biết có video; video lỗi thì tự quay về ảnh).
async fn save_upload(dir: &std::path::Path, img: &[u8], ext: &str, video: Option<Vec<u8>>) -> std::io::Result<String> {
    let stem = uuid::Uuid::new_v4().simple().to_string();
    let name = if video.is_some() { format!("{stem}.v.{ext}") } else { format!("{stem}.{ext}") };
    tokio::fs::write(dir.join(&name), img).await?;
    if let Some(v) = video {
        let _ = tokio::fs::write(dir.join(format!("{stem}.mp4")), &v).await;
    }
    Ok(name)
}

// Xoá file ảnh tải lên, kèm video MP4 đi cùng nếu có.
async fn remove_upload_file(dir: &std::path::Path, f: &str) {
    let _ = tokio::fs::remove_file(dir.join(f)).await;
    if let Some((stem, _)) = f.split_once(".v.") {
        let _ = tokio::fs::remove_file(dir.join(format!("{stem}.mp4"))).await;
    }
}

// PNG/JPG/WEBP lưu nguyên file gốc; GIF được tối ưu (gifsicle → WebP động) qua optimize_upload, vẫn giữ chuyển động.
async fn upload_avatar(State(st): State<AppState>, headers: HeaderMap, body: Bytes) -> R<User> {
    let user = require(&st, &headers)?;
    if body.len() > 10 * 1024 * 1024 {
        return Err(bad("Avatar tối đa 10MB"));
    }
    let ext = sniff_image(&body).ok_or_else(|| bad("Chỉ nhận ảnh PNG, JPG, GIF, WEBP (tối đa 10MB)"))?;
    let (body, ext, video) = optimize_upload(body, ext, 512, true).await;
    let name = save_upload(&st.avatar_dir, &body, ext, video).await.map_err(|_| e500())?;
    let url = format!("/avatars/{name}");
    let old: String;
    {
        let db = st.db.lock().unwrap();
        old = db.query_row("SELECT avatar FROM users WHERE id=?1", [user.id], |r| r.get(0)).unwrap_or_default();
        db.execute("UPDATE users SET avatar=?1 WHERE id=?2", params![url, user.id]).map_err(|_| e500())?;
    }
    if let Some(f) = old.strip_prefix("/avatars/") {
        if !f.contains('/') && !f.contains("..") {
            remove_upload_file(&st.avatar_dir, f).await;
        }
    }
    Ok(Json(User { avatar: url, ..user }))
}

#[derive(Deserialize)]
struct FriendReq {
    code: String,
}

fn notify_user(st: &AppState, uid: i64, msg: serde_json::Value) {
    let tx = st.notify.lock().unwrap().get(&uid).cloned();
    if let Some(tx) = tx {
        let _ = tx.send(msg.to_string());
    }
}

fn friend_from_row(r: &rusqlite::Row, online: &HashSet<i64>) -> rusqlite::Result<Friend> {
    let id: i64 = r.get(0)?;
    Ok(Friend {
        id,
        display_name: r.get(1)?,
        avatar: r.get(2)?,
        online: online.contains(&id),
        accent: r.get(3)?,
        banner: r.get(4)?,
        banner_color: r.get(5)?,
        status: r.get(6)?,
        crop: r.get(7)?,
        supporter: if r.get::<_, i64>(9)? != 0 { 2 } else { r.get(8)? },
    })
}

const FRIEND_COLS: &str = "u.id, u.display_name, u.avatar, u.accent, u.banner, u.banner_color, u.status, u.crop, u.supporter, u.is_admin";

// Gửi lời mời kết bạn bằng mã bạn bè (hoặc tên đăng nhập). Nếu người kia đã mời mình trước thì tự chấp nhận.
async fn add_friend(State(st): State<AppState>, headers: HeaderMap, Json(r): Json<FriendReq>) -> Result<Json<serde_json::Value>, ApiErr> {
    let user = require(&st, &headers)?;
    let key = r.code.trim().to_string();
    if key.is_empty() {
        return Err(bad("Hãy nhập mã bạn bè hoặc tên đăng nhập"));
    }
    let result = {
        let db = st.db.lock().unwrap();
        let fid: Option<i64> = db
            .query_row("SELECT id FROM users WHERE code=?1", [key.to_uppercase()], |r| r.get(0))
            .ok()
            .or_else(|| db.query_row("SELECT id FROM users WHERE username=?1", [&key], |r| r.get(0)).ok());
        let Some(fid) = fid else { return Err((StatusCode::NOT_FOUND, "Không tìm thấy người dùng này".into())) };
        if fid == user.id {
            return Err(bad("Không thể tự kết bạn với chính mình"));
        }
        let already = db
            .query_row("SELECT 1 FROM friends WHERE user_id=?1 AND friend_id=?2", params![user.id, fid], |_| Ok(()))
            .is_ok();
        if already {
            return Err((StatusCode::CONFLICT, "Hai bạn đã là bạn bè rồi".into()));
        }
        let reverse = db
            .query_row("SELECT 1 FROM friend_requests WHERE from_id=?1 AND to_id=?2", params![fid, user.id], |_| Ok(()))
            .is_ok();
        if reverse {
            for (a, b) in [(user.id, fid), (fid, user.id)] {
                db.execute("INSERT OR IGNORE INTO friends(user_id,friend_id) VALUES(?1,?2)", params![a, b])
                    .map_err(|_| e500())?;
            }
            db.execute(
                "DELETE FROM friend_requests WHERE (from_id=?1 AND to_id=?2) OR (from_id=?2 AND to_id=?1)",
                params![user.id, fid],
            )
            .map_err(|_| e500())?;
            (fid, "accepted")
        } else {
            let n = db
                .execute("INSERT OR IGNORE INTO friend_requests(from_id,to_id) VALUES(?1,?2)", params![user.id, fid])
                .map_err(|_| e500())?;
            if n == 0 {
                return Err((StatusCode::CONFLICT, "Bạn đã gửi lời mời cho người này rồi".into()));
            }
            (fid, "pending")
        }
    };
    let (fid, status) = result;
    if status == "pending" {
        notify_user(&st, fid, json!({ "type": "friend_request", "from": user.display_name, "avatar": user.avatar }));
    } else {
        notify_user(&st, fid, json!({ "type": "friend_update", "from": user.display_name, "avatar": user.avatar, "accepted": true }));
    }
    Ok(Json(json!({ "status": status })))
}

async fn list_friends(State(st): State<AppState>, headers: HeaderMap) -> R<Vec<Friend>> {
    let user = require(&st, &headers)?;
    let online: HashSet<i64> = st.notify.lock().unwrap().keys().cloned().collect();
    let db = st.db.lock().unwrap();
    let sql = format!("SELECT {FRIEND_COLS} FROM friends f JOIN users u ON u.id=f.friend_id WHERE f.user_id=?1 ORDER BY u.display_name COLLATE NOCASE");
    let mut stmt = db.prepare(&sql).map_err(|_| e500())?;
    let rows = stmt.query_map([user.id], |r| friend_from_row(r, &online)).map_err(|_| e500())?;
    let list: Vec<Friend> = rows.filter_map(|x| x.ok()).collect();
    Ok(Json(list))
}

// Lời mời đang chờ: incoming = người khác mời mình, outgoing = mình đã mời.
async fn list_requests(State(st): State<AppState>, headers: HeaderMap) -> Result<Json<serde_json::Value>, ApiErr> {
    let user = require(&st, &headers)?;
    let online: HashSet<i64> = st.notify.lock().unwrap().keys().cloned().collect();
    let db = st.db.lock().unwrap();
    let q = |sql: String| -> Result<Vec<Friend>, ApiErr> {
        let mut stmt = db.prepare(&sql).map_err(|_| e500())?;
        let rows = stmt.query_map([user.id], |r| friend_from_row(r, &online)).map_err(|_| e500())?;
        let list: Vec<Friend> = rows.filter_map(|x| x.ok()).collect();
        Ok(list)
    };
    let incoming = q(format!("SELECT {FRIEND_COLS} FROM friend_requests q JOIN users u ON u.id=q.from_id WHERE q.to_id=?1 ORDER BY q.created_at DESC"))?;
    let outgoing = q(format!("SELECT {FRIEND_COLS} FROM friend_requests q JOIN users u ON u.id=q.to_id WHERE q.from_id=?1 ORDER BY q.created_at DESC"))?;
    Ok(Json(json!({ "incoming": incoming, "outgoing": outgoing })))
}

#[derive(Deserialize)]
struct RespondReq {
    user_id: i64,
    action: String, // accept | decline | cancel
}

async fn respond_request(State(st): State<AppState>, headers: HeaderMap, Json(r): Json<RespondReq>) -> Result<StatusCode, ApiErr> {
    let user = require(&st, &headers)?;
    {
        let db = st.db.lock().unwrap();
        match r.action.as_str() {
            "accept" => {
                let n = db
                    .execute("DELETE FROM friend_requests WHERE from_id=?1 AND to_id=?2", params![r.user_id, user.id])
                    .map_err(|_| e500())?;
                if n == 0 {
                    return Err((StatusCode::NOT_FOUND, "Lời mời không còn tồn tại".into()));
                }
                for (a, b) in [(user.id, r.user_id), (r.user_id, user.id)] {
                    db.execute("INSERT OR IGNORE INTO friends(user_id,friend_id) VALUES(?1,?2)", params![a, b])
                        .map_err(|_| e500())?;
                }
            }
            "decline" => {
                db.execute("DELETE FROM friend_requests WHERE from_id=?1 AND to_id=?2", params![r.user_id, user.id])
                    .map_err(|_| e500())?;
            }
            "cancel" => {
                db.execute("DELETE FROM friend_requests WHERE from_id=?1 AND to_id=?2", params![user.id, r.user_id])
                    .map_err(|_| e500())?;
            }
            _ => return Err(bad("Thao tác không hợp lệ")),
        }
    }
    notify_user(
        &st,
        r.user_id,
        json!({ "type": "friend_update", "from": user.display_name, "avatar": user.avatar, "accepted": r.action == "accept" }),
    );
    Ok(StatusCode::OK)
}

async fn remove_friend(State(st): State<AppState>, headers: HeaderMap, Path(fid): Path<i64>) -> Result<StatusCode, ApiErr> {
    let user = require(&st, &headers)?;
    {
        let db = st.db.lock().unwrap();
        db.execute(
            "DELETE FROM friends WHERE (user_id=?1 AND friend_id=?2) OR (user_id=?2 AND friend_id=?1)",
            params![user.id, fid],
        )
        .map_err(|_| e500())?;
    }
    notify_user(&st, fid, json!({ "type": "friend_update", "from": user.display_name, "avatar": user.avatar, "accepted": false }));
    Ok(StatusCode::OK)
}

#[derive(Deserialize)]
struct WsQ {
    ticket: String,
}

fn now_secs() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0)
}

// POST /api/ws-ticket (Authorization: Bearer ...) -> { ticket }: vé dùng một lần cho /ws/:room và /ws-user.
async fn ws_ticket(State(st): State<AppState>, headers: HeaderMap) -> R<Value> {
    let user = require(&st, &headers)?;
    let ticket = new_token();
    let now = now_secs();
    let mut g = st.ws_tickets.lock().unwrap();
    g.retain(|_, (_, exp)| *exp > now);
    if g.len() > 5000 {
        return Err((StatusCode::TOO_MANY_REQUESTS, "Quá nhiều yêu cầu".into()));
    }
    g.insert(ticket.clone(), (user, now + 30));
    Ok(Json(json!({ "ticket": ticket })))
}

fn take_ws_ticket(st: &AppState, ticket: &str) -> Option<User> {
    let (u, exp) = st.ws_tickets.lock().unwrap().remove(ticket)?;
    if exp > now_secs() { Some(u) } else { None }
}

// Mã ghép thiết bị dùng một lần: chỉ máy đang chạy server mới tạo được mã (loopback),
// để đăng nhập nhanh tài khoản ADMIN_USER trên desktop/Android khác mà không cần gõ mật khẩu
// và không bao giờ đóng mật khẩu/token vào các bản build phân phát chung.
// "Chính máy chạy server" = kết nối đến từ loopback, KHÔNG đi qua reverse proxy/tunnel (Caddy, nginx, cloudflared, ngrok... đều
// kết nối từ 127.0.0.1 nhưng gắn header chuyển tiếp) và Host là localhost. Chỉ kiểm tra IP thì khi Caddy chạy cùng máy,
// MỌI người trên internet đều bị coi là "localhost" và tự lấy được mã admin.
fn is_local_request(addr: &std::net::SocketAddr, h: &HeaderMap) -> bool {
    if !addr.ip().is_loopback() {
        return false;
    }
    for k in ["x-forwarded-for", "x-forwarded-host", "x-forwarded-proto", "x-real-ip", "forwarded", "via", "cf-connecting-ip", "true-client-ip"] {
        if h.contains_key(k) {
            return false;
        }
    }
    let host = h.get(header::HOST).and_then(|v| v.to_str().ok()).unwrap_or("").trim().to_lowercase();
    let name = if let Some(rest) = host.strip_prefix('[') { rest.split(']').next().unwrap_or("") } else { host.split(':').next().unwrap_or("") };
    matches!(name, "localhost" | "127.0.0.1" | "::1")
}

#[derive(Deserialize)]
struct PairRedeem {
    code: String,
}

// ---- Đăng nhập bằng QR ----
// Thiết bị mới: POST /api/qr/new -> { id, url } rồi hiện QR chứa url, đồng thời hỏi GET /api/qr/poll/:id mỗi 2 giây.
// Thiết bị đã đăng nhập (quét QR): POST /api/qr/approve { id } -> server tạo phiên mới cho tài khoản đó và giao cho thiết bị mới (một lần).
struct QrEntry {
    exp: u64,
    done: Option<(String, User)>,
}
static QR_SESSIONS: std::sync::OnceLock<Mutex<HashMap<String, QrEntry>>> = std::sync::OnceLock::new();
fn qr_map() -> &'static Mutex<HashMap<String, QrEntry>> {
    QR_SESSIONS.get_or_init(|| Mutex::new(HashMap::new()))
}
async fn qr_new(State(st): State<AppState>, ConnectInfo(addr): ConnectInfo<std::net::SocketAddr>, headers: HeaderMap) -> R<Value> {
    let ikey = format!("qrnew:{}", client_key(&headers, &addr));
    if rl_blocked(&st, &ikey, 30, 600) {
        return Err(too_many());
    }
    rl_hit(&st, &ikey, 600);
    let now = now_secs();
    let id = uuid::Uuid::new_v4().simple().to_string();
    {
        let mut g = qr_map().lock().unwrap();
        g.retain(|_, e| e.exp > now);
        if g.len() >= 2000 {
            return Err(too_many());
        }
        g.insert(id.clone(), QrEntry { exp: now + 180, done: None });
    }
    let base = std::env::var("PUBLIC_URL").unwrap_or_default();
    Ok(Json(json!({ "id": id, "expires_in": 180, "base": base.trim().trim_end_matches('/') })))
}
async fn qr_poll(Path(id): Path<String>) -> R<Value> {
    let now = now_secs();
    let mut g = qr_map().lock().unwrap();
    g.retain(|_, e| e.exp > now);
    let Some(e) = g.get_mut(&id) else {
        return Err((StatusCode::GONE, "Mã QR đã hết hạn".into()));
    };
    if let Some((token, user)) = e.done.take() {
        g.remove(&id);
        return Ok(Json(json!({ "status": "ok", "token": token, "user": user })));
    }
    Ok(Json(json!({ "status": "wait" })))
}
#[derive(Deserialize)]
struct QrApproveReq {
    id: String,
}
async fn qr_approve(State(st): State<AppState>, headers: HeaderMap, Json(r): Json<QrApproveReq>) -> R<Value> {
    let user = require(&st, &headers)?;
    let now = now_secs();
    {
        let mut g = qr_map().lock().unwrap();
        g.retain(|_, e| e.exp > now);
        match g.get(r.id.trim()) {
            Some(e) if e.done.is_none() => {}
            _ => return Err((StatusCode::GONE, "Mã QR đã hết hạn hoặc đã được dùng".into())),
        }
    }
    let token = new_token();
    {
        let db = st.db.lock().unwrap();
        db.execute("INSERT INTO sessions(token,user_id) VALUES(?1,?2)", params![tok_hash(&token), user.id]).map_err(|_| e500())?;
    }
    let name = user.display_name.clone();
    let mut g = qr_map().lock().unwrap();
    match g.get_mut(r.id.trim()) {
        Some(e) if e.done.is_none() => e.done = Some((token, user)),
        _ => return Err((StatusCode::GONE, "Mã QR đã hết hạn hoặc đã được dùng".into())),
    }
    Ok(Json(json!({ "ok": true, "name": name })))
}

// ---- Ủng hộ qua Ko-fi: webhook ghi nhận donate -> mở khoá "Thẻ chia sẻ" (FANS NO.) ----
// Cấu hình: KOFI_TOKEN (Verification Token trong Ko-fi > More > API), KOFI_SMALL_MIN (mặc định 1), KOFI_BIG_MIN (mặc định 10).
// Người donate ghi MÃ KẾT BẠN (hoặc tên đăng nhập) của tài khoản BBeat vào lời nhắn Ko-fi để server khớp tài khoản.
// Tổng tiền donate >= KOFI_BIG_MIN -> thành viên lớn (2), >= KOFI_SMALL_MIN -> thành viên nhỏ (1). Chỉ tăng, không tự hạ.
fn tier_for_total(total: f64) -> i64 {
    let envf = |k: &str, d: f64| std::env::var(k).ok().and_then(|v| v.trim().parse::<f64>().ok()).unwrap_or(d);
    if total >= envf("KOFI_BIG_MIN", 10.0) {
        2
    } else if total >= envf("KOFI_SMALL_MIN", 1.0) {
        1
    } else {
        0
    }
}
fn same_secret(a: &str, b: &str) -> bool {
    let (a, b) = (a.as_bytes(), b.as_bytes());
    a.len() == b.len() && a.iter().zip(b.iter()).fold(0u8, |d, (x, y)| d | (x ^ y)) == 0
}
#[derive(Deserialize)]
struct KofiForm {
    data: String,
}
async fn kofi_webhook(State(st): State<AppState>, axum::extract::Form(f): axum::extract::Form<KofiForm>) -> Result<StatusCode, ApiErr> {
    let secret = std::env::var("KOFI_TOKEN").unwrap_or_default();
    let secret = secret.trim();
    if secret.is_empty() {
        return Err(bad("Server chưa đặt KOFI_TOKEN"));
    }
    let v: Value = serde_json::from_str(&f.data).map_err(|_| bad("Dữ liệu không hợp lệ"))?;
    let tok = v.get("verification_token").and_then(|x| x.as_str()).unwrap_or("");
    if !same_secret(tok, secret) {
        return Err((StatusCode::FORBIDDEN, "Sai token".into()));
    }
    let kind = v.get("type").and_then(|x| x.as_str()).unwrap_or("");
    if kind != "Donation" && kind != "Subscription" {
        return Ok(StatusCode::OK);
    }
    let txn = v.get("kofi_transaction_id").and_then(|x| x.as_str()).unwrap_or("").to_string();
    if txn.is_empty() {
        return Err(bad("Thiếu mã giao dịch"));
    }
    let amount: f64 = v.get("amount").and_then(|x| x.as_str()).and_then(|a| a.trim().parse().ok()).unwrap_or(0.0);
    let currency = v.get("currency").and_then(|x| x.as_str()).unwrap_or("").to_string();
    let from_name = v.get("from_name").and_then(|x| x.as_str()).unwrap_or("").to_string();
    let message: String = v.get("message").and_then(|x| x.as_str()).unwrap_or("").chars().take(500).collect();
    let db = st.db.lock().unwrap();
    // Khớp tài khoản: từ nào trong lời nhắn trùng mã kết bạn (8 ký tự) hoặc tên đăng nhập.
    let mut uid: Option<i64> = None;
    for w in message.split(|c: char| !(c.is_alphanumeric() || c == '_' || c == '-' || c == '.')).filter(|w| !w.is_empty()) {
        if let Ok(id) = db.query_row(
            "SELECT id FROM users WHERE code=?1 COLLATE NOCASE OR username=?1 COLLATE NOCASE",
            [w],
            |r| r.get::<_, i64>(0),
        ) {
            uid = Some(id);
            break;
        }
    }
    let inserted = db
        .execute(
            "INSERT OR IGNORE INTO kofi_donations(txn_id,user_id,amount,currency,from_name,message) VALUES(?1,?2,?3,?4,?5,?6)",
            params![txn, uid, amount, currency, from_name, message],
        )
        .map_err(|_| e500())?;
    if inserted > 0 {
        if let Some(id) = uid {
            let total: f64 = db
                .query_row("SELECT COALESCE(SUM(amount),0) FROM kofi_donations WHERE user_id=?1", [id], |r| r.get(0))
                .unwrap_or(0.0);
            let tier = tier_for_total(total);
            if tier > 0 {
                let _ = db.execute("UPDATE users SET supporter=MAX(supporter,?1) WHERE id=?2", params![tier, id]);
            }
        }
    }
    Ok(StatusCode::OK)
}

#[derive(Deserialize)]
struct SupporterReq {
    username: String,
    tier: i64,
}
// Admin gán tay (khi người donate quên ghi mã vào lời nhắn): POST /api/admin/supporter { username, tier: 0|1|2 }
async fn admin_set_supporter(State(st): State<AppState>, h: HeaderMap, Json(r): Json<SupporterReq>) -> R<Value> {
    let me = require(&st, &h)?;
    if !me.is_admin {
        return Err((StatusCode::FORBIDDEN, "Chỉ admin".into()));
    }
    if !(0..=2).contains(&r.tier) {
        return Err(bad("tier chỉ là 0, 1 hoặc 2"));
    }
    let db = st.db.lock().unwrap();
    let n = db
        .execute("UPDATE users SET supporter=?1 WHERE username=?2 COLLATE NOCASE", params![r.tier, r.username.trim()])
        .map_err(|_| e500())?;
    if n == 0 {
        return Err(bad("Không có tài khoản này"));
    }
    Ok(Json(json!({ "ok": true, "tier": r.tier })))
}


#[derive(Deserialize)]
struct AdminUserTarget {
    username: String,
}

// Admin cảnh cáo avatar bậy bạ: trả avatar về mặc định (🎧), không đụng tài khoản/dữ liệu khác.
// POST /api/admin/users/reset-avatar { username }
async fn admin_reset_avatar(State(st): State<AppState>, headers: HeaderMap, Json(r): Json<AdminUserTarget>) -> R<Value> {
    let me = require(&st, &headers)?;
    if !me.is_admin {
        return Err((StatusCode::FORBIDDEN, "Chỉ admin".into()));
    }
    let name = r.username.trim();
    if name.is_empty() {
        return Err(bad("Thiếu username"));
    }
    let (uid, old_avatar): (i64, String) = {
        let db = st.db.lock().unwrap();
        db.query_row("SELECT id, avatar FROM users WHERE username=?1 COLLATE NOCASE", [name], |r| Ok((r.get(0)?, r.get(1)?)))
            .map_err(|_| bad("Không có tài khoản này"))?
    };
    {
        let db = st.db.lock().unwrap();
        db.execute("UPDATE users SET avatar='🎧' WHERE id=?1", [uid]).map_err(|_| e500())?;
    }
    if let Some(f) = old_avatar.strip_prefix("/avatars/") {
        if !f.contains('/') && !f.contains("..") {
            remove_upload_file(&st.avatar_dir, f).await;
        }
    }
    Ok(Json(json!({ "ok": true })))
}

// POST /api/admin/pair-code — chỉ gọi được từ chính máy chạy server (127.0.0.1/::1).
fn ct_eq(a: &str, b: &str) -> bool {
    let (a, b) = (a.as_bytes(), b.as_bytes());
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b.iter()).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

// Hai cách được cấp mã admin: (1) gọi từ chính máy chạy server (loopback, không qua proxy); (2) kèm header x-pair-key khớp biến môi trường
// ADMIN_PAIR_KEY (chuỗi ngẫu nhiên >= 24 ký tự, chỉ đặt trong env của server chính và của máy nhà chạy cổng nhà — KHÔNG đưa vào repo).
// Cách (2) là đường của cổng nhà: giao diện mở ở http://127.0.0.1:PORT, cổng nhà tự kiểm tra là máy mình rồi gắn khoá khi chuyển tiếp lên server chính.
async fn admin_pair_code(State(st): State<AppState>, ConnectInfo(addr): ConnectInfo<std::net::SocketAddr>, headers: HeaderMap) -> R<Value> {
    if rl_blocked(&st, "pairkey:all", 10, 600) {
        return Err(too_many());
    }
    let key_ok = match (std::env::var("ADMIN_PAIR_KEY").ok(), headers.get("x-pair-key").and_then(|v| v.to_str().ok())) {
        (Some(k), Some(v)) => {
            let k = k.trim();
            k.len() >= 24 && ct_eq(v.trim(), k)
        }
        _ => false,
    };
    if headers.contains_key("x-pair-key") && !key_ok {
        rl_hit(&st, "pairkey:all", 600);
    }
    if !key_ok && !is_local_request(&addr, &headers) {
        return Err((StatusCode::FORBIDDEN, "Server từ chối: chỉ tạo được mã ở chính máy chạy server, hoặc đặt cùng một ADMIN_PAIR_KEY (≥ 24 ký tự) ở server chính và ở máy chạy cổng nhà".into()));
    }
    let Ok(admin_user) = std::env::var("ADMIN_USER") else {
        return Err(bad("Chưa đặt ADMIN_USER"));
    };
    let admin_user = admin_user.trim().to_lowercase();
    let uid: i64 = {
        let db = st.db.lock().unwrap();
        db.query_row("SELECT id FROM users WHERE username=?1 AND is_admin=1", [&admin_user], |r| r.get(0))
            .map_err(|_| bad("Chưa có tài khoản admin (kiểm tra ADMIN_USER/ADMIN_PASS rồi khởi động lại server)"))?
    };
    let now = now_secs();
    let mut g = st.pair_codes.lock().unwrap();
    g.retain(|_, (_, exp)| *exp > now);
    // Chỉ giữ một mã hiệu lực tại một thời điểm: tạo mã mới thì mã cũ (nếu còn) mất hiệu lực.
    g.retain(|_, (u, _)| *u != uid);
    let code = format!("{:06}", uuid::Uuid::new_v4().as_u128() % 1_000_000);
    let exp = now + 300;
    g.insert(code.clone(), (uid, exp));
    // base: domain public (nếu có PUBLIC_URL) để link redeem dùng được từ máy khác, không chỉ trên localhost.
    let base = std::env::var("PUBLIC_URL").unwrap_or_default().trim().trim_end_matches('/').to_string();
    Ok(Json(json!({ "code": code, "expires_in": 300, "base": base })))
}

#[derive(Deserialize)]
struct PairCreateReq {
    password: String,
}

// POST /api/pair/create { password } — đã đăng nhập VÀ nhập lại mật khẩu → mã ghép 6 số cho CHÍNH tài khoản này (dùng ở Cài đặt).
async fn pair_create(State(st): State<AppState>, ConnectInfo(addr): ConnectInfo<std::net::SocketAddr>, headers: HeaderMap, Json(r): Json<PairCreateReq>) -> R<Value> {
    let user = require(&st, &headers)?;
    let ukey = format!("pc:{}", user.id);
    let ikey = format!("pcip:{}", client_key(&headers, &addr));
    if rl_blocked(&st, &ukey, 5, 600) || rl_blocked(&st, &ikey, 20, 600) {
        return Err(too_many());
    }
    let hash: String = {
        let db = st.db.lock().unwrap();
        let x = db.query_row("SELECT pass_hash FROM users WHERE id=?1", [user.id], |row| row.get::<_, String>(0)).map_err(|_| e500())?;
        x
    };
    let pw = r.password.clone();
    let ok = tokio::task::spawn_blocking(move || verify_pw(&pw, &hash)).await.unwrap_or(false);
    if !ok {
        rl_hit(&st, &ukey, 600);
        rl_hit(&st, &ikey, 600);
        return Err(bad("Sai mật khẩu"));
    }
    rl_clear(&st, &ukey);
    // Server chạy ở máy nhà (localhost) nhưng ứng dụng đăng nhập vào server chính: đặt PAIR_REMOTE=<địa chỉ server chính> và cùng ADMIN_PAIR_KEY
    // ở hai nơi thì mã được xin THẲNG từ server chính (tài khoản ADMIN_USER của server chính), nên nhập ở ứng dụng là vào được.
    // Chỉ admin của server này, hoặc người đang ngồi ở chính máy này (localhost), mới dùng được đường này.
    if let (Ok(remote), Ok(key)) = (std::env::var("PAIR_REMOTE"), std::env::var("ADMIN_PAIR_KEY")) {
        let (remote, key) = (remote.trim().trim_end_matches('/').to_string(), key.trim().to_string());
        if !remote.is_empty() && key.len() >= 24 && (user.is_admin || is_local_request(&addr, &headers)) {
            let resp = st
                .http
                .post(format!("{remote}/api/admin/pair-code"))
                .header("x-pair-key", key)
                .send()
                .await
                .map_err(|e| (StatusCode::BAD_GATEWAY, format!("Không nối được server chính {remote}: {e}")))?;
            let status = resp.status();
            if !status.is_success() {
                let t = resp.text().await.unwrap_or_default();
                return Err((StatusCode::BAD_GATEWAY, format!("Server chính từ chối ({}): {}", status.as_u16(), t.trim())));
            }
            let mut j = resp.json::<Value>().await.map_err(|_| (StatusCode::BAD_GATEWAY, "Server chính trả dữ liệu lạ".to_string()))?;
            if let Some(o) = j.as_object_mut() {
                let empty = o.get("base").and_then(|b| b.as_str()).map(|b| b.is_empty()).unwrap_or(true);
                if empty {
                    o.insert("base".into(), json!(remote));
                }
            }
            return Ok(Json(j));
        }
    }
    let now = now_secs();
    let code = format!("{:06}", uuid::Uuid::new_v4().as_u128() % 1_000_000);
    {
        let mut g = st.pair_codes.lock().unwrap();
        g.retain(|_, (u, exp)| *exp > now && *u != user.id);
        g.insert(code.clone(), (user.id, now + 300));
    }
    Ok(Json(json!({ "code": code, "expires_in": 300, "base": card_base_url(&headers) })))
}

// POST /api/pair/redeem { code } -> { token, user }: dùng mã trên để đăng nhập, không cần mật khẩu. Dùng một lần.
async fn pair_redeem(
    State(st): State<AppState>,
    ConnectInfo(addr): ConnectInfo<std::net::SocketAddr>,
    headers: HeaderMap,
    Json(r): Json<PairRedeem>,
) -> R<AuthResp> {
    let ikey = format!("pair:{}", client_key(&headers, &addr));
    if rl_blocked(&st, &ikey, 10, 600) {
        return Err(too_many());
    }
    rl_hit(&st, &ikey, 600);
    // Khoá toàn cục theo số lần SAI (không phụ thuộc IP/header, nên không né được bằng X-Forwarded-For giả).
    if rl_blocked(&st, "pairfail:all", 20, 300) {
        return Err(too_many());
    }
    let code = r.code.trim();
    let now = now_secs();
    let found = {
        let mut g = st.pair_codes.lock().unwrap();
        g.retain(|_, (_, exp)| *exp > now);
        g.remove(code)
    };
    let Some((uid, _)) = found else {
        rl_hit(&st, "pairfail:all", 300);
        return Err((StatusCode::UNAUTHORIZED, "Mã sai hoặc đã hết hạn".into()));
    };
    let db = st.db.lock().unwrap();
    let user = db
        .query_row(&format!("SELECT {USER_COLS} FROM users WHERE id=?1"), [uid], row_to_user)
        .map_err(|_| (StatusCode::UNAUTHORIZED, "Tài khoản không còn tồn tại".into()))?;
    let token = new_token();
    db.execute("INSERT INTO sessions(token,user_id) VALUES(?1,?2)", params![tok_hash(&token), uid]).map_err(|_| e500())?;
    Ok(Json(AuthResp { token, user }))
}

async fn ws_handler(ws: WebSocketUpgrade, Path(room): Path<String>, Query(q): Query<WsQ>, State(st): State<AppState>) -> Response {
    let Some(user) = take_ws_ticket(&st, &q.ticket) else { return StatusCode::UNAUTHORIZED.into_response() };
    if room.is_empty() || room.len() > 40 {
        return StatusCode::BAD_REQUEST.into_response();
    }
    ws.on_upgrade(move |s| handle_socket(s, room, user, st))
}

async fn handle_socket(socket: WebSocket, room: String, user: User, st: AppState) {
    let (mut sender, mut receiver) = socket.split();
    // Vào phòng: đăng ký kết nối. `first` = đây là kết nối đầu tiên của người này (không phải kết nối lại sau khi chập chờn mạng).
    let (tx, mut rx, last, first) = {
        let mut g = st.rooms.lock().unwrap();
        let r = g
            .entry(room.clone())
            .or_insert_with(|| Room { tx: broadcast::channel(64).0, last_state: None, members: HashMap::new() });
        let first = !r.members.contains_key(&user.id);
        *r.members.entry(user.id).or_insert(0) += 1;
        (r.tx.clone(), r.tx.subscribe(), r.last_state.clone(), first)
    };
    if let Some(s) = last {
        let _ = sender.send(Message::Text(s)).await;
    }
    let mut send_task = tokio::spawn(async move {
        while let Ok(m) = rx.recv().await {
            if sender.send(Message::Text(m)).await.is_err() {
                break;
            }
        }
    });
    let recv_room = room.clone();
    let recv_st = st.clone();
    let recv_user = user.clone();
    let recv_tx = tx.clone();
    let mut recv_task = tokio::spawn(async move {
        let (st, room, user, tx) = (recv_st, recv_room, recv_user, recv_tx);
        // Mỗi kết nối chỉ được báo "vào phòng" tối đa 1 lần, và chỉ khi là kết nối đầu tiên của người đó.
        let mut announce = first;
        while let Some(Ok(msg)) = receiver.next().await {
            let Message::Text(t) = msg else { continue };
            if t.len() > 4096 {
                continue;
            }
            let Ok(mut v) = serde_json::from_str::<serde_json::Value>(&t) else { continue };
            let ty = v.get("type").and_then(|x| x.as_str()).unwrap_or("").to_string();
            if !["hello", "chat", "state"].contains(&ty.as_str()) {
                continue;
            }
            if ty == "hello" {
                if !announce {
                    continue;
                }
                announce = false;
            }
            // Server tự gắn danh tính để không ai giả mạo người khác.
            v["from"] = json!(user.display_name);
            v["avatar"] = json!(user.avatar);
            let out = v.to_string();
            if ty == "state" {
                let mut g = st.rooms.lock().unwrap();
                if let Some(r) = g.get_mut(&room) {
                    r.last_state = Some(out.clone());
                }
            }
            let _ = tx.send(out);
        }
    });
    tokio::select! {
        _ = &mut send_task => recv_task.abort(),
        _ = &mut recv_task => send_task.abort(),
    }
    // Rời phòng: hết kết nối nào của người này thì chờ vài giây (để app kịp nối lại khi mạng chập chờn) rồi mới báo "thoát phòng".
    let left_to_zero = {
        let mut g = st.rooms.lock().unwrap();
        match g.get_mut(&room).and_then(|r| r.members.get_mut(&user.id)) {
            Some(c) => {
                *c = c.saturating_sub(1);
                *c == 0
            }
            None => false,
        }
    };
    if left_to_zero {
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(6)).await;
            let mut g = st.rooms.lock().unwrap();
            let Some(r) = g.get_mut(&room) else { return };
            if r.members.get(&user.id) == Some(&0) {
                r.members.remove(&user.id);
                let _ = r.tx.send(json!({ "type": "leave", "from": user.display_name, "avatar": user.avatar }).to_string());
                // Phòng trống hẳn thì dọn luôn để không rò bộ nhớ.
                if r.members.is_empty() && r.tx.receiver_count() == 0 {
                    g.remove(&room);
                }
            }
        });
    }
}

// Giờ của server (ms) – client dùng để căn đồng hồ, tránh lệch giờ giữa các máy.
async fn time_now() -> Json<serde_json::Value> {
    let ms = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0);
    Json(json!({ "t": ms }))
}

#[derive(Deserialize)]
struct InviteReq {
    friend_id: i64,
    room: String,
}

async fn invite(State(st): State<AppState>, headers: HeaderMap, Json(r): Json<InviteReq>) -> Result<StatusCode, ApiErr> {
    let user = require(&st, &headers)?;
    if r.room.is_empty() || r.room.len() > 40 || !r.room.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_') {
        return Err(bad("Mã phòng không hợp lệ"));
    }
    let is_friend = {
        let db = st.db.lock().unwrap();
        let x = db
            .query_row(
                "SELECT 1 FROM friends WHERE user_id=?1 AND friend_id=?2",
                params![user.id, r.friend_id],
                |_| Ok(()),
            )
            .is_ok();
        x
    };
    if !is_friend {
        return Err((StatusCode::FORBIDDEN, "Người này chưa là bạn của bạn".into()));
    }
    let tx = st.notify.lock().unwrap().get(&r.friend_id).cloned();
    let msg = json!({ "type": "invite", "from": user.display_name, "avatar": user.avatar, "room": r.room }).to_string();
    match tx {
        Some(tx) if tx.send(msg).is_ok() => Ok(StatusCode::OK),
        _ => Err((StatusCode::CONFLICT, "Bạn ấy đang offline".into())),
    }
}

async fn ws_user(ws: WebSocketUpgrade, Query(q): Query<WsQ>, State(st): State<AppState>) -> Response {
    let Some(user) = take_ws_ticket(&st, &q.ticket) else { return StatusCode::UNAUTHORIZED.into_response() };
    ws.on_upgrade(move |s| handle_user_socket(s, user.id, st))
}

async fn handle_user_socket(socket: WebSocket, uid: i64, st: AppState) {
    let (mut sender, mut receiver) = socket.split();
    let mut rx = {
        let mut g = st.notify.lock().unwrap();
        g.entry(uid).or_insert_with(|| broadcast::channel(16).0).subscribe()
    };
    loop {
        tokio::select! {
            m = rx.recv() => match m {
                Ok(t) => { if sender.send(Message::Text(t)).await.is_err() { break; } }
                Err(broadcast::error::RecvError::Lagged(_)) => {}
                Err(_) => break,
            },
            c = receiver.next() => match c {
                Some(Ok(Message::Close(_))) | Some(Err(_)) | None => break,
                _ => {}
            },
        }
    }
    drop(rx);
    let mut g = st.notify.lock().unwrap();
    if let Some(tx) = g.get(&uid) {
        if tx.receiver_count() == 0 {
            g.remove(&uid);
        }
    }
}

// ---------- Yêu thích, lịch sử, playlist ----------
fn track_key(v: &Value) -> Option<String> {
    let src = v.get("src")?.as_str()?;
    if src != "jm" && src != "yt" && src != "au" {
        return None;
    }
    let id = match v.get("id")? {
        Value::String(s) => s.clone(),
        Value::Number(n) => n.to_string(),
        _ => return None,
    };
    if id.is_empty() || id.len() > 64 {
        return None;
    }
    Some(format!("{src}:{id}"))
}
fn valid_track(v: &Value) -> Result<(String, String), ApiErr> {
    let k = track_key(v).ok_or_else(|| bad("Bài hát không hợp lệ"))?;
    let s = v.to_string();
    if s.len() > 4096 {
        return Err(bad("Dữ liệu bài hát quá lớn"));
    }
    Ok((k, s))
}
fn parse_rows(stmt: &mut rusqlite::Statement, p: impl rusqlite::Params) -> Result<Vec<Value>, ApiErr> {
    let rows = stmt.query_map(p, |r| r.get::<_, String>(0)).map_err(|_| e500())?;
    let v: Vec<Value> = rows.filter_map(|x| x.ok()).filter_map(|s| serde_json::from_str::<Value>(&s).ok()).collect();
    Ok(v)
}
fn owns(db: &Connection, pid: i64, uid: i64) -> bool {
    db.query_row("SELECT 1 FROM playlists WHERE id=?1 AND user_id=?2", params![pid, uid], |_| Ok(())).is_ok()
}

// ===== Tài khoản: đổi tên hiển thị, đổi mật khẩu =====
#[derive(Deserialize)]
struct NameChange {
    display_name: String,
}

async fn account_name(State(st): State<AppState>, headers: HeaderMap, Json(r): Json<NameChange>) -> R<User> {
    let user = require(&st, &headers)?;
    let name: String = r.display_name.chars().filter(|c| !c.is_control()).collect::<String>().trim().chars().take(24).collect();
    if name.is_empty() {
        return Err(bad("Tên hiển thị không được trống"));
    }
    let db = st.db.lock().unwrap();
    db.execute("UPDATE users SET display_name=?1 WHERE id=?2", params![name, user.id]).map_err(|_| e500())?;
    let out = db
        .query_row(&format!("SELECT {USER_COLS} FROM users WHERE id=?1"), [user.id], row_to_user)
        .map_err(|_| e500())?;
    Ok(Json(out))
}

#[derive(Deserialize)]
struct PwChange {
    current: String,
    new: String,
}

async fn account_password(State(st): State<AppState>, headers: HeaderMap, Json(r): Json<PwChange>) -> Result<StatusCode, ApiErr> {
    let user = require(&st, &headers)?;
    let token = bearer(&headers).ok_or_else(unauth)?;
    if r.new.len() < 8 || r.new.len() > 128 {
        return Err(bad("Mật khẩu mới từ 8 đến 128 ký tự"));
    }
    let old_hash: String = {
        let db = st.db.lock().unwrap();
        db.query_row("SELECT pass_hash FROM users WHERE id=?1", [user.id], |x| x.get(0)).map_err(|_| e500())?
    };
    let cur = r.current.clone();
    let ok = tokio::task::spawn_blocking(move || verify_pw(&cur, &old_hash)).await.unwrap_or(false);
    if !ok {
        return Err((StatusCode::FORBIDDEN, "Mật khẩu hiện tại không đúng".into()));
    }
    let newpw = r.new.clone();
    let hash = tokio::task::spawn_blocking(move || hash_pw(&newpw)).await.map_err(|_| e500())?.map_err(|_| e500())?;
    let db = st.db.lock().unwrap();
    db.execute("UPDATE users SET pass_hash=?1 WHERE id=?2", params![hash, user.id]).map_err(|_| e500())?;
    // Đăng xuất mọi thiết bị khác, giữ phiên hiện tại.
    let _ = db.execute("DELETE FROM sessions WHERE user_id=?1 AND token<>?2", params![user.id, tok_hash(&token)]);
    Ok(StatusCode::OK)
}

#[derive(Deserialize)]
struct AccDel {
    password: String,
}

// Xóa tài khoản vĩnh viễn: cần nhập đúng mật khẩu. Xóa luôn bạn bè, yêu thích, lịch sử, playlist, dữ liệu đồng bộ (ON DELETE CASCADE) và ảnh đại diện/nền/banner.
async fn account_delete(State(st): State<AppState>, headers: HeaderMap, Json(r): Json<AccDel>) -> Result<StatusCode, ApiErr> {
    let user = require(&st, &headers)?;
    let (hash, av, bg, bn): (String, String, String, String) = {
        let db = st.db.lock().unwrap();
        db.query_row("SELECT pass_hash, avatar, bg_url, banner FROM users WHERE id=?1", [user.id], |x| Ok((x.get(0)?, x.get(1)?, x.get(2)?, x.get(3)?)))
            .map_err(|_| e500())?
    };
    let pw = r.password.clone();
    let ok = tokio::task::spawn_blocking(move || verify_pw(&pw, &hash)).await.unwrap_or(false);
    if !ok {
        return Err((StatusCode::FORBIDDEN, "Mật khẩu không đúng".into()));
    }
    {
        let db = st.db.lock().unwrap();
        db.execute("DELETE FROM users WHERE id=?1", [user.id]).map_err(|_| e500())?;
    }
    for (prefix, dir) in [("/avatars/", &st.avatar_dir), ("/backgrounds/", &st.bg_dir), ("/banners/", &st.banner_dir)] {
        for url in [&av, &bg, &bn] {
            if let Some(f) = url.strip_prefix(prefix) {
                if !f.contains('/') && !f.contains("..") && !f.is_empty() {
                    remove_upload_file(&dir, f).await;
                }
            }
        }
    }
    Ok(StatusCode::NO_CONTENT)
}

// ===== Cài đặt người dùng (lưu dạng JSON, chỉ nhận các khóa đã biết) =====
const BOOL_SETTINGS: [&str; 11] = [
    "track_history", "save_state", "crossfade", "explicit", "offline_pl", "proxy", "eq_on", "delay_on", "reverb_on", "discord_rpc", "show_errors",
];
// (khóa, nhỏ nhất, lớn nhất, mặc định)
const NUM_SETTINGS: [(&str, i64, i64, i64); 6] = [
    ("crossfade_secs", 1, 12, 5),
    ("delay_ms", 50, 1000, 300),
    ("delay_fb", 0, 90, 35),
    ("delay_mix", 0, 100, 35),
    ("reverb_mix", 0, 100, 30),
    ("eq_pre", -12, 12, 0),
];
const REVERB_TYPES: [&str; 3] = ["room", "hall", "plate"];
const EQ_BANDS: usize = 10;

fn default_settings() -> serde_json::Map<String, Value> {
    let mut m = serde_json::Map::new();
    m.insert("track_history".into(), json!(true));
    for k in BOOL_SETTINGS {
        if k != "track_history" {
            m.insert(k.into(), json!(false));
        }
    }
    for (k, _, _, d) in NUM_SETTINGS {
        m.insert(k.into(), json!(d));
    }
    m.insert("reverb_type".into(), json!("hall"));
    m.insert("eq_gains".into(), json!(vec![0; EQ_BANDS]));
    m
}

fn clean_gains(v: &Value) -> Option<Value> {
    let a = v.as_array()?;
    if a.len() != EQ_BANDS {
        return None;
    }
    let mut out = Vec::new();
    for x in a {
        let n = x.as_f64()?;
        out.push(json!((n.clamp(-12.0, 12.0) * 10.0).round() / 10.0));
    }
    Some(Value::Array(out))
}

fn load_settings(db: &Connection, uid: i64) -> serde_json::Map<String, Value> {
    let raw: String = db.query_row("SELECT settings FROM users WHERE id=?1", [uid], |r| r.get(0)).unwrap_or_default();
    let mut out = default_settings();
    if let Ok(Value::Object(saved)) = serde_json::from_str::<Value>(&raw) {
        for (k, v) in saved {
            if !out.contains_key(&k) {
                continue;
            }
            if k == "eq_gains" {
                if let Some(g) = clean_gains(&v) {
                    out.insert(k, g);
                }
            } else {
                out.insert(k, v);
            }
        }
    }
    out
}

async fn settings_get(State(st): State<AppState>, headers: HeaderMap) -> R<Value> {
    let u = require(&st, &headers)?;
    let db = st.db.lock().unwrap();
    Ok(Json(Value::Object(load_settings(&db, u.id))))
}

async fn settings_set(State(st): State<AppState>, headers: HeaderMap, Json(patch): Json<Value>) -> R<Value> {
    let u = require(&st, &headers)?;
    let Value::Object(patch) = patch else { return Err(bad("Dữ liệu không hợp lệ")) };
    let db = st.db.lock().unwrap();
    let mut cur = load_settings(&db, u.id);
    for (k, v) in patch {
        if BOOL_SETTINGS.contains(&k.as_str()) {
            let Some(b) = v.as_bool() else { return Err(bad("Giá trị cài đặt không hợp lệ")) };
            if k == "discord_rpc" && b {
                let linked: String = db
                    .query_row("SELECT discord_id FROM users WHERE id=?1", [u.id], |r| r.get(0))
                    .unwrap_or_default();
                if linked.is_empty() {
                    return Err(bad("Hãy đăng nhập Discord để liên kết tài khoản trước"));
                }
            }
            if k == "show_errors" && b && !u.is_admin {
                return Err((StatusCode::FORBIDDEN, "Chỉ admin mới bật được thông báo lỗi".into()));
            }
            cur.insert(k, json!(b));
        } else if let Some((_, lo, hi, _)) = NUM_SETTINGS.iter().find(|x| x.0 == k.as_str()) {
            let Some(n) = v.as_f64() else { return Err(bad("Giá trị cài đặt không hợp lệ")) };
            cur.insert(k, json!((n.round() as i64).clamp(*lo, *hi)));
        } else if k == "reverb_type" {
            let Some(t) = v.as_str().filter(|t| REVERB_TYPES.contains(t)) else { return Err(bad("Giá trị cài đặt không hợp lệ")) };
            cur.insert(k, json!(t));
        } else if k == "eq_gains" {
            let Some(g) = clean_gains(&v) else { return Err(bad("Giá trị cài đặt không hợp lệ")) };
            cur.insert(k, g);
        }
    }
    let s = Value::Object(cur.clone()).to_string();
    db.execute("UPDATE users SET settings=?1 WHERE id=?2", params![s, u.id]).map_err(|_| e500())?;
    Ok(Json(Value::Object(cur)))
}

// ===== Lịch sử =====
async fn hist_clear(State(st): State<AppState>, headers: HeaderMap) -> Result<StatusCode, ApiErr> {
    let u = require(&st, &headers)?;
    let db = st.db.lock().unwrap();
    db.execute("DELETE FROM history WHERE user_id=?1", [u.id]).map_err(|_| e500())?;
    Ok(StatusCode::OK)
}

// ===== Sao lưu / khôi phục / nhập playlist =====
const MAX_FAVS: usize = 5000;
const MAX_PLAYLISTS: usize = 50;
const MAX_PL_TRACKS: usize = 5000;

async fn backup_export(State(st): State<AppState>, headers: HeaderMap) -> R<Value> {
    let u = require(&st, &headers)?;
    let db = st.db.lock().unwrap();
    let favorites = {
        let mut stmt = db.prepare("SELECT track_json FROM favorites WHERE user_id=?1 ORDER BY rowid").map_err(|_| e500())?;
        let v = parse_rows(&mut stmt, [u.id])?;
        v
    };
    let history = {
        let mut stmt = db.prepare("SELECT track_json FROM history WHERE user_id=?1 ORDER BY rowid").map_err(|_| e500())?;
        let v = parse_rows(&mut stmt, [u.id])?;
        v
    };
    let mut playlists: Vec<Value> = Vec::new();
    let pls: Vec<(i64, String)> = {
        let mut stmt = db.prepare("SELECT id, name FROM playlists WHERE user_id=?1 ORDER BY id").map_err(|_| e500())?;
        let rows = stmt.query_map([u.id], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?))).map_err(|_| e500())?;
        let v: Vec<(i64, String)> = rows.filter_map(|x| x.ok()).collect();
        v
    };
    for (pid, name) in pls {
        let mut stmt = db.prepare("SELECT track_json FROM playlist_tracks WHERE playlist_id=?1 ORDER BY rowid").map_err(|_| e500())?;
        let tracks = parse_rows(&mut stmt, [pid])?;
        playlists.push(json!({ "name": name, "tracks": tracks }));
    }
    let settings = Value::Object(load_settings(&db, u.id));
    let sync: serde_json::Map<String, Value> = sync_read(&db, u.id).into_iter().map(|(k, (v, _))| (k, v)).collect();
    Ok(Json(json!({
        "app": "muse",
        "version": 1,
        "sync": sync,
        "exported_at": SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0),
        "favorites": favorites,
        "history": history,
        "playlists": playlists,
        "settings": settings,
    })))
}

// Chèn danh sách playlist (mỗi phần tử {name, tracks:[...]}) cho người dùng; trả về (số playlist, số bài).
fn insert_playlists(db: &Connection, uid: i64, list: &[Value]) -> Result<(usize, usize), ApiErr> {
    let mut have: usize = db.query_row("SELECT COUNT(*) FROM playlists WHERE user_id=?1", [uid], |r| r.get::<_, i64>(0)).unwrap_or(0) as usize;
    let (mut np, mut nt) = (0usize, 0usize);
    for p in list {
        if have >= MAX_PLAYLISTS {
            break;
        }
        let name: String = p
            .get("name")
            .and_then(|x| x.as_str())
            .unwrap_or("Playlist đã nhập")
            .chars()
            .filter(|c| !c.is_control())
            .collect::<String>()
            .trim()
            .chars()
            .take(40)
            .collect();
        let name = if name.is_empty() { "Playlist đã nhập".to_string() } else { name };
        db.execute("INSERT INTO playlists(user_id,name) VALUES(?1,?2)", params![uid, name]).map_err(|_| e500())?;
        let pid = db.last_insert_rowid();
        have += 1;
        np += 1;
        if let Some(tracks) = p.get("tracks").and_then(|x| x.as_array()) {
            for t in tracks.iter().take(MAX_PL_TRACKS) {
                if let Ok((k, s)) = valid_track(t) {
                    if db
                        .execute("INSERT OR IGNORE INTO playlist_tracks(playlist_id,track_key,track_json) VALUES(?1,?2,?3)", params![pid, k, s])
                        .map(|n| n > 0)
                        .unwrap_or(false)
                    {
                        nt += 1;
                    }
                }
            }
        }
    }
    Ok((np, nt))
}

// Nhập thêm playlist (không xóa dữ liệu hiện có). Nhận {"playlists":[...]} hoặc một mảng playlist.
async fn import_playlists(State(st): State<AppState>, headers: HeaderMap, Json(body): Json<Value>) -> R<Value> {
    let u = require(&st, &headers)?;
    let list: Vec<Value> = match &body {
        Value::Array(a) => a.clone(),
        Value::Object(o) => o.get("playlists").and_then(|x| x.as_array()).cloned().unwrap_or_default(),
        _ => Vec::new(),
    };
    if list.is_empty() {
        return Err(bad("Không tìm thấy danh sách phát nào trong file"));
    }
    let db = st.db.lock().unwrap();
    let (np, nt) = insert_playlists(&db, u.id, &list)?;
    Ok(Json(json!({ "playlists": np, "tracks": nt })))
}

// Khôi phục từ file sao lưu: thay thế yêu thích, lịch sử và playlist hiện có.
async fn backup_restore(State(st): State<AppState>, headers: HeaderMap, Json(body): Json<Value>) -> R<Value> {
    let u = require(&st, &headers)?;
    if body.get("app").and_then(|x| x.as_str()) != Some("muse") {
        return Err(bad("Đây không phải file sao lưu của Muse"));
    }
    let favs: Vec<Value> = body.get("favorites").and_then(|x| x.as_array()).cloned().unwrap_or_default();
    let hist: Vec<Value> = body.get("history").and_then(|x| x.as_array()).cloned().unwrap_or_default();
    let pls: Vec<Value> = body.get("playlists").and_then(|x| x.as_array()).cloned().unwrap_or_default();
    let mut db = st.db.lock().unwrap();
    let tx = db.transaction().map_err(|_| e500())?;
    tx.execute("DELETE FROM favorites WHERE user_id=?1", [u.id]).map_err(|_| e500())?;
    tx.execute("DELETE FROM history WHERE user_id=?1", [u.id]).map_err(|_| e500())?;
    tx.execute("DELETE FROM playlists WHERE user_id=?1", [u.id]).map_err(|_| e500())?;
    let mut nf = 0usize;
    for t in favs.iter().take(MAX_FAVS) {
        if let Ok((k, s)) = valid_track(t) {
            if tx.execute("INSERT OR IGNORE INTO favorites(user_id,track_key,track_json) VALUES(?1,?2,?3)", params![u.id, k, s]).map(|n| n > 0).unwrap_or(false) {
                nf += 1;
            }
        }
    }
    for t in hist.iter().take(100) {
        if let Ok((k, s)) = valid_track(t) {
            let _ = tx.execute("INSERT OR IGNORE INTO history(user_id,track_key,track_json) VALUES(?1,?2,?3)", params![u.id, k, s]);
        }
    }
    let (np, nt) = insert_playlists(&tx, u.id, &pls)?;
    // Dữ liệu nghe đồng bộ (nếu file sao lưu có) cũng được khôi phục.
    if let Some(sy) = body.get("sync").and_then(|x| x.as_object()) {
        let now_ms = (SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis()).unwrap_or(0)) as i64;
        tx.execute("DELETE FROM user_sync WHERE user_id=?1", [u.id]).map_err(|_| e500())?;
        for key in SYNC_KEYS {
            if let Some(v) = sy.get(key).and_then(|x| sync_clean(key, x)) {
                let s = v.to_string();
                if s.len() <= SYNC_MAX_BYTES {
                    tx.execute("INSERT INTO user_sync(user_id,k,v,ts) VALUES(?1,?2,?3,?4)", params![u.id, key, s, now_ms]).map_err(|_| e500())?;
                }
            }
        }
    }
    tx.commit().map_err(|_| e500())?;
    Ok(Json(json!({ "favorites": nf, "playlists": np, "tracks": nt })))
}

async fn fav_list(State(st): State<AppState>, headers: HeaderMap) -> R<Vec<Value>> {
    let u = require(&st, &headers)?;
    let db = st.db.lock().unwrap();
    let mut stmt = db.prepare("SELECT track_json FROM favorites WHERE user_id=?1 ORDER BY rowid DESC").map_err(|_| e500())?;
    let v = parse_rows(&mut stmt, [u.id])?;
    Ok(Json(v))
}

async fn fav_toggle(State(st): State<AppState>, headers: HeaderMap, Json(t): Json<Value>) -> R<Value> {
    let u = require(&st, &headers)?;
    let (k, s) = valid_track(&t)?;
    let db = st.db.lock().unwrap();
    let n = db.execute("DELETE FROM favorites WHERE user_id=?1 AND track_key=?2", params![u.id, k]).map_err(|_| e500())?;
    if n > 0 {
        return Ok(Json(json!({ "fav": false })));
    }
    db.execute("INSERT INTO favorites(user_id,track_key,track_json) VALUES(?1,?2,?3)", params![u.id, k, s])
        .map_err(|_| e500())?;
    Ok(Json(json!({ "fav": true })))
}

// ===== Bình luận theo bài hát: chỉ lưu và hiển thị trong app này (không liên quan bình luận YouTube) =====
#[derive(Deserialize)]
struct CommentsQ {
    key: String,
}

#[derive(Deserialize)]
struct CommentReq {
    key: String,
    body: String,
}

fn valid_comment_key(k: &str) -> bool {
    let ok_src = k.starts_with("yt:") || k.starts_with("jm:") || k.starts_with("au:") || k.starts_with("local:");
    ok_src && k.len() <= 200 && !k.chars().any(|c| c.is_control())
}

fn comment_row(r: &rusqlite::Row, me: i64, admin: bool) -> rusqlite::Result<Value> {
    let uid: i64 = r.get(1)?;
    Ok(json!({
        "id": r.get::<_, i64>(0)?,
        "user_id": uid,
        "body": r.get::<_, String>(2)?,
        "created_at": r.get::<_, String>(3)?,
        "display_name": r.get::<_, String>(4)?,
        "avatar": r.get::<_, String>(5)?,
        "accent": r.get::<_, String>(6)?,
        "can_delete": uid == me || admin,
    }))
}

const COMMENT_SQL: &str = "SELECT c.id, c.user_id, c.body, c.created_at, u.display_name, u.avatar, u.accent FROM comments c JOIN users u ON u.id=c.user_id";

async fn comments_list(State(st): State<AppState>, headers: HeaderMap, Query(q): Query<CommentsQ>) -> Result<Json<Value>, ApiErr> {
    let user = require(&st, &headers)?;
    if !valid_comment_key(&q.key) {
        return Err(bad("Bài hát không hợp lệ"));
    }
    let db = st.db.lock().unwrap();
    let total: i64 = db
        .query_row("SELECT COUNT(*) FROM comments WHERE track_key=?1", [&q.key], |r| r.get(0))
        .map_err(|_| e500())?;
    let sql = format!("{COMMENT_SQL} WHERE c.track_key=?1 ORDER BY c.id DESC LIMIT 200");
    let mut stmt = db.prepare(&sql).map_err(|_| e500())?;
    let rows = stmt
        .query_map([&q.key], |r| comment_row(r, user.id, user.is_admin))
        .map_err(|_| e500())?;
    let items: Vec<Value> = rows.filter_map(|x| x.ok()).collect();
    Ok(Json(json!({ "count": total, "items": items })))
}

async fn comments_add(State(st): State<AppState>, headers: HeaderMap, Json(r): Json<CommentReq>) -> Result<Json<Value>, ApiErr> {
    let user = require(&st, &headers)?;
    if !valid_comment_key(&r.key) {
        return Err(bad("Bài hát không hợp lệ"));
    }
    let body: String = r.body.trim().chars().take(500).collect();
    if body.is_empty() {
        return Err(bad("Bình luận không được trống"));
    }
    let db = st.db.lock().unwrap();
    // chống spam: tối đa 5 bình luận / 30 giây / người
    let recent: i64 = db
        .query_row(
            "SELECT COUNT(*) FROM comments WHERE user_id=?1 AND created_at > datetime('now','-30 seconds')",
            [user.id],
            |r| r.get(0),
        )
        .map_err(|_| e500())?;
    if recent >= 5 {
        return Err(bad("Bạn bình luận hơi nhanh, đợi một chút nhé"));
    }
    db.execute(
        "INSERT INTO comments(track_key,user_id,body) VALUES(?1,?2,?3)",
        params![r.key, user.id, body],
    )
    .map_err(|_| e500())?;
    let id = db.last_insert_rowid();
    let sql = format!("{COMMENT_SQL} WHERE c.id=?1");
    let item = db
        .query_row(&sql, [id], |r| comment_row(r, user.id, user.is_admin))
        .map_err(|_| e500())?;
    let total: i64 = db
        .query_row("SELECT COUNT(*) FROM comments WHERE track_key=?1", [&r.key], |r| r.get(0))
        .map_err(|_| e500())?;
    Ok(Json(json!({ "count": total, "item": item })))
}

async fn comments_delete(State(st): State<AppState>, headers: HeaderMap, Path(id): Path<i64>) -> Result<StatusCode, ApiErr> {
    let user = require(&st, &headers)?;
    let db = st.db.lock().unwrap();
    let res = if user.is_admin {
        db.execute("DELETE FROM comments WHERE id=?1", [id])
    } else {
        db.execute("DELETE FROM comments WHERE id=?1 AND user_id=?2", params![id, user.id])
    };
    let n = res.map_err(|_| e500())?;
    if n == 0 {
        return Err((StatusCode::NOT_FOUND, "Không tìm thấy bình luận".into()));
    }
    Ok(StatusCode::NO_CONTENT)
}

async fn hist_list(State(st): State<AppState>, headers: HeaderMap) -> R<Vec<Value>> {
    let u = require(&st, &headers)?;
    let db = st.db.lock().unwrap();
    let mut stmt = db
        .prepare("SELECT track_json FROM history WHERE user_id=?1 ORDER BY rowid DESC LIMIT 100")
        .map_err(|_| e500())?;
    let v = parse_rows(&mut stmt, [u.id])?;
    Ok(Json(v))
}

async fn hist_add(State(st): State<AppState>, headers: HeaderMap, Json(t): Json<Value>) -> Result<StatusCode, ApiErr> {
    let u = require(&st, &headers)?;
    let (k, s) = valid_track(&t)?;
    let db = st.db.lock().unwrap();
    db.execute("INSERT OR REPLACE INTO history(user_id,track_key,track_json) VALUES(?1,?2,?3)", params![u.id, k, s])
        .map_err(|_| e500())?;
    let _ = db.execute(
        "DELETE FROM history WHERE user_id=?1 AND rowid NOT IN (SELECT rowid FROM history WHERE user_id=?1 ORDER BY rowid DESC LIMIT 100)",
        params![u.id],
    );
    Ok(StatusCode::OK)
}

// ===== Chuẩn hóa tên nghệ sĩ (alias) =====
// YouTube hay gắn TÊN KÊNH (vd. "Abuse") thay cho tên nghệ sĩ thật ("Abu-Se-Ken"). Các tên này phải được tính là MỘT nghệ sĩ,
// nếu không lịch sử nghe bị chia đôi và gợi ý sai. Khóa so khớp = tên viết thường, chỉ giữ chữ/số ("Abu Se Ken" == "abu-se-ken").
// Muốn gộp thêm nghệ sĩ khác: thêm một dòng vào đây.
const ARTIST_ALIASES: &[(&str, &[&str])] = &[("Abu-Se-Ken", &["Abuse", "Abu Se Ken", "AbuSeKen", "アブセケン"])];

fn art_norm(s: &str) -> String {
    s.chars().filter(|c| c.is_alphanumeric()).flat_map(|c| c.to_lowercase()).collect()
}

fn alias_map() -> &'static HashMap<String, String> {
    static M: std::sync::OnceLock<HashMap<String, String>> = std::sync::OnceLock::new();
    M.get_or_init(|| {
        let mut m = HashMap::new();
        for (canon, aliases) in ARTIST_ALIASES {
            m.insert(art_norm(canon), canon.to_string());
            for a in aliases.iter() {
                m.insert(art_norm(a), canon.to_string());
            }
        }
        m
    })
}

// Tên nghệ sĩ chuẩn (alias → tên chính); tên lạ giữ nguyên.
fn canon_artist(name: &str) -> String {
    let n = name.trim();
    alias_map().get(&art_norm(n)).cloned().unwrap_or_else(|| n.to_string())
}

// Tách chuỗi nhiều nghệ sĩ ("A & B", "A feat. B", "A, B"...) thành danh sách tên chuẩn, không trùng.
fn split_artists(s: &str) -> Vec<String> {
    const SEPS: [&str; 11] = [",", "&", ";", "/", "、", "・", " x ", " ft. ", " ft ", " feat. ", " feat "];
    let lower = s.to_ascii_lowercase(); // giữ nguyên độ dài byte nên cắt trên `s` theo vị trí của `lower` là an toàn
    let mut parts: Vec<&str> = Vec::new();
    let (mut start, mut i) = (0usize, 0usize);
    while i < lower.len() {
        if !lower.is_char_boundary(i) {
            i += 1;
            continue;
        }
        if let Some(sp) = SEPS.iter().find(|p| lower[i..].starts_with(**p)) {
            parts.push(&s[start..i]);
            i += sp.len();
            start = i;
        } else {
            i += 1;
        }
    }
    parts.push(&s[start..]);
    let mut seen: HashSet<String> = HashSet::new();
    let mut out = Vec::new();
    for p in parts {
        let p = p.trim();
        let n = p.chars().count();
        if !(2..40).contains(&n) || !p.chars().any(|c| c.is_alphabetic()) {
            continue;
        }
        let c = canon_artist(p);
        if seen.insert(c.to_lowercase()) {
            out.push(c);
        }
    }
    out
}

fn track_artists(t: &Value) -> Vec<String> {
    split_artists(t.get("artist").and_then(|x| x.as_str()).unwrap_or(""))
}

// ===== Đồng bộ dữ liệu nghe theo tài khoản =====
// Client giữ bản trên máy (localStorage) và đẩy lên đây; server gộp rồi trả bản đã gộp.
//  - listen: {khóa bài: {t, ok, sk, last}}  → từng bài lấy bản mới hơn (cùng mốc thì lấy số lớn hơn)
//  - plays : {khóa bài: {t, n, last}}       → số lượt phát chỉ tăng nên lấy lớn nhất
//  - dislike / follow / jump                → bản có mốc `ts` mới hơn thắng; cùng mốc thì hợp nhất
const SYNC_KEYS: [&str; 5] = ["listen", "plays", "dislike", "follow", "jump"];
const SYNC_MAX_BYTES: usize = 1_500_000;

fn jnum(v: &Value, k: &str) -> i64 {
    v.get(k).and_then(|x| x.as_i64().or_else(|| x.as_f64().map(|f| f as i64))).unwrap_or(0)
}

fn entry_ok(e: &Value) -> bool {
    e.is_object() && e.get("t").map(|t| valid_track(t).is_ok()).unwrap_or(false)
}

// Làm sạch một giá trị client gửi lên; None nếu sai dạng.
fn sync_clean(key: &str, v: &Value) -> Option<Value> {
    match key {
        "listen" | "plays" | "dislike" => {
            let o = v.as_object()?;
            let m: serde_json::Map<String, Value> = o.iter().filter(|(_, e)| entry_ok(e)).take(1500).map(|(k, e)| (k.clone(), e.clone())).collect();
            Some(Value::Object(m))
        }
        "follow" => {
            let a = v.as_array()?;
            let mut seen: HashSet<String> = HashSet::new();
            let out: Vec<Value> = a
                .iter()
                .filter_map(|x| x.as_str())
                .map(|s| canon_artist(s))
                .filter(|s| !s.is_empty() && s.chars().count() <= 60 && seen.insert(s.to_lowercase()))
                .take(200)
                .map(Value::String)
                .collect();
            Some(Value::Array(out))
        }
        "jump" => {
            let a = v.as_array()?;
            let out: Vec<Value> = a.iter().filter(|x| x.get("n").map(|n| n.is_string()).unwrap_or(false) && x.get("tr").map(|t| t.is_array()).unwrap_or(false)).take(10).cloned().collect();
            Some(Value::Array(out))
        }
        _ => None,
    }
}

fn merge_entries(stored: &Value, pushed: &Value, counters: &[&str], take_max: bool, cap_by: &str, cap: usize) -> Value {
    let mut out: serde_json::Map<String, Value> = stored.as_object().cloned().unwrap_or_default();
    if let Some(b) = pushed.as_object() {
        for (k, nv) in b {
            match out.get(k).cloned() {
                None => {
                    out.insert(k.clone(), nv.clone());
                }
                Some(old) => {
                    let (a, b2) = (jnum(nv, "last"), jnum(&old, "last"));
                    let mut base = if a >= b2 { nv.clone() } else { old.clone() };
                    if let Some(m) = base.as_object_mut() {
                        for c in counters {
                            if take_max || a == b2 {
                                m.insert((*c).to_string(), json!(jnum(nv, c).max(jnum(&old, c))));
                            }
                        }
                        m.insert("last".into(), json!(a.max(b2)));
                    }
                    out.insert(k.clone(), base);
                }
            }
        }
    }
    if out.len() > cap {
        let mut ks: Vec<(String, i64)> = out.iter().map(|(k, e)| (k.clone(), jnum(e, cap_by))).collect();
        ks.sort_by(|x, y| y.1.cmp(&x.1));
        for (k, _) in ks.into_iter().skip(cap) {
            out.remove(&k);
        }
    }
    Value::Object(out)
}

fn merge_union(key: &str, a: &Value, b: &Value) -> Value {
    match key {
        "dislike" => {
            let mut out: serde_json::Map<String, Value> = a.as_object().cloned().unwrap_or_default();
            for (k, e) in b.as_object().into_iter().flatten() {
                if out.get(k).map(|o| jnum(o, "at") < jnum(e, "at")).unwrap_or(true) {
                    out.insert(k.clone(), e.clone());
                }
            }
            Value::Object(out)
        }
        "follow" => {
            let mut seen: HashSet<String> = HashSet::new();
            let v: Vec<Value> = a.as_array().into_iter().flatten().chain(b.as_array().into_iter().flatten()).filter(|x| x.as_str().map(|s| seen.insert(s.to_lowercase())).unwrap_or(false)).take(200).cloned().collect();
            Value::Array(v)
        }
        _ => {
            // jump: giữ danh sách đã có, thêm mục chưa có theo tên
            let mut seen: HashSet<String> = HashSet::new();
            let v: Vec<Value> = a.as_array().into_iter().flatten().chain(b.as_array().into_iter().flatten()).filter(|x| seen.insert(x.get("n").and_then(|n| n.as_str()).unwrap_or("").to_string())).take(10).cloned().collect();
            Value::Array(v)
        }
    }
}

fn sync_merge(key: &str, stored: Option<(Value, i64)>, pushed: Value, pts: i64) -> (Value, i64) {
    let Some((sv, sts)) = stored else { return (pushed, pts) };
    match key {
        "listen" => (merge_entries(&sv, &pushed, &["ok", "sk"], false, "last", 800), sts.max(pts)),
        "plays" => (merge_entries(&sv, &pushed, &["n"], true, "n", 800), sts.max(pts)),
        _ => {
            if pts > sts {
                (pushed, pts)
            } else if pts < sts {
                (sv, sts)
            } else {
                (merge_union(key, &sv, &pushed), sts)
            }
        }
    }
}

fn sync_read(db: &Connection, uid: i64) -> HashMap<String, (Value, i64)> {
    let mut out = HashMap::new();
    if let Ok(mut stmt) = db.prepare("SELECT k, v, ts FROM user_sync WHERE user_id=?1") {
        if let Ok(it) = stmt.query_map([uid], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?, r.get::<_, i64>(2)?))) {
            for (k, v, ts) in it.flatten() {
                if let Ok(j) = serde_json::from_str::<Value>(&v) {
                    out.insert(k, (j, ts));
                }
            }
        }
    }
    out
}

async fn sync_get(State(st): State<AppState>, headers: HeaderMap) -> R<Value> {
    let u = require(&st, &headers)?;
    let db = st.db.lock().unwrap();
    let mut out = serde_json::Map::new();
    for (k, (v, ts)) in sync_read(&db, u.id) {
        out.insert(k, json!({ "v": v, "ts": ts }));
    }
    Ok(Json(Value::Object(out)))
}

// Nhận {khóa: {v, ts}} (có thể rỗng), gộp với bản trên server rồi trả về toàn bộ bản đã gộp.
async fn sync_post(State(st): State<AppState>, headers: HeaderMap, Json(body): Json<Value>) -> R<Value> {
    let u = require(&st, &headers)?;
    let Value::Object(body) = body else { return Err(bad("Dữ liệu không hợp lệ")) };
    let db = st.db.lock().unwrap();
    let mut cur = sync_read(&db, u.id);
    let mut out = serde_json::Map::new();
    for key in SYNC_KEYS {
        let stored = cur.remove(key);
        let mut merged = stored.clone();
        if let Some(e) = body.get(key) {
            if let Some(pv) = e.get("v").and_then(|x| sync_clean(key, x)) {
                let m = sync_merge(key, stored.clone(), pv, jnum(e, "ts"));
                if m.0.to_string().len() <= SYNC_MAX_BYTES {
                    if stored.as_ref().map(|s| s.0 != m.0 || s.1 != m.1).unwrap_or(true) {
                        db.execute(
                            "INSERT INTO user_sync(user_id,k,v,ts) VALUES(?1,?2,?3,?4) ON CONFLICT(user_id,k) DO UPDATE SET v=excluded.v, ts=excluded.ts",
                            params![u.id, key, m.0.to_string(), m.1],
                        )
                        .map_err(|_| e500())?;
                    }
                    merged = Some(m);
                }
            }
        }
        if let Some((v, ts)) = merged {
            out.insert(key.to_string(), json!({ "v": v, "ts": ts }));
        }
    }
    Ok(Json(Value::Object(out)))
}

// ===== Gợi ý nhạc: item-item KNN với trọng số BM25 (ý tưởng từ thư viện `implicit`) =====
// "Hàng" = mỗi tài khoản (yêu thích + lịch sử, yêu thích nặng hơn như độ tin cậy của ALS) và mỗi playlist.
// Hai bài hay xuất hiện cùng nhau trong nhiều hàng thì giống nhau; bài chưa có trong thư viện của bạn
// nhưng giống những bài bạn thích sẽ được xếp lên đầu.
const BM25_K1: f64 = 100.0;
const BM25_B: f64 = 0.8;

type RecRows = std::collections::HashMap<String, std::collections::HashMap<String, f64>>;

fn rec_load(
    db: &Connection,
    sql: &str,
    prefix: &str,
    w: f64,
    me: i64,
    rows: &mut RecRows,
    meta: &mut std::collections::HashMap<String, String>,
    mine: &mut std::collections::HashMap<String, f64>,
    owned: &mut std::collections::HashSet<String>,
) -> Result<(), ApiErr> {
    let mut stmt = db.prepare(sql).map_err(|_| e500())?;
    let it = stmt
        .query_map([], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?, r.get::<_, String>(2)?, r.get::<_, String>(3)?)))
        .map_err(|_| e500())?;
    for (row_id, owner, key, js) in it.flatten() {
        let rk = format!("{}{}", prefix, row_id);
        *rows.entry(rk).or_default().entry(key.clone()).or_insert(0.0) += w;
        meta.entry(key.clone()).or_insert(js);
        if owner == me {
            *mine.entry(key.clone()).or_insert(0.0) += w;
            owned.insert(key);
        }
    }
    Ok(())
}

// ----- "Radio" của YouTube Music cho một bài (danh sách bài liên quan do YouTube tính từ hành vi nghe của hàng triệu người) -----
fn yt_panel_items<'a>(v: &'a Value, out: &mut Vec<&'a Value>) {
    match v {
        Value::Object(m) => {
            if let Some(r) = m.get("playlistPanelVideoRenderer") {
                out.push(r);
                return;
            }
            for x in m.values() {
                yt_panel_items(x, out);
            }
        }
        Value::Array(a) => {
            for x in a {
                yt_panel_items(x, out);
            }
        }
        _ => {}
    }
}

fn yt_panel_track(r: &Value) -> Option<Value> {
    let id = r.get("videoId")?.as_str()?;
    if id.len() != 11 {
        return None;
    }
    let title = r.get("title").map(yt_text).unwrap_or_default();
    if title.is_empty() {
        return None;
    }
    let by = r.get("longBylineText").map(yt_text).unwrap_or_default();
    let artist = by.split(" \u{2022} ").map(|x| x.trim()).find(|p| !yt_junk_part(p)).unwrap_or("").to_string();
    let dur = r.get("lengthText").map(yt_text).and_then(|t| yt_dur(&t)).unwrap_or(0);
    let image = r
        .pointer("/thumbnail/thumbnails")
        .and_then(|a| a.as_array())
        .and_then(|a| a.last())
        .and_then(|t| t.get("url"))
        .and_then(|u| u.as_str())
        .map(|x| x.to_string())
        .unwrap_or_else(|| format!("https://i.ytimg.com/vi/{id}/hqdefault.jpg"));
    let explicit = r.get("badges").map(|b| b.to_string().contains("MUSIC_EXPLICIT_BADGE")).unwrap_or(false);
    Some(json!({ "src": "yt", "id": id, "title": title, "artist": artist, "image": image, "url": "", "dur": dur, "explicit": explicit }))
}

static RADIO_CACHE: std::sync::OnceLock<Mutex<HashMap<String, (u64, Vec<Value>)>>> = std::sync::OnceLock::new();
fn radio_cache() -> &'static Mutex<HashMap<String, (u64, Vec<Value>)>> {
    RADIO_CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

// Bài của một nghệ sĩ (tìm "Tên songs", chỉ giữ bài mà nghệ sĩ – sau khi gộp alias – đúng là người đó), nhớ 6 giờ.
static ARTSONG_CACHE: std::sync::OnceLock<Mutex<HashMap<String, (u64, Vec<Value>)>>> = std::sync::OnceLock::new();
async fn artist_songs(client: &reqwest::Client, name: &str) -> Vec<Value> {
    let ck = art_norm(name);
    let cache = ARTSONG_CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    if let Some((t, v)) = cache.lock().unwrap().get(&ck) {
        if now_secs().saturating_sub(*t) < 6 * 3600 {
            return v.clone();
        }
    }
    let want = canon_artist(name).to_lowercase();
    let mut list = yt_songs(client, &format!("{name} songs")).await;
    list.retain(|t| track_artists(t).iter().any(|a| a.to_lowercase() == want));
    list.truncate(30);
    if !list.is_empty() {
        let mut c = cache.lock().unwrap();
        if c.len() > 300 {
            c.clear();
        }
        c.insert(ck, (now_secs(), list.clone()));
    }
    list
}

async fn yt_radio(client: &reqwest::Client, id: &str) -> Vec<Value> {
    if let Some((t, v)) = radio_cache().lock().unwrap().get(id) {
        if now_secs().saturating_sub(*t) < 6 * 3600 {
            return v.clone();
        }
    }
    let body = json!({
        "context": { "client": { "clientName": "WEB_REMIX", "clientVersion": "1.20250310.01.00", "hl": "en", "gl": "VN" } },
        "videoId": id,
        "playlistId": format!("RDAMVM{id}"),
        "isAudioOnly": true,
    });
    let resp = match client
        .post("https://music.youtube.com/youtubei/v1/next?prettyPrint=false")
        .header("origin", "https://music.youtube.com")
        .header("referer", "https://music.youtube.com/")
        .json(&body)
        .timeout(Duration::from_secs(12))
        .send()
        .await
    {
        Ok(r) if r.status().is_success() => r,
        _ => return Vec::new(),
    };
    let v: Value = match resp.json().await {
        Ok(v) => v,
        Err(_) => return Vec::new(),
    };
    let mut items: Vec<&Value> = Vec::new();
    yt_panel_items(&v, &mut items);
    let mut tracks: Vec<Value> = items
        .into_iter()
        .filter_map(yt_panel_track)
        .filter(|t| t.get("id").and_then(|x| x.as_str()) != Some(id))
        .collect();
    tracks.truncate(30);
    if !tracks.is_empty() {
        let mut c = radio_cache().lock().unwrap();
        if c.len() > 500 {
            c.clear();
        }
        c.insert(id.to_string(), (now_secs(), tracks.clone()));
    }
    tracks
}

// Gợi ý cho bạn = item-item KNN kiểu `implicit` (BM25 + cosine) trên các "hàng" dữ liệu:
//   - hàng của từng tài khoản (yêu thích ×3, nghe ×1 – như độ tin cậy c = 1 + α·r của ALS) và từng playlist,
//   - hàng "radio" của vài bài bạn yêu thích nhất (YouTube Music cho biết bài nào hay được nghe cùng bài đó).
// Nhờ hàng radio, tài khoản chỉ có một người dùng vẫn có dữ liệu "bài nào đi với bài nào".
// Hoà trộn gu: gộp yêu thích + lịch sử của mình và của một người bạn thành một danh sách (bài chung xếp đầu, rồi xen kẽ hai bên).
async fn blend(State(st): State<AppState>, headers: HeaderMap, Path(fid): Path<i64>) -> R<Value> {
    let u = require(&st, &headers)?;
    let db = st.db.lock().unwrap();
    let is_friend: i64 = db
        .query_row("SELECT COUNT(*) FROM friends WHERE user_id=?1 AND friend_id=?2", params![u.id, fid], |r| r.get(0))
        .unwrap_or(0);
    if is_friend == 0 {
        return Err((StatusCode::NOT_FOUND, "Người này chưa là bạn của bạn".to_string()));
    }
    let fname: String = db.query_row("SELECT display_name FROM users WHERE id=?1", [fid], |r| r.get(0)).map_err(|_| e500())?;
    let load = |uid: i64| -> Vec<(String, Value)> {
        let mut out: Vec<(String, Value)> = Vec::new();
        let mut seen: HashSet<String> = HashSet::new();
        for tbl in ["favorites", "history"] {
            let sql = format!("SELECT track_key, track_json FROM {tbl} WHERE user_id=?1 ORDER BY rowid DESC LIMIT 200");
            if let Ok(mut stmt) = db.prepare(&sql) {
                if let Ok(it) = stmt.query_map([uid], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))) {
                    for (k, js) in it.flatten() {
                        if seen.insert(k.clone()) {
                            if let Ok(v) = serde_json::from_str::<Value>(&js) {
                                out.push((k, v));
                            }
                        }
                    }
                }
            }
        }
        out
    };
    let mine = load(u.id);
    let theirs = load(fid);
    let tset: HashSet<&String> = theirs.iter().map(|x| &x.0).collect();
    let mut shared: Vec<Value> = Vec::new();
    let mut used: HashSet<String> = HashSet::new();
    for (k, v) in &mine {
        if tset.contains(k) {
            shared.push(v.clone());
            used.insert(k.clone());
        }
    }
    let a: Vec<&Value> = mine.iter().filter(|x| !used.contains(&x.0)).map(|x| &x.1).collect();
    let b: Vec<&Value> = theirs.iter().filter(|x| !used.contains(&x.0)).map(|x| &x.1).collect();
    let mut tracks: Vec<Value> = shared.clone();
    for i in 0..a.len().max(b.len()) {
        if let Some(x) = a.get(i) {
            tracks.push((*x).clone());
        }
        if let Some(x) = b.get(i) {
            tracks.push((*x).clone());
        }
    }
    tracks.truncate(100);
    Ok(Json(json!({ "friend": { "id": fid, "display_name": fname }, "shared": shared.len(), "tracks": tracks })))
}

async fn recommend(State(st): State<AppState>, headers: HeaderMap) -> R<Vec<Value>> {
    let u = require(&st, &headers)?;
    let (allow_explicit, use_proxy) = user_net_prefs(&st, u.id);
    let client = pick_client(&st, use_proxy);
    let mut rows: RecRows = HashMap::new();
    let mut meta: HashMap<String, String> = HashMap::new();
    let mut mine: HashMap<String, f64> = HashMap::new();
    let mut owned: HashSet<String> = HashSet::new();
    {
        let db = st.db.lock().unwrap();
        rec_load(&db, "SELECT user_id, user_id, track_key, track_json FROM favorites", "u", 3.0, u.id, &mut rows, &mut meta, &mut mine, &mut owned)?;
        rec_load(&db, "SELECT user_id, user_id, track_key, track_json FROM history", "u", 1.0, u.id, &mut rows, &mut meta, &mut mine, &mut owned)?;
        rec_load(
            &db,
            "SELECT t.playlist_id, p.user_id, t.track_key, t.track_json FROM playlist_tracks t JOIN playlists p ON p.id=t.playlist_id",
            "p",
            2.0,
            u.id,
            &mut rows,
            &mut meta,
            &mut mine,
            &mut owned,
        )?;
    }
    if mine.is_empty() {
        return Ok(Json(Vec::new()));
    }

    // Độ yêu thích từng NGHỆ SĨ (đã gộp alias: "Abuse" và "Abu-Se-Ken" là một) tính từ yêu thích + lịch sử + playlist của bạn.
    let mut art_aff: HashMap<String, f64> = HashMap::new();
    let mut art_name: HashMap<String, String> = HashMap::new();
    let mut art_tracks: HashMap<String, Vec<String>> = HashMap::new();
    for (k, w) in mine.iter() {
        let Some(t) = meta.get(k).and_then(|js| serde_json::from_str::<Value>(js).ok()) else { continue };
        for a in track_artists(&t) {
            let lk = a.to_lowercase();
            *art_aff.entry(lk.clone()).or_insert(0.0) += *w;
            art_tracks.entry(lk.clone()).or_default().push(k.clone());
            art_name.entry(lk).or_insert(a);
        }
    }
    let mut top_arts: Vec<(String, f64)> = art_aff.iter().map(|(k, w)| (k.clone(), *w)).collect();
    top_arts.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal).then(a.0.cmp(&b.0)));
    top_arts.truncate(6);
    let aff_max = top_arts.first().map(|x| x.1).unwrap_or(1.0).max(1e-9);

    // Chọn tối đa 6 bài "hạt giống" trong 16 bài độ tin cậy cao nhất, xoay theo giờ để gợi ý không đứng yên.
    let mut ranked_seeds: Vec<(String, f64)> = mine.iter().map(|(k, w)| (k.clone(), *w)).collect();
    ranked_seeds.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal).then(a.0.cmp(&b.0)));
    ranked_seeds.truncate(16);
    let hour = (now_secs() / 3600) as usize;
    let n_s = ranked_seeds.len();
    let picked: Vec<(String, String)> = (0..n_s.min(6))
        .filter_map(|i| {
            let (k, _) = &ranked_seeds[(i + hour) % n_s];
            let v: Value = serde_json::from_str(meta.get(k)?).ok()?;
            if v.get("src").and_then(|x| x.as_str()) != Some("yt") {
                return None;
            }
            let id = v.get("id").and_then(|x| x.as_str())?.to_string();
            Some((k.clone(), id))
        })
        .collect();
    let radios = futures_util::future::join_all(picked.iter().map(|(_, id)| yt_radio(&client, id))).await;
    for ((seed_key, id), list) in picked.iter().zip(radios.into_iter()) {
        let row = rows.entry(format!("r{}", id)).or_default();
        *row.entry(seed_key.clone()).or_insert(0.0) += 2.0;
        for (rank, t) in list.iter().enumerate() {
            let Some(k) = track_key(t) else { continue };
            *row.entry(k.clone()).or_insert(0.0) += 1.0 / (1.0 + rank as f64 / 8.0);
            meta.entry(k).or_insert_with(|| t.to_string());
        }
    }
    // Hàng "nghệ sĩ": bài của vài nghệ sĩ bạn nghe nhiều nhất (xoay theo giờ) cùng các bài bạn đã nghe của họ,
    // để bài khác của đúng nghệ sĩ đó (kể cả khi YouTube ghi tên kênh khác như "Abuse") được gợi ý.
    let n_a = top_arts.len();
    let art_pick: Vec<String> = (0..n_a.min(3)).map(|i| top_arts[(i + hour) % n_a].0.clone()).collect();
    let art_names: Vec<String> = art_pick.iter().map(|lk| art_name.get(lk).cloned().unwrap_or_else(|| lk.clone())).collect();
    let art_lists = futures_util::future::join_all(art_names.iter().map(|nm| artist_songs(&client, nm))).await;
    for (lk, list) in art_pick.iter().zip(art_lists.into_iter()) {
        let row = rows.entry(format!("a{}", lk)).or_default();
        for k in art_tracks.get(lk).into_iter().flatten() {
            *row.entry(k.clone()).or_insert(0.0) += 2.0;
        }
        for (rank, t) in list.iter().enumerate() {
            let Some(k) = track_key(t) else { continue };
            *row.entry(k.clone()).or_insert(0.0) += 1.0 / (1.0 + rank as f64 / 8.0);
            meta.entry(k).or_insert_with(|| t.to_string());
        }
    }
    if rows.len() < 2 {
        return Ok(Json(Vec::new()));
    }

    // BM25: giảm trọng số bài quá phổ biến (idf) và chuẩn hóa theo độ dài hàng
    let n = rows.len() as f64;
    let mut df: HashMap<&str, f64> = HashMap::new();
    for r in rows.values() {
        for k in r.keys() {
            *df.entry(k.as_str()).or_insert(0.0) += 1.0;
        }
    }
    let avg = (rows.values().map(|r| r.values().sum::<f64>()).sum::<f64>() / n).max(1e-9);
    let mut wrows: Vec<Vec<(&str, f64)>> = Vec::new();
    let mut norm: HashMap<&str, f64> = HashMap::new();
    let mut inv: HashMap<&str, Vec<usize>> = HashMap::new();
    for r in rows.values() {
        let len: f64 = r.values().sum();
        let mut v: Vec<(&str, f64)> = Vec::with_capacity(r.len());
        for (k, x) in r.iter() {
            let d = df.get(k.as_str()).copied().unwrap_or(1.0);
            let idf = (1.0 + (n - d + 0.5) / (d + 0.5)).ln();
            let w = *x * (BM25_K1 + 1.0) / (BM25_K1 * ((1.0 - BM25_B) + BM25_B * len / avg) + *x) * idf;
            *norm.entry(k.as_str()).or_insert(0.0) += w * w;
            v.push((k.as_str(), w));
        }
        let idx = wrows.len();
        for (k, _) in v.iter() {
            inv.entry(*k).or_default().push(idx);
        }
        wrows.push(v);
    }

    // điểm của bài j = Σ trọng số(bài i của bạn) × độ giống cosine(i, j)
    let mut score: HashMap<&str, f64> = HashMap::new();
    for (seed, sw) in mine.iter() {
        let Some(rs) = inv.get(seed.as_str()) else { continue };
        let ni = norm.get(seed.as_str()).copied().unwrap_or(0.0).sqrt();
        if ni <= 0.0 {
            continue;
        }
        for &ri in rs.iter() {
            let row = &wrows[ri];
            let wi = row.iter().find(|e| e.0 == seed.as_str()).map(|e| e.1).unwrap_or(0.0);
            for e in row.iter() {
                let (j, wj) = (e.0, e.1);
                if j == seed.as_str() || owned.contains(j) {
                    continue;
                }
                let nj = norm.get(j).copied().unwrap_or(0.0).sqrt();
                if nj <= 0.0 {
                    continue;
                }
                *score.entry(j).or_insert(0.0) += *sw * wi * wj / (ni * nj);
            }
        }
    }
    let mut ranked: Vec<(&str, f64)> = score.into_iter().collect();
    ranked.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal).then(a.0.cmp(b.0)));

    // Cộng điểm theo độ quen thuộc của nghệ sĩ: bài của nghệ sĩ bạn hay nghe (theo tên đã gộp alias) được đẩy lên.
    let top_score = ranked.first().map(|x| x.1).unwrap_or(1.0).max(1e-9);
    let mut ranked: Vec<(&str, f64)> = ranked
        .into_iter()
        .map(|(k, s)| {
            let aff = meta
                .get(k)
                .and_then(|js| serde_json::from_str::<Value>(js).ok())
                .map(|t| track_artists(&t).iter().map(|a| art_aff.get(&a.to_lowercase()).copied().unwrap_or(0.0)).fold(0.0, f64::max) / aff_max)
                .unwrap_or(0.0);
            (k, s / top_score + 0.6 * aff.min(1.0))
        })
        .collect();
    ranked.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal).then(a.0.cmp(b.0)));

    // Lọc: bỏ nội dung không phải nhạc (truyện, podcast, bản dài), tôn trọng cài đặt explicit,
    // mỗi nghệ sĩ (tên đã gộp alias) tối đa 3 bài; 3 nghệ sĩ bạn nghe nhiều nhất được tối đa 5.
    let mut per_artist: HashMap<String, u32> = HashMap::new();
    let mut out: Vec<Value> = Vec::new();
    for (k, _) in ranked {
        let Some(t) = meta.get(k).and_then(|js| serde_json::from_str::<Value>(js).ok()) else { continue };
        let title = t.get("title").and_then(|x| x.as_str()).unwrap_or("").to_lowercase();
        let dur = t.get("dur").and_then(|x| x.as_u64()).unwrap_or(0);
        if yt_is_episode(&t) || dur > 720 || ["truyện", "audiobook", "audio book", "podcast", "full album", "chương "].iter().any(|w| title.contains(w)) {
            continue;
        }
        if !allow_explicit && t.get("explicit").and_then(|x| x.as_bool()).unwrap_or(false) {
            continue;
        }
        let arts: Vec<String> = track_artists(&t).iter().map(|a| a.to_lowercase()).collect();
        let art = arts.first().cloned().unwrap_or_default();
        let fav = top_arts.iter().take(3).any(|(k, _)| arts.contains(k));
        let cap: u32 = if fav { 5 } else { 3 };
        let cnt = per_artist.entry(art).or_insert(0);
        if *cnt >= cap {
            continue;
        }
        *cnt += 1;
        out.push(t);
        if out.len() >= 40 {
            break;
        }
    }
    Ok(Json(out))
}

async fn pl_list(State(st): State<AppState>, headers: HeaderMap) -> R<Vec<Value>> {
    let u = require(&st, &headers)?;
    let db = st.db.lock().unwrap();
    let mut stmt = db
        .prepare("SELECT p.id, p.name, (SELECT COUNT(*) FROM playlist_tracks t WHERE t.playlist_id=p.id) FROM playlists p WHERE p.user_id=?1 ORDER BY p.id")
        .map_err(|_| e500())?;
    let rows = stmt
        .query_map([u.id], |r| {
            Ok(json!({ "id": r.get::<_, i64>(0)?, "name": r.get::<_, String>(1)?, "count": r.get::<_, i64>(2)? }))
        })
        .map_err(|_| e500())?;
    let mut v: Vec<Value> = rows.filter_map(|x| x.ok()).collect();
    // Ảnh bìa playlist: lấy ảnh của 4 bài đầu có ảnh (giống ảnh ghép ở trang playlist)
    for p in v.iter_mut() {
        let pid = p.get("id").and_then(|x| x.as_i64()).unwrap_or(0);
        let mut covers: Vec<String> = Vec::new();
        if let Ok(mut s2) = db.prepare("SELECT track_json FROM playlist_tracks WHERE playlist_id=?1 ORDER BY rowid LIMIT 24") {
            if let Ok(rows2) = s2.query_map([pid], |r| r.get::<_, String>(0)) {
                for js in rows2.flatten() {
                    if let Ok(tv) = serde_json::from_str::<Value>(&js) {
                        if let Some(img) = tv.get("image").and_then(|x| x.as_str()) {
                            if !img.is_empty() {
                                covers.push(img.to_string());
                                if covers.len() >= 4 {
                                    break;
                                }
                            }
                        }
                    }
                }
            }
        }
        p["covers"] = json!(covers);
    }
    Ok(Json(v))
}

#[derive(Deserialize)]
struct NameReq {
    name: String,
}

async fn pl_create(State(st): State<AppState>, headers: HeaderMap, Json(r): Json<NameReq>) -> R<Value> {
    let u = require(&st, &headers)?;
    let name: String = r.name.trim().chars().take(40).collect();
    if name.is_empty() {
        return Err(bad("Tên playlist không được trống"));
    }
    let db = st.db.lock().unwrap();
    let n: i64 = db.query_row("SELECT COUNT(*) FROM playlists WHERE user_id=?1", [u.id], |r| r.get(0)).unwrap_or(0);
    if n >= 50 {
        return Err(bad("Tối đa 50 playlist"));
    }
    db.execute("INSERT INTO playlists(user_id,name) VALUES(?1,?2)", params![u.id, name]).map_err(|_| e500())?;
    Ok(Json(json!({ "id": db.last_insert_rowid(), "name": name, "count": 0 })))
}

async fn pl_get(State(st): State<AppState>, headers: HeaderMap, Path(pid): Path<i64>) -> R<Value> {
    let u = require(&st, &headers)?;
    let db = st.db.lock().unwrap();
    let name: String = db
        .query_row("SELECT name FROM playlists WHERE id=?1 AND user_id=?2", params![pid, u.id], |r| r.get(0))
        .map_err(|_| (StatusCode::NOT_FOUND, "Không tìm thấy playlist".to_string()))?;
    let mut stmt = db
        .prepare("SELECT track_json FROM playlist_tracks WHERE playlist_id=?1 ORDER BY rowid")
        .map_err(|_| e500())?;
    let tracks = parse_rows(&mut stmt, [pid])?;
    Ok(Json(json!({ "id": pid, "name": name, "tracks": tracks })))
}

async fn pl_delete(State(st): State<AppState>, headers: HeaderMap, Path(pid): Path<i64>) -> Result<StatusCode, ApiErr> {
    let u = require(&st, &headers)?;
    let db = st.db.lock().unwrap();
    db.execute("DELETE FROM playlists WHERE id=?1 AND user_id=?2", params![pid, u.id]).map_err(|_| e500())?;
    Ok(StatusCode::OK)
}

async fn pl_add(State(st): State<AppState>, headers: HeaderMap, Path(pid): Path<i64>, Json(t): Json<Value>) -> Result<StatusCode, ApiErr> {
    let u = require(&st, &headers)?;
    let (k, s) = valid_track(&t)?;
    let db = st.db.lock().unwrap();
    if !owns(&db, pid, u.id) {
        return Err((StatusCode::NOT_FOUND, "Không tìm thấy playlist".into()));
    }
    db.execute("INSERT OR IGNORE INTO playlist_tracks(playlist_id,track_key,track_json) VALUES(?1,?2,?3)", params![pid, k, s])
        .map_err(|_| e500())?;
    Ok(StatusCode::OK)
}

async fn pl_remove(State(st): State<AppState>, headers: HeaderMap, Path((pid, key)): Path<(i64, String)>) -> Result<StatusCode, ApiErr> {
    let u = require(&st, &headers)?;
    let db = st.db.lock().unwrap();
    if !owns(&db, pid, u.id) {
        return Err((StatusCode::NOT_FOUND, "Không tìm thấy playlist".into()));
    }
    db.execute("DELETE FROM playlist_tracks WHERE playlist_id=?1 AND track_key=?2", params![pid, key]).map_err(|_| e500())?;
    Ok(StatusCode::OK)
}


// ---------- Ảnh nền (hỗ trợ GIF), icon app (admin) ----------
async fn upload_bg(State(st): State<AppState>, headers: HeaderMap, body: Bytes) -> R<User> {
    let user = require(&st, &headers)?;
    if body.len() > 10_000_000 {
        return Err(bad("Ảnh nền tối đa 10MB"));
    }
    let ext = sniff_image(&body).ok_or_else(|| bad("Chỉ nhận ảnh PNG, JPG, GIF, WEBP"))?;
    let (body, ext, video) = optimize_upload(body, ext, 960, false).await;
    let name = save_upload(&st.bg_dir, &body, ext, video).await.map_err(|_| e500())?;
    let url = format!("/backgrounds/{name}");
    let old: String;
    {
        let db = st.db.lock().unwrap();
        old = db.query_row("SELECT bg_url FROM users WHERE id=?1", [user.id], |r| r.get(0)).unwrap_or_default();
        db.execute(
            "UPDATE users SET bg_url=?1, bg=CASE WHEN bg='glass' THEN 'glass' ELSE 'custom' END WHERE id=?2",
            params![url, user.id],
        )
        .map_err(|_| e500())?;
    }
    if let Some(f) = old.strip_prefix("/backgrounds/") {
        if !f.contains('/') && !f.contains("..") {
            remove_upload_file(&st.bg_dir, f).await;
        }
    }
    let out = {
        let db = st.db.lock().unwrap();
        db.query_row(&format!("SELECT {USER_COLS} FROM users WHERE id=?1"), [user.id], row_to_user)
            .map_err(|_| e500())?
    };
    Ok(Json(out))
}

// Banner hồ sơ (PNG/JPG/GIF/WEBP ≤ 10MB). Lưu nguyên file gốc để GIF giữ chuyển động.
async fn upload_banner(State(st): State<AppState>, headers: HeaderMap, body: Bytes) -> R<User> {
    let user = require(&st, &headers)?;
    if body.len() > 10 * 1024 * 1024 {
        return Err(bad("Banner tối đa 10MB"));
    }
    let ext = sniff_image(&body).ok_or_else(|| bad("Chỉ nhận ảnh PNG, JPG, GIF, WEBP"))?;
    let (body, ext, video) = optimize_upload(body, ext, 720, true).await;
    let name = save_upload(&st.banner_dir, &body, ext, video).await.map_err(|_| e500())?;
    let url = format!("/banners/{name}");
    let old: String;
    {
        let db = st.db.lock().unwrap();
        old = db.query_row("SELECT banner FROM users WHERE id=?1", [user.id], |r| r.get(0)).unwrap_or_default();
        db.execute("UPDATE users SET banner=?1 WHERE id=?2", params![url, user.id]).map_err(|_| e500())?;
    }
    remove_banner_file(&st, &old).await;
    Ok(Json(User { banner: url, ..user }))
}

async fn remove_banner(State(st): State<AppState>, headers: HeaderMap) -> R<User> {
    let user = require(&st, &headers)?;
    {
        let db = st.db.lock().unwrap();
        db.execute("UPDATE users SET banner='' WHERE id=?1", [user.id]).map_err(|_| e500())?;
    }
    remove_banner_file(&st, &user.banner).await;
    Ok(Json(User { banner: String::new(), ..user }))
}

async fn remove_banner_file(st: &AppState, url: &str) {
    if let Some(f) = url.strip_prefix("/banners/") {
        if !f.contains('/') && !f.contains("..") {
            remove_upload_file(&st.banner_dir, f).await;
        }
    }
}

async fn admin_icon(State(st): State<AppState>, headers: HeaderMap, body: Bytes) -> Result<StatusCode, ApiErr> {
    let user = require(&st, &headers)?;
    if !user.is_admin {
        return Err((StatusCode::FORBIDDEN, "Chỉ admin mới đổi được icon".into()));
    }
    if body.len() > 2 * 1024 * 1024 || !body.starts_with(&[0x89, b'P', b'N', b'G']) {
        return Err(bad("Icon phải là file PNG, tối đa 2MB"));
    }
    tokio::fs::write(st.app_dir.join("icon.png"), &body).await.map_err(|_| e500())?;
    Ok(StatusCode::OK)
}

async fn admin_icon_reset(State(st): State<AppState>, headers: HeaderMap) -> Result<StatusCode, ApiErr> {
    let user = require(&st, &headers)?;
    if !user.is_admin {
        return Err((StatusCode::FORBIDDEN, "Chỉ admin mới đổi được icon".into()));
    }
    let _ = tokio::fs::remove_file(st.app_dir.join("icon.png")).await;
    Ok(StatusCode::OK)
}

// ---------- Thẻ chia sẻ hồ sơ ----------
// App vẽ thẻ "FANS NO." bằng canvas rồi tải PNG lên đây (POST /api/card). Server lưu data/cards/<id>.png và phục vụ:
//   GET /u/<username>      trang có thẻ Open Graph (Zalo/Messenger/Discord/Telegram hiện ảnh thẻ + tên khi dán link) và nút mở thẳng vào app Android
//   GET /api/u/<username>  thông tin công khai tối thiểu để app hiện thẻ hồ sơ khi mở link
// Chỉ người đã tạo thẻ chia sẻ mới có trang công khai; chưa tạo thì trả 404 (không lộ tên/ảnh của người chưa chia sẻ).
fn card_path(st: &AppState, id: i64) -> PathBuf {
    st.card_dir.join(format!("{id}.jpg"))
}

async fn card_version(st: &AppState, id: i64) -> Option<u64> {
    let m = tokio::fs::metadata(card_path(st, id)).await.ok()?;
    m.modified().ok()?.duration_since(UNIX_EPOCH).ok().map(|d| d.as_secs())
}

fn card_name_ok(n: &str) -> bool {
    !n.is_empty() && n.len() <= 64 && n.chars().all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-' || c == '.')
}

#[derive(Serialize)]
struct PublicUser {
    id: i64,
    username: String,
    display_name: String,
    avatar: String,
    accent: String,
    banner: String,
    banner_color: String,
    status: String,
    crop: String,
    supporter: i64,
    online: bool,
}

fn public_user_by_name(st: &AppState, username: &str) -> Option<PublicUser> {
    let db = st.db.lock().unwrap();
    db.query_row(
        "SELECT id,username,display_name,avatar,accent,banner,banner_color,status,crop,supporter,is_admin FROM users WHERE username=?1 COLLATE NOCASE",
        [username],
        |r| {
            let is_admin = r.get::<_, i64>(10)? != 0;
            Ok(PublicUser {
                id: r.get(0)?,
                username: r.get(1)?,
                display_name: r.get(2)?,
                avatar: r.get(3)?,
                accent: r.get(4)?,
                banner: r.get(5)?,
                banner_color: r.get(6)?,
                status: r.get(7)?,
                crop: r.get(8)?,
                supporter: if is_admin { 2 } else { r.get(9)? },
                online: false,
            })
        },
    )
    .ok()
}

async fn upload_card(State(st): State<AppState>, headers: HeaderMap, body: Bytes) -> Result<Json<Value>, ApiErr> {
    let user = require(&st, &headers)?;
    if body.len() > 3 * 1024 * 1024 || !body.starts_with(&[0xFF, 0xD8, 0xFF]) {
        return Err(bad("Ảnh thẻ phải là JPEG, tối đa 3MB"));
    }
    tokio::fs::write(card_path(&st, user.id), &body).await.map_err(|_| e500())?;
    let v = card_version(&st, user.id).await.unwrap_or(0);
    Ok(Json(json!({ "username": user.username, "path": format!("/u/{}", user.username), "v": v, "base": card_base_url(&headers) })))
}

// ---------- Cấp độ nghe nhạc ----------
// App báo số giây thực sự đã nghe (gộp mỗi ~60 giây). Server chặn gian lận: mỗi lần tối đa bằng thời gian trôi qua từ lần báo trước (+15s), không quá 600s.
// Cấp độ và huy hiệu tính ở giao diện từ tổng số giây (listen_secs).
#[derive(Deserialize)]
struct ListenReq {
    secs: i64,
}

async fn listen_post(State(st): State<AppState>, headers: HeaderMap, Json(req): Json<ListenReq>) -> Result<Json<Value>, ApiErr> {
    let user = require(&st, &headers)?;
    let now = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs() as i64).unwrap_or(0);
    let db = st.db.lock().unwrap();
    let (cur, last): (i64, i64) = db
        .query_row("SELECT listen_secs, listen_at FROM users WHERE id=?1", [user.id], |row| Ok((row.get(0)?, row.get(1)?)))
        .map_err(|_| e500())?;
    let cap = if last > 0 { (now - last + 15).clamp(0, 600) } else { 300 };
    let total = cur + req.secs.clamp(0, cap);
    db.execute("UPDATE users SET listen_secs=?1, listen_at=?2 WHERE id=?3", params![total, now, user.id]).map_err(|_| e500())?;
    Ok(Json(json!({ "secs": total })))
}

async fn level_get(State(st): State<AppState>, headers: HeaderMap, Path(id): Path<i64>) -> Result<Json<Value>, ApiErr> {
    require(&st, &headers)?;
    let db = st.db.lock().unwrap();
    let secs: i64 = db
        .query_row("SELECT listen_secs FROM users WHERE id=?1", [id], |row| row.get(0))
        .map_err(|_| (StatusCode::NOT_FOUND, "Không tìm thấy".to_string()))?;
    Ok(Json(json!({ "id": id, "secs": secs })))
}

// Số nghệ sĩ người dùng đã theo dõi (lấy từ dữ liệu đồng bộ "follow") — hiện trên thẻ "FANS NO.".
fn follow_count(db: &rusqlite::Connection, id: i64) -> i64 {
    db.query_row("SELECT v FROM user_sync WHERE user_id=?1 AND k='follow'", [id], |r| r.get::<_, String>(0))
        .ok()
        .and_then(|t| serde_json::from_str::<Value>(&t).ok())
        .and_then(|v| v.as_array().map(|a| a.len() as i64))
        .unwrap_or(0)
}

async fn fans_get(State(st): State<AppState>, headers: HeaderMap, Path(id): Path<i64>) -> Result<Json<Value>, ApiErr> {
    require(&st, &headers)?;
    let db = st.db.lock().unwrap();
    Ok(Json(json!({ "id": id, "n": follow_count(&db, id) })))
}

async fn card_user(State(st): State<AppState>, Path(username): Path<String>) -> Result<Json<PublicUser>, ApiErr> {
    if !card_name_ok(&username) {
        return Err((StatusCode::NOT_FOUND, "Không tìm thấy thẻ".into()));
    }
    let u = public_user_by_name(&st, &username).ok_or((StatusCode::NOT_FOUND, "Không tìm thấy thẻ".to_string()))?;
    if card_version(&st, u.id).await.is_none() {
        return Err((StatusCode::NOT_FOUND, "Không tìm thấy thẻ".into()));
    }
    Ok(Json(u))
}

fn card_esc(s: &str) -> String {
    s.replace('&', "&amp;").replace('<', "&lt;").replace('>', "&gt;").replace('"', "&quot;").replace('\'', "&#39;")
}

fn card_base_url(headers: &HeaderMap) -> String {
    if let Ok(p) = std::env::var("PUBLIC_URL") {
        let p = p.trim().trim_end_matches('/').to_string();
        if !p.is_empty() {
            return p;
        }
    }
    let host = headers
        .get("x-forwarded-host")
        .or_else(|| headers.get(header::HOST))
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.split(',').next())
        .map(|s| s.trim().chars().filter(|c| c.is_ascii_alphanumeric() || *c == '.' || *c == '-' || *c == ':').collect::<String>())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "localhost".into());
    let proto = headers
        .get("x-forwarded-proto")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.split(',').next())
        .map(|s| s.trim().to_string())
        .filter(|s| s == "http" || s == "https")
        .unwrap_or_else(|| "https".into());
    format!("{proto}://{host}")
}

const CARD_PAGE: &str = r##"<!doctype html><html lang="vi"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>@TITLE@</title>
<meta property="og:type" content="profile"><meta property="og:site_name" content="BBeat">
<meta property="og:title" content="@TITLE@"><meta property="og:description" content="@DESC@">
<meta property="og:url" content="@URL@"><meta property="og:image" content="@IMG@">
<meta property="og:image:type" content="image/jpeg"><meta property="og:image:width" content="1200"><meta property="og:image:height" content="548">
<meta name="twitter:card" content="summary_large_image"><meta name="twitter:title" content="@TITLE@"><meta name="twitter:description" content="@DESC@"><meta name="twitter:image" content="@IMG@">
<meta name="theme-color" content="#07070b">
<style>html,body{margin:0;background:#07070b}</style>
<noscript><meta http-equiv="refresh" content="0;url=@WEB@"></noscript>
</head><body>
<script>(function(){try{if(/Android/i.test(navigator.userAgent||"")&&!/[?&]web=1/.test(location.search)){location.replace("@INTENT_JS@");return;}}catch(e){}location.replace("@WEB@");})();</script>
</body></html>"##;

#[derive(Deserialize)]
struct CardQ {
    web: Option<String>,
}

async fn card_page(State(st): State<AppState>, headers: HeaderMap, Path(username): Path<String>, Query(q): Query<CardQ>) -> Response {
    let nf = || (StatusCode::NOT_FOUND, [(header::CONTENT_TYPE, "text/plain; charset=utf-8")], "Không tìm thấy thẻ BBeat này").into_response();
    if !card_name_ok(&username) {
        return nf();
    }
    let Some(u) = public_user_by_name(&st, &username) else { return nf() };
    let Some(v) = card_version(&st, u.id).await else { return nf() };
    let base = card_base_url(&headers);
    let uname = u.username.clone(); // dạng chữ hoa/thường chuẩn trong DB; chỉ gồm [A-Za-z0-9_.-]
    let page_url = format!("{base}/u/{uname}");
    let img = format!("{base}/cards/{}.jpg?v={v}", u.id);
    let title = format!("{} · Bấm để xem hồ sơ trên BBeat", u.display_name);
    let fans = { let db = st.db.lock().unwrap(); follow_count(&db, u.id) };
    let desc = format!("FANS NO. {} · Thẻ hồ sơ BBeat của {}", fans, u.display_name);
    let enc = |t: &str| t.replace(':', "%3A").replace('/', "%2F").replace('?', "%3F").replace('=', "%3D");
    let intent = format!("intent://u/{uname}#Intent;scheme=bbeat;package=vn.muse.app;S.browser_fallback_url={};end", enc(&format!("{page_url}?web=1")));
    // Người dùng Android bấm link (không phải bot xem trước, không phải ?web=1) → chuyển thẳng vào app, không hiện trang web.
    let ua = headers.get(header::USER_AGENT).and_then(|v| v.to_str().ok()).unwrap_or("").to_lowercase();
    let is_bot = ["bot", "crawler", "spider", "preview", "facebookexternalhit", "zalo", "slurp", "embed", "whatsapp", "telegram", "skype", "linkedin", "pinterest"]
        .iter()
        .any(|k| ua.contains(k));
    if ua.contains("android") && !is_bot && !q.web.as_deref().map(|w| w == "1").unwrap_or(false) {
        return (StatusCode::FOUND, [(header::LOCATION, intent.clone()), (header::CACHE_CONTROL, "no-store".to_string())]).into_response();
    }
    let html = CARD_PAGE
        .replace("@INTENT_JS@", &intent) // chuỗi này chỉ chứa ký tự an toàn trong "..." (tên đã lọc, URL đã mã hoá)
        .replace("@INTENT@", &card_esc(&intent))
        .replace("@WEB@", &card_esc(&format!("{base}/?u={uname}")))
        .replace("@TITLE@", &card_esc(&title))
        .replace("@DESC@", &card_esc(&desc))
        .replace("@NAME@", &card_esc(&u.display_name))
        .replace("@URL@", &card_esc(&page_url))
        .replace("@IMGREL@", &card_esc(&format!("/cards/{}.jpg?v={v}", u.id)))
        .replace("@IMG@", &card_esc(&img));
    ([(header::CONTENT_TYPE, "text/html; charset=utf-8"), (header::CACHE_CONTROL, "public, max-age=300")], html).into_response()
}

// Android App Links (tuỳ chọn): đặt ANDROID_CERT_SHA256 = vân tay SHA-256 của chứng chỉ ký APK (nhiều giá trị cách nhau dấu phẩy)
// thì link https://<server>/u/<tên> mở thẳng vào app, không qua trình duyệt. Không đặt thì trả 404 và nút "Mở trong BBeat" ở trang link vẫn dùng được.
async fn assetlinks() -> Response {
    let fps: Vec<String> = std::env::var("ANDROID_CERT_SHA256")
        .unwrap_or_default()
        .split(',')
        .map(|s| s.trim().to_uppercase())
        .filter(|s| !s.is_empty())
        .collect();
    if fps.is_empty() {
        return StatusCode::NOT_FOUND.into_response();
    }
    let v = json!([{ "relation": ["delegate_permission/common.handle_all_urls"],
        "target": { "namespace": "android_app", "package_name": "vn.muse.app", "sha256_cert_fingerprints": fps } }]);
    ([(header::CONTENT_TYPE, "application/json")], v.to_string()).into_response()
}

async fn serve_icon(State(st): State<AppState>) -> Response {
    match tokio::fs::read(st.app_dir.join("icon.png")).await {
        Ok(b) => ([(header::CONTENT_TYPE, "image/png"), (header::CACHE_CONTROL, "no-cache")], b).into_response(),
        Err(_) => StatusCode::NOT_FOUND.into_response(),
    }
}

// ---------- Tìm kiếm YouTube Music (InnerTube – cùng API mà InnerTune/SmartTube dùng) ----------
// Chỉ lấy danh sách bài; phát nhạc vẫn qua player chính thức của YouTube ở giao diện web.
#[derive(Deserialize)]
struct YtQ {
    q: Option<String>,
    f: Option<String>,
}

fn yt_text(v: &Value) -> String {
    v.get("runs")
        .and_then(|r| r.as_array())
        .map(|a| a.iter().filter_map(|x| x.get("text").and_then(|t| t.as_str())).collect::<String>())
        .unwrap_or_default()
}

fn yt_dur(s: &str) -> Option<u64> {
    let s = s.trim();
    if s.is_empty() || !s.contains(':') {
        return None;
    }
    let mut sec = 0u64;
    for x in s.split(':') {
        sec = sec * 60 + x.trim().parse::<u64>().ok()?;
    }
    Some(sec)
}

// Phần không phải tên nghệ sĩ: thời lượng (3:38), lượt xem/nghe, năm, nhãn Song/Video.
fn yt_junk_part(p: &str) -> bool {
    let l = p.trim().to_lowercase();
    l.is_empty()
        || yt_dur(&l).is_some()
        || l.contains("views")
        || l.contains("plays")
        || l.contains("lượt")
        || l == "song"
        || l == "video"
        || l.chars().all(|c| c.is_ascii_digit())
}

fn yt_track(item: &Value) -> Option<Value> {
    let id = item
        .pointer("/playlistItemData/videoId")
        .and_then(|x| x.as_str())
        .or_else(|| {
            item.pointer("/overlay/musicItemThumbnailOverlayRenderer/content/musicPlayButtonRenderer/playNavigationEndpoint/watchEndpoint/videoId")
                .and_then(|x| x.as_str())
        })?;
    if id.len() != 11 {
        return None;
    }
    let cols = item.get("flexColumns")?.as_array()?;
    let col = |i: usize| -> String {
        cols.get(i)
            .and_then(|c| c.pointer("/musicResponsiveListItemFlexColumnRenderer/text"))
            .map(yt_text)
            .unwrap_or_default()
    };
    let title = col(0);
    if title.is_empty() {
        return None;
    }
    // Cột 2 dạng "Nghệ sĩ • Album • 3:45" (có thể có nhãn "Song"/"Video" ở đầu).
    let parts: Vec<String> = col(1)
        .split(" \u{2022} ")
        .map(|x| x.trim().to_string())
        .filter(|x| !x.is_empty())
        .collect();
    // Tên nghệ sĩ là cột đầu tiên KHÔNG phải nhãn/thời lượng/lượt xem (trước đây video chỉ có "3:38" bị lấy nhầm làm nghệ sĩ).
    let artist = parts.iter().find(|p| !yt_junk_part(p)).cloned().unwrap_or_default();
    let dur: u64 = parts.iter().rev().find_map(|p| yt_dur(p)).unwrap_or(0);
    let image = item
        .pointer("/thumbnail/musicThumbnailRenderer/thumbnail/thumbnails")
        .and_then(|a| a.as_array())
        .and_then(|a| a.last())
        .and_then(|t| t.get("url"))
        .and_then(|u| u.as_str())
        .map(|x| x.to_string())
        .unwrap_or_else(|| format!("https://i.ytimg.com/vi/{id}/hqdefault.jpg"));
    let explicit = item
        .get("badges")
        .map(|b| b.to_string().contains("MUSIC_EXPLICIT_BADGE"))
        .unwrap_or(false);
    // Loại video do YouTube Music gắn: OMV = MV chính chủ của nghệ sĩ, ATV = bản âm thanh chính thức, UGC = video người dùng tự đăng (fan, remix, cover...).
    let vt = {
        let s = item.to_string();
        let key = "\"musicVideoType\":\"MUSIC_VIDEO_TYPE_";
        s.find(key)
            .map(|i| s[i + key.len()..].chars().take_while(|c| c.is_ascii_uppercase() || *c == '_').collect::<String>())
            .unwrap_or_default()
    };
    Some(json!({ "src": "yt", "id": id, "title": title, "artist": artist, "image": image, "url": "", "dur": dur, "explicit": explicit, "vt": vt }))
}

// Mục không phải bài hát trong kết quả tìm kiếm: album, nghệ sĩ, danh sách phát, podcast.
fn yt_entity(item: &Value) -> Option<Value> {
    let be = item.pointer("/navigationEndpoint/browseEndpoint")?;
    let id = be.get("browseId")?.as_str()?;
    let pt = be
        .pointer("/browseEndpointContextSupportedConfigs/browseEndpointContextMusicConfig/pageType")
        .and_then(|x| x.as_str())
        .unwrap_or("");
    let kind = match pt {
        "MUSIC_PAGE_TYPE_ALBUM" => "album",
        "MUSIC_PAGE_TYPE_ARTIST" | "MUSIC_PAGE_TYPE_USER_CHANNEL" => "artist",
        "MUSIC_PAGE_TYPE_PLAYLIST" => "playlist",
        "MUSIC_PAGE_TYPE_PODCAST_SHOW_DETAIL_PAGE" => "podcast",
        _ => return None,
    };
    let cols = item.get("flexColumns")?.as_array()?;
    let col = |i: usize| -> String {
        cols.get(i)
            .and_then(|c| c.pointer("/musicResponsiveListItemFlexColumnRenderer/text"))
            .map(yt_text)
            .unwrap_or_default()
    };
    let title = col(0);
    if title.is_empty() {
        return None;
    }
    let image = item
        .pointer("/thumbnail/musicThumbnailRenderer/thumbnail/thumbnails")
        .and_then(|a| a.as_array())
        .and_then(|a| a.last())
        .and_then(|t| t.get("url"))
        .and_then(|u| u.as_str())
        .unwrap_or("")
        .to_string();
    Some(json!({ "kind": kind, "id": id, "title": title, "sub": col(1), "image": image }))
}

// Tập cả bài hát lẫn mục khác từ một kết quả tìm kiếm (giữ nguyên thứ tự YouTube Music trả về).
fn yt_collect_all(v: &Value, tracks: &mut Vec<Value>, ents: &mut Vec<Value>) {
    match v {
        Value::Object(m) => {
            if let Some(item) = m.get("musicResponsiveListItemRenderer") {
                if let Some(t) = yt_track(item) {
                    tracks.push(t);
                } else if let Some(e) = yt_entity(item) {
                    ents.push(e);
                }
            } else {
                for x in m.values() {
                    yt_collect_all(x, tracks, ents);
                }
            }
        }
        Value::Array(a) => {
            for x in a {
                yt_collect_all(x, tracks, ents);
            }
        }
        _ => {}
    }
}

// Tập tập podcast (dạng musicMultiRowListItemRenderer) ở trang chương trình podcast.
fn yt_collect_episodes(v: &Value, out: &mut Vec<Value>) {
    match v {
        Value::Object(m) => {
            if let Some(it) = m.get("musicMultiRowListItemRenderer") {
                let id = it.pointer("/onTap/watchEndpoint/videoId").and_then(|x| x.as_str()).unwrap_or("");
                if id.len() == 11 {
                    let title = it.get("title").map(yt_text).unwrap_or_default();
                    let artist = it.get("subtitle").map(yt_text).unwrap_or_default();
                    let image = it
                        .pointer("/thumbnail/musicThumbnailRenderer/thumbnail/thumbnails")
                        .and_then(|a| a.as_array())
                        .and_then(|a| a.last())
                        .and_then(|t| t.get("url"))
                        .and_then(|u| u.as_str())
                        .map(|x| x.to_string())
                        .unwrap_or_else(|| format!("https://i.ytimg.com/vi/{id}/hqdefault.jpg"));
                    out.push(json!({ "src": "yt", "id": id, "title": title, "artist": artist, "image": image, "url": "", "dur": 0, "explicit": false, "vt": "PODCAST_EPISODE" }));
                }
            } else {
                for x in m.values() {
                    yt_collect_episodes(x, out);
                }
            }
        }
        Value::Array(a) => {
            for x in a {
                yt_collect_episodes(x, out);
            }
        }
        _ => {}
    }
}

// Tham số bộ lọc của YouTube Music (giống ytmusicapi). "all" = không lọc.
fn yt_filter_params(f: &str) -> Option<&'static str> {
    match f {
        "songs" => Some("EgWKAQIIAWoMEA4QChADEAQQCRAF"),
        "videos" => Some("EgWKAQIQAWoMEA4QChADEAQQCRAF"),
        "albums" => Some("EgWKAQIYAWoMEA4QChADEAQQCRAF"),
        "artists" => Some("EgWKAQIgAWoMEA4QChADEAQQCRAF"),
        "podcasts" => Some("EgWKAQJQAWoMEA4QChADEAQQCRAF"),
        "playlists" => Some("EgeKAQQoAEABagwQDhAKEAMQBBAJEAU="),
        "featured" => Some("EgeKAQQoADgBagwQDhAKEAMQBBAJEAU="),
        _ => None,
    }
}

fn yt_collect(v: &Value, out: &mut Vec<Value>) {
    match v {
        Value::Object(m) => {
            if let Some(item) = m.get("musicResponsiveListItemRenderer") {
                if let Some(t) = yt_track(item) {
                    out.push(t);
                }
            } else {
                for x in m.values() {
                    yt_collect(x, out);
                }
            }
        }
        Value::Array(a) => {
            for x in a {
                yt_collect(x, out);
            }
        }
        _ => {}
    }
}

// ---------- Lọc kết quả: bỏ podcast / nội dung không phải nhạc, bỏ bài không liên quan tới từ khoá ----------
// Tập podcast do YouTube Music gắn nhãn "Episode" (musicVideoType = PODCAST_EPISODE).
fn yt_is_episode(t: &Value) -> bool {
    t.get("vt").and_then(|x| x.as_str()) == Some("PODCAST_EPISODE")
        || t.get("artist").and_then(|x| x.as_str()).map(|a| a.trim().eq_ignore_ascii_case("episode")).unwrap_or(false)
}

// Không phải nhạc: tập podcast, truyện / sách nói, video quá dài (> 20 phút).
// MV chính chủ (OMV) và bản âm thanh chính thức (ATV) luôn được giữ.
// Video do người khác đăng (UGC: bản live, cover, MV fan làm, playlist True LIVE...) vẫn được giữ, vì nhiều người muốn xem bản đó.
fn yt_is_non_music(t: &Value) -> bool {
    if yt_is_episode(t) {
        return true;
    }
    let vt = t.get("vt").and_then(|x| x.as_str()).unwrap_or("");
    if vt == "OMV" || vt == "ATV" || vt == "OFFICIAL_SOURCE_MUSIC" {
        return false;
    }
    let title = t.get("title").and_then(|x| x.as_str()).unwrap_or("").to_lowercase();
    let dur = t.get("dur").and_then(|x| x.as_u64()).unwrap_or(0);
    dur > 1200 || ["podcast", "audiobook", "audio book", "truyện", "chương "].iter().any(|w| title.contains(w))
}

const YT_STOP: &[&str] = &[
    "the", "and", "feat", "featuring", "with", "official", "video", "music", "lyrics", "lyric", "audio", "live", "version", "remix", "cover",
    "instrumental", "full", "song", "songs", "topic", "hdr", "new", "from", "you", "your", "vol", "ver", "inst", "vocal", "edit", "short",
];

// Tách từ khoá: chữ Latin >= 3 ký tự, chữ Nhật/Hàn/Trung... >= 2 ký tự; bỏ từ phổ biến và số thuần.
fn yt_tokens(s: &str) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for w in s.to_lowercase().split(|c: char| !c.is_alphanumeric()) {
        let n = w.chars().count();
        let wide = !w.is_ascii();
        if (wide && n >= 2) || (!wide && n >= 3) {
            if !YT_STOP.iter().any(|x| *x == w) && !w.chars().all(|c| c.is_ascii_digit()) {
                out.push(w.to_string());
            }
        }
    }
    out
}

// Chữ Latin: khớp nguyên từ (tránh "ster" khớp nhầm "monster"); chữ CJK: khớp chuỗi con (vì không có dấu cách giữa các từ).
fn yt_has_tok(hay: &str, toks: &HashSet<String>, tok: &str) -> bool {
    if tok.is_ascii() {
        toks.contains(tok)
    } else {
        hay.contains(tok)
    }
}

// Bỏ bài không liên quan tới từ khoá tìm kiếm. YouTube Music hay trả thêm vài bài lạc đề ở cuối (ví dụ tìm "Gakumas" ra Drake, M83).
// Không so chữ cứng với từ khoá (vì "Gakumas" = "学マス" = "学園アイドルマスター" = "Hatsuboshi Gakuen"), mà học từ chính kết quả:
// lấy 8 bài đứng đầu (YouTube xếp theo độ khớp) làm mốc, những từ xuất hiện ở >= 2 bài mốc là "từ chủ đề".
// Các bài phía sau chỉ được giữ nếu có từ chủ đề (hoặc khớp trực tiếp từ khoá). Bài do người khác đăng nhưng đúng chủ đề (ví dụ bài Roar) vẫn được giữ.
fn yt_relevance_filter(tracks: Vec<Value>, query: &str) -> Vec<Value> {
    const SEED: usize = 8;
    if tracks.len() <= SEED {
        return tracks;
    }
    let hays: Vec<String> = tracks
        .iter()
        .map(|t| {
            format!(
                "{} {}",
                t.get("title").and_then(|x| x.as_str()).unwrap_or(""),
                t.get("artist").and_then(|x| x.as_str()).unwrap_or("")
            )
            .to_lowercase()
        })
        .collect();
    let toks: Vec<HashSet<String>> = hays.iter().map(|h| yt_tokens(h).into_iter().collect()).collect();
    let q = query.trim().to_lowercase();
    let qt = yt_tokens(&q);
    let direct: Vec<bool> = (0..tracks.len())
        .map(|i| {
            let whole = q.chars().count() >= 3 && hays[i].contains(q.as_str());
            let hit = qt.iter().filter(|w| yt_has_tok(&hays[i], &toks[i], w.as_str())).count();
            whole || (!qt.is_empty() && hit * 2 >= qt.len())
        })
        .collect();
    let mut cnt: HashMap<String, u32> = HashMap::new();
    for i in 0..tracks.len() {
        if i < SEED || direct[i] {
            for w in toks[i].iter() {
                *cnt.entry(w.clone()).or_insert(0) += 1;
            }
        }
    }
    let mut anchors: Vec<String> = cnt.into_iter().filter(|(_, c)| *c >= 2).map(|(w, _)| w).collect();
    for w in qt {
        if !anchors.contains(&w) {
            anchors.push(w);
        }
    }
    if anchors.is_empty() {
        return tracks;
    }
    let kept: Vec<Value> = tracks
        .iter()
        .enumerate()
        .filter(|(i, _)| *i < SEED || direct[*i] || anchors.iter().any(|a| yt_has_tok(&hays[*i], &toks[*i], a.as_str())))
        .map(|(_, t)| t.clone())
        .collect();
    if kept.len() < 3 {
        tracks
    } else {
        kept
    }
}

// Trả về (cho phép nội dung nhạy cảm, dùng proxy) theo cài đặt của người dùng.
fn user_net_prefs(st: &AppState, uid: i64) -> (bool, bool) {
    let db = st.db.lock().unwrap();
    let s = load_settings(&db, uid);
    let g = |k: &str| s.get(k).and_then(|v| v.as_bool()).unwrap_or(false);
    (g("explicit"), g("proxy"))
}
fn pick_client(st: &AppState, use_proxy: bool) -> reqwest::Client {
    if use_proxy {
        if let Some(c) = &st.http_proxy {
            return c.clone();
        }
    }
    st.http.clone()
}

async fn yt_search(State(st): State<AppState>, headers: HeaderMap, Query(q): Query<YtQ>) -> R<Value> {
    let dbg_t0 = std::time::Instant::now();
    let dbg_src = if st.gateway.is_some() { "gateway" } else { "server" };
    // Không bắt buộc đăng nhập: khách (không có token) vẫn tìm nhạc được với cài đặt mặc định.
    let (allow_explicit, use_proxy) = net_prefs(&st, &headers).await;
    let dbg_prefs = dbg_t0.elapsed().as_millis();
    let client = pick_client(&st, use_proxy);
    let text: String = q.q.unwrap_or_default().trim().chars().take(100).collect();
    if text.is_empty() {
        return Err(bad("Thiếu từ khoá tìm kiếm"));
    }
    let fail = |m: &str| -> ApiErr { (StatusCode::BAD_GATEWAY, m.to_string()) };
    // Cache 10 phút: trang chủ mở lại là có ngay, không phải chờ YouTube Music.
    // Có tham số f (all/songs/videos/albums/artists/playlists/featured/podcasts) thì trả {tracks, entities}; không có thì trả mảng bài hát như cũ.
    let filt: Option<String> = q.f.as_deref().map(|x| x.trim().to_lowercase()).filter(|x| !x.is_empty());
    let ckey = format!("{}|{}|{}|{}", text.to_lowercase(), allow_explicit as u8, use_proxy as u8, filt.clone().unwrap_or_default());
    if let Some((t, v)) = search_cache().lock().unwrap().get(&ckey) {
        if now_secs().saturating_sub(*t) < 600 {
            dbg_log(&format!("[SEARCH] query={text:?} f={} source={dbg_src} cache=HIT(RAM) prefs={dbg_prefs}ms total={}ms", filt.clone().unwrap_or_default(), dbg_t0.elapsed().as_millis()));
            return Ok(Json(v.clone()));
        }
    }
    let mut body = json!({
        "context": { "client": { "clientName": "WEB_REMIX", "clientVersion": "1.20250310.01.00", "hl": "en", "gl": "VN" } },
        "query": text,
    });
    match filt.as_deref() {
        // Không có f: bộ lọc "Songs" như trước đây
        None => body["params"] = json!("EgWKAQIIAWoKEAkQBRAKEAMQBA=="),
        Some(f) => {
            if let Some(p) = yt_filter_params(f) {
                body["params"] = json!(p);
            }
        }
    }
    let dbg_f = filt.clone().unwrap_or_default();
    let dbg_t1 = std::time::Instant::now();
    let resp = client
        .post("https://music.youtube.com/youtubei/v1/search?prettyPrint=false")
        .header("origin", "https://music.youtube.com")
        .header("referer", "https://music.youtube.com/")
        .json(&body)
        .send()
        .await
        .map_err(|e| {
            dbg_log(&format!("[SEARCH] query={text:?} f={dbg_f} source={dbg_src} cache=MISS LỖI_KẾT_NỐI prefs={dbg_prefs}ms after={}ms err={e}", dbg_t1.elapsed().as_millis()));
            fail("Không kết nối được YouTube Music")
        })?;
    let dbg_hdr = dbg_t1.elapsed().as_millis();
    if !resp.status().is_success() {
        dbg_log(&format!("[SEARCH] query={text:?} f={dbg_f} source={dbg_src} cache=MISS YOUTUBE_TRẢ_LỖI status={} prefs={dbg_prefs}ms youtube_headers={dbg_hdr}ms", resp.status().as_u16()));
        return Err(fail("YouTube Music trả về lỗi"));
    }
    let v: Value = resp.json().await.map_err(|e| {
        dbg_log(&format!("[SEARCH] query={text:?} f={dbg_f} source={dbg_src} cache=MISS LỖI_ĐỌC_JSON prefs={dbg_prefs}ms youtube_headers={dbg_hdr}ms err={e}"));
        fail("YouTube Music trả dữ liệu lạ")
    })?;
    let dbg_body = dbg_t1.elapsed().as_millis() - dbg_hdr;
    let dbg_t2 = std::time::Instant::now();
    let mut out: Vec<Value> = Vec::new();
    let mut ents: Vec<Value> = Vec::new();
    if filt.is_some() {
        yt_collect_all(&v, &mut out, &mut ents);
    } else {
        yt_collect(&v, &mut out);
    }
    let dbg_raw = out.len();
    let dbg_parse = dbg_t2.elapsed().as_millis();
    let dbg_t3 = std::time::Instant::now();
    if !allow_explicit {
        out.retain(|t| !t.get("explicit").and_then(|x| x.as_bool()).unwrap_or(false));
    }
    // Bỏ podcast / nội dung không phải nhạc (trừ khi người dùng chủ động chọn bộ lọc Podcasts).
    if filt.as_deref() != Some("podcasts") {
        out.retain(|t| !yt_is_non_music(t));
        // Bỏ bài lạc đề khi người dùng tự tìm (tab Tất cả / Bài hát / Video).
        if matches!(filt.as_deref(), Some("all") | Some("songs") | Some("videos")) {
            out = yt_relevance_filter(out, &text);
        }
    }
    out.truncate(30);
    ents.truncate(30);
    dbg_log(&format!(
        "[SEARCH] query={text:?} f={dbg_f} source={dbg_src} cache=MISS prefs={dbg_prefs}ms youtube_headers={dbg_hdr}ms youtube_body={dbg_body}ms json_parse={dbg_parse}ms filter={}ms total={}ms raw_tracks={dbg_raw} kept_tracks={} entities={}{}",
        dbg_t3.elapsed().as_millis(),
        dbg_t0.elapsed().as_millis(),
        out.len(),
        ents.len(),
        if dbg_raw > 0 && out.is_empty() { "  <<< LỌC MẤT HẾT KẾT QUẢ" } else { "" }
    ));
    let val = if filt.is_some() {
        json!({ "tracks": out, "entities": ents })
    } else {
        Value::Array(out)
    };
    let non_empty = match &val {
        Value::Array(a) => !a.is_empty(),
        Value::Object(o) => o.values().any(|x| x.as_array().map(|a| !a.is_empty()).unwrap_or(false)),
        _ => false,
    };
    if non_empty {
        let mut c = search_cache().lock().unwrap();
        if c.len() > 300 {
            c.clear();
        }
        c.insert(ckey, (now_secs(), val.clone()));
    }
    Ok(Json(val))
}

static SEARCH_CACHE: std::sync::OnceLock<Mutex<HashMap<String, (u64, Value)>>> = std::sync::OnceLock::new();
fn search_cache() -> &'static Mutex<HashMap<String, (u64, Value)>> {
    SEARCH_CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

// ---------- Nghệ sĩ liên quan (mục "Fans might also like" trên trang nghệ sĩ của YouTube Music) ----------
// Dùng cho kệ "Giống X" ở trang chủ: thay vì tìm chữ "nhạc giống X", lấy đúng danh sách nghệ sĩ mà YouTube Music cho là gần với X,
// rồi lấy vài bài của từng người và xen kẽ lại để một kệ có nhiều giọng khác nhau. Ý tưởng: get_artist(...)["related"] của thư viện ytmusicapi.
#[derive(Deserialize)]
struct YtSimQ {
    artist: Option<String>,
    genre: Option<String>,
}

static SIMILAR_CACHE: std::sync::OnceLock<Mutex<HashMap<String, (u64, Value)>>> = std::sync::OnceLock::new();
fn similar_cache() -> &'static Mutex<HashMap<String, (u64, Value)>> {
    SIMILAR_CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

// Thể loại cho kệ "Giống X": (từ khoá để tìm kiếm, dấu hiệu trong tên bài/nghệ sĩ để ưu tiên). Chỉ liệt kê vài thể loại hay gặp.
fn genre_spec(g: &str) -> Option<(&'static str, &'static [&'static str])> {
    match g.trim().to_lowercase().as_str() {
        "edm" | "edm / remix" | "electronic" | "electro" => Some((
            "edm",
            &["edm", "remix", "house", "bass", "dj ", " dj", "club", "electro", "trance", "dubstep", "dnb", "drum and bass", "hardstyle", "future", "vip", "festival"],
        )),
        "lofi" | "lofi / chill" | "chill" => Some(("lofi", &["lofi", "lo-fi", "chill", "beats", "study"])),
        "rap" | "hiphop" | "hip-hop" | "rap / hip-hop" => Some(("hip hop", &["rap", "hip hop", "hip-hop", "trap", "drill", "cypher"])),
        _ => None,
    }
}
fn genre_match(t: &Value, words: &[&str]) -> bool {
    let txt = format!(
        "{} {}",
        t.get("title").and_then(|x| x.as_str()).unwrap_or(""),
        t.get("artist").and_then(|x| x.as_str()).unwrap_or("")
    )
    .to_lowercase();
    words.iter().any(|w| txt.contains(w))
}

// Chuẩn hoá tên để so khớp: bỏ dấu cách / ký tự đặc biệt, chữ thường ("Hatsuboshi Gakuen" == "hatsuboshi-gakuen").
fn norm_name(s: &str) -> String {
    s.chars().filter(|c| c.is_alphanumeric()).flat_map(|c| c.to_lowercase()).collect()
}
// Tách chuỗi nghệ sĩ của một bài ("A, B & C feat. D") thành các tên đã chuẩn hoá.
fn credit_names(s: &str) -> Vec<String> {
    let t = s.to_lowercase().replace(" feat. ", ",").replace(" feat ", ",").replace(" ft. ", ",").replace(" ft ", ",").replace(" x ", ",");
    t.split(|c: char| matches!(c, ',' | '&' | ';' | '/' | '、' | '・')).map(norm_name).filter(|x| !x.is_empty()).collect()
}
// Bài này có phải của nghệ sĩ gốc (hoặc người cùng nhóm/franchise với họ) không?
fn is_blocked_track(t: &Value, block: &HashSet<String>) -> bool {
    let a = t.get("artist").and_then(|x| x.as_str()).unwrap_or("");
    credit_names(a).iter().any(|n| block.contains(n) || block.iter().any(|b| b.chars().count() >= 4 && n.contains(b.as_str())))
}

fn yt_ctx() -> Value {
    json!({ "client": { "clientName": "WEB_REMIX", "clientVersion": "1.20250310.01.00", "hl": "en", "gl": "VN" } })
}

// POST tới một endpoint InnerTube (search / browse ...), trả None nếu lỗi mạng hoặc YouTube báo lỗi.
async fn yt_post(client: &reqwest::Client, ep: &str, body: Value) -> Option<Value> {
    let resp = client
        .post(format!("https://music.youtube.com/youtubei/v1/{ep}?prettyPrint=false"))
        .header("origin", "https://music.youtube.com")
        .header("referer", "https://music.youtube.com/")
        .json(&body)
        .timeout(Duration::from_secs(12))
        .send()
        .await
        .ok()?;
    if !resp.status().is_success() {
        return None;
    }
    resp.json::<Value>().await.ok()
}

// Tìm bài hát (bộ lọc "Songs") theo từ khoá.
async fn yt_songs(client: &reqwest::Client, query: &str) -> Vec<Value> {
    let body = json!({ "context": yt_ctx(), "query": query, "params": "EgWKAQIIAWoKEAkQBRAKEAMQBA==" });
    let mut out: Vec<Value> = Vec::new();
    if let Some(v) = yt_post(client, "search", body).await {
        yt_collect(&v, &mut out);
    }
    out.retain(|t| !yt_is_non_music(t));
    out
}

// Các thẻ nghệ sĩ (musicTwoRowItemRenderer có pageType = ARTIST) trên trang nghệ sĩ: chỉ mục "Fans might also like" có loại thẻ này.
fn yt_related_artists(v: &Value, out: &mut Vec<(String, String)>) {
    match v {
        Value::Object(m) => {
            if let Some(r) = m.get("musicTwoRowItemRenderer") {
                if let Some(be) = r.pointer("/navigationEndpoint/browseEndpoint") {
                    let pt = be
                        .pointer("/browseEndpointContextSupportedConfigs/browseEndpointContextMusicConfig/pageType")
                        .and_then(|x| x.as_str())
                        .unwrap_or("");
                    let id = be.get("browseId").and_then(|x| x.as_str()).unwrap_or("");
                    let name = r.get("title").map(yt_text).unwrap_or_default();
                    if pt == "MUSIC_PAGE_TYPE_ARTIST" && !id.is_empty() && !name.is_empty() {
                        out.push((name, id.to_string()));
                    }
                }
            } else {
                for x in m.values() {
                    yt_related_artists(x, out);
                }
            }
        }
        Value::Array(a) => {
            for x in a {
                yt_related_artists(x, out);
            }
        }
        _ => {}
    }
}

// GET /api/yt/similar?artist=Tên → { artist, related: [{name,id}], tracks: [...] }  (tracks xen kẽ giữa các nghệ sĩ liên quan)
async fn yt_similar(State(st): State<AppState>, headers: HeaderMap, Query(q): Query<YtSimQ>) -> R<Value> {
    let (allow_explicit, use_proxy) = net_prefs(&st, &headers).await;
    let client = pick_client(&st, use_proxy);
    let name: String = q.artist.unwrap_or_default().trim().chars().take(80).collect();
    if name.is_empty() {
        return Err(bad("Thiếu tên nghệ sĩ"));
    }
    let lname = name.to_lowercase();
    let genre_in: String = q.genre.unwrap_or_default().trim().chars().take(30).collect();
    let ckey = format!("{}|{}|{}|{}", lname, allow_explicit as u8, use_proxy as u8, genre_in.to_lowercase());
    if let Some((t, v)) = similar_cache().lock().unwrap().get(&ckey) {
        if now_secs().saturating_sub(*t) < 6 * 3600 {
            return Ok(Json(v.clone()));
        }
    }
    let empty = json!({ "artist": name, "related": [], "tracks": [] });

    // 1) Tìm nghệ sĩ: ưu tiên tên trùng khớp, không có thì lấy kết quả nghệ sĩ đầu tiên.
    let Some(sv) = yt_post(&client, "search", json!({ "context": yt_ctx(), "query": name, "params": yt_filter_params("artists").unwrap_or("") })).await else {
        return Err((StatusCode::BAD_GATEWAY, "Không kết nối được YouTube Music".to_string()));
    };
    let mut tr0: Vec<Value> = Vec::new();
    let mut ents: Vec<Value> = Vec::new();
    yt_collect_all(&sv, &mut tr0, &mut ents);
    let is_artist = |e: &&Value| e.get("kind").and_then(|x| x.as_str()) == Some("artist");
    let exact = ents
        .iter()
        .filter(is_artist)
        .find(|e| e.get("title").and_then(|x| x.as_str()).map(|t| t.to_lowercase() == lname).unwrap_or(false));
    // Chỉ nhận trang nghệ sĩ có tên thật sự khớp (trùng hoặc chứa nhau). Trước đây lấy kết quả đầu tiên nên "Ume Hanami" (không có trang riêng)
    // bị nhận nhầm thành một nghệ sĩ lạ rồi gợi ý lung tung.
    let nname = norm_name(&name);
    let near = ents.iter().filter(is_artist).find(|e| {
        let t = norm_name(e.get("title").and_then(|x| x.as_str()).unwrap_or(""));
        !t.is_empty() && !nname.is_empty() && (t.contains(&nname) || nname.contains(&t))
    });
    let seed_ent = exact.or(near);
    let aid: String = match seed_ent.and_then(|e| e.get("id")).and_then(|x| x.as_str()) {
        Some(s) => s.to_string(),
        None => {
            // Không có trang nghệ sĩ riêng (vd. chỉ là một giọng trong franchise). Có thể loại thì vẫn cho kệ đúng thể loại đó; không thì ẩn kệ.
            if let Some((kw, _)) = genre_spec(&genre_in) {
                let mut extra = yt_songs(&client, &format!("{kw} hits")).await;
                extra.retain(|t| t.get("dur").and_then(|x| x.as_u64()).unwrap_or(0) <= 720);
                if !allow_explicit {
                    extra.retain(|t| !t.get("explicit").and_then(|x| x.as_bool()).unwrap_or(false));
                }
                extra.truncate(20);
                return Ok(Json(json!({ "artist": name, "related": [], "tracks": extra })));
            }
            return Ok(Json(empty));
        }
    };
    let seed_title: String = seed_ent.and_then(|e| e.get("title")).and_then(|x| x.as_str()).unwrap_or("").to_string();

    // "Nhóm loại trừ": nghệ sĩ gốc + tất cả người từng đứng chung tên với họ trên bài hát của họ.
    // Vd. Hatsuboshi Gakuen là một franchise (Gakuen Idolmaster) do nhiều nhân vật / ca sĩ hát: Saki Hanami, Ume Hanami...
    // đều là "người nhà", không phải "giống" → bỏ hết khỏi kệ.
    let mut block: HashSet<String> = HashSet::new();
    block.insert(norm_name(&name));
    if !seed_title.is_empty() {
        block.insert(norm_name(&seed_title));
    }
    block.retain(|x| !x.is_empty());

    // 2) Trang nghệ sĩ → danh sách nghệ sĩ liên quan (song song với việc lấy bài của chính nghệ sĩ gốc để dò "người nhà").
    let (bv_opt, seed_songs) = futures_util::future::join(
        yt_post(&client, "browse", json!({ "context": yt_ctx(), "browseId": aid })),
        yt_songs(&client, &name),
    )
    .await;
    let Some(bv) = bv_opt else {
        return Ok(Json(empty));
    };
    // Thể loại: ưu tiên tham số genre do app gửi; không có thì tự đoán từ chính các bài của nghệ sĩ gốc (đa số mang dấu hiệu EDM → EDM).
    let own: Vec<&Value> = seed_songs.iter().filter(|t| is_blocked_track(t, &block)).collect();
    let mut spec = genre_spec(&genre_in);
    if spec.is_none() && own.len() >= 3 {
        if let Some(e) = genre_spec("edm") {
            if own.iter().filter(|t| genre_match(t, e.1)).count() * 100 >= own.len() * 40 {
                spec = Some(e);
            }
        }
    }
    for t in seed_songs.iter() {
        if is_blocked_track(t, &block) {
            let a = t.get("artist").and_then(|x| x.as_str()).unwrap_or("");
            for n in credit_names(a) {
                block.insert(n);
            }
        }
    }
    let mut rel: Vec<(String, String)> = Vec::new();
    yt_related_artists(&bv, &mut rel);
    let mut seen_ids: HashSet<String> = HashSet::new();
    rel.retain(|(n, id)| id != &aid && n.to_lowercase() != lname && !block.contains(&norm_name(n)) && seen_ids.insert(id.clone()));
    rel.truncate(7);
    if rel.is_empty() && spec.is_none() {
        return Ok(Json(empty));
    }

    // 3) Vài bài của từng nghệ sĩ liên quan (tìm song song), rồi xen kẽ.
    let lists = futures_util::future::join_all(rel.iter().map(|(n, _)| {
        let qs = match spec {
            Some((kw, _)) => format!("{n} {kw}"),
            None => n.clone(),
        };
        let c = client.clone();
        async move { yt_songs(&c, &qs).await }
    }))
    .await;
    // Một tên cùng đứng trong bài của nhiều nghệ sĩ liên quan khác nhau = nhãn / franchise (vd. Hatsuboshi Gakuen) chứ không phải một giọng riêng → coi là "người nhà", bỏ hết.
    {
        let mut cnt: HashMap<String, usize> = HashMap::new();
        for ((n, _), l) in rel.iter().zip(lists.iter()) {
            let own_n = norm_name(n);
            let mut names: HashSet<String> = HashSet::new();
            for t in l.iter() {
                let a = t.get("artist").and_then(|x| x.as_str()).unwrap_or("");
                for c in credit_names(a) {
                    if c != own_n && !own_n.contains(&c) && !c.contains(&own_n) {
                        names.insert(c);
                    }
                }
            }
            for c in names {
                *cnt.entry(c).or_insert(0) += 1;
            }
        }
        let need = if rel.len() >= 3 { std::cmp::max(2, rel.len() / 2) } else { usize::MAX };
        for (c, k) in cnt {
            if k >= need {
                block.insert(c);
            }
        }
    }
    let mut per: Vec<Vec<Value>> = Vec::new();
    for ((n, _), l) in rel.iter().zip(lists.into_iter()) {
        let ln = n.to_lowercase();
        // Nghệ sĩ "liên quan" mà phần lớn bài tìm được lại ghi tên nghệ sĩ gốc / người nhà → cũng là người nhà, bỏ cả người đó.
        let hits = l.iter().filter(|t| is_blocked_track(t, &block)).count();
        if !l.is_empty() && hits * 2 >= l.len() {
            per.push(Vec::new());
            continue;
        }
        let l: Vec<Value> = l.into_iter().filter(|t| !is_blocked_track(t, &block)).collect();
        let mut mine: Vec<Value> = l
            .iter()
            .filter(|t| t.get("artist").and_then(|x| x.as_str()).map(|a| a.to_lowercase().contains(&ln)).unwrap_or(false))
            .cloned()
            .collect();
        if mine.len() < 2 {
            mine = l;
        }
        mine.retain(|t| t.get("dur").and_then(|x| x.as_u64()).unwrap_or(0) <= 720);
        if !allow_explicit {
            mine.retain(|t| !t.get("explicit").and_then(|x| x.as_bool()).unwrap_or(false));
        }
        if let Some((_, words)) = spec {
            mine.sort_by_key(|t| !genre_match(t, words));   // sort ổn định: bài đúng thể loại lên trước
        }
        mine.truncate(5);
        per.push(mine);
    }
    let mut tracks: Vec<Value> = Vec::new();
    let mut seen_t: HashSet<String> = HashSet::new();
    for i in 0..5 {
        for l in per.iter() {
            if let Some(t) = l.get(i) {
                let id = t.get("id").and_then(|x| x.as_str()).unwrap_or("").to_string();
                if !id.is_empty() && seen_t.insert(id) {
                    tracks.push(t.clone());
                }
            }
        }
    }
    // Có thể loại mà vẫn thiếu bài (vì nghệ sĩ liên quan phần lớn là "người nhà" đã bị loại) → bổ sung bằng bài hot của đúng thể loại đó.
    if tracks.len() < 6 {
        if let Some((kw, _)) = spec {
            let extra = yt_songs(&client, &format!("{kw} hits")).await;
            for t in extra {
                if is_blocked_track(&t, &block) || t.get("dur").and_then(|x| x.as_u64()).unwrap_or(0) > 720 {
                    continue;
                }
                if !allow_explicit && t.get("explicit").and_then(|x| x.as_bool()).unwrap_or(false) {
                    continue;
                }
                let id = t.get("id").and_then(|x| x.as_str()).unwrap_or("").to_string();
                if !id.is_empty() && seen_t.insert(id) {
                    tracks.push(t);
                }
            }
        }
    }
    let related: Vec<Value> = rel.iter().zip(per.iter()).filter(|(_, l)| !l.is_empty()).map(|((n, id), _)| json!({ "name": n, "id": id })).collect();
    let val = json!({ "artist": name, "related": related, "tracks": tracks });
    if !tracks.is_empty() {
        let mut c = similar_cache().lock().unwrap();
        if c.len() > 300 {
            c.clear();
        }
        c.insert(ckey, (now_secs(), val.clone()));
    }
    Ok(Json(val))
}

// ---------- Bài MỚI của một nghệ sĩ (kệ "X · bài mới" ở trang chủ) ----------
// Tìm theo tên chỉ ra bài cũ nổi tiếng nên không dùng được. Ở đây: mở trang nghệ sĩ → các single/album của năm nay (và năm ngoái) →
// lấy bài trong đó → hỏi YouTube ngày đăng thật của từng bài (microformat.publishDate). App chỉ coi là "mới" khi bài ra trong vài ngày gần đây.
#[derive(Deserialize)]
struct YtFreshQ {
    artist: Option<String>,
}

static FRESH_CACHE: std::sync::OnceLock<Mutex<HashMap<String, (u64, Value)>>> = std::sync::OnceLock::new();
fn fresh_cache() -> &'static Mutex<HashMap<String, (u64, Value)>> {
    FRESH_CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}
// ngày đăng của từng video (số ngày từ 1970-01-01); ngày đăng không đổi nên nhớ luôn
static PUBDAY_CACHE: std::sync::OnceLock<Mutex<HashMap<String, i64>>> = std::sync::OnceLock::new();
fn pubday_cache() -> &'static Mutex<HashMap<String, i64>> {
    PUBDAY_CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

// (năm, tháng, ngày) → số ngày kể từ 1970-01-01
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = (if y >= 0 { y } else { y - 399 }) / 400;
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}
// "2026-11-08T00:00:00-07:00" hoặc "2026-11-08" → số ngày
fn parse_ymd_days(s: &str) -> Option<i64> {
    let d = s.get(0..10)?;
    let mut it = d.split('-');
    let y: i64 = it.next()?.parse().ok()?;
    let m: i64 = it.next()?.parse().ok()?;
    let dd: i64 = it.next()?.parse().ok()?;
    if !(1990..=2100).contains(&y) || !(1..=12).contains(&m) || !(1..=31).contains(&dd) {
        return None;
    }
    Some(days_from_civil(y, m, dd))
}

async fn yt_pubday(client: &reqwest::Client, vid: &str) -> Option<i64> {
    if let Some(d) = pubday_cache().lock().unwrap().get(vid) {
        return Some(*d);
    }
    // thử lần lượt: client của YouTube Music rồi client web thường
    let tries: [(&str, &str, Value); 2] = [
        ("https://music.youtube.com", "https://music.youtube.com/youtubei/v1/player?prettyPrint=false", yt_ctx()),
        (
            "https://www.youtube.com",
            "https://www.youtube.com/youtubei/v1/player?prettyPrint=false",
            json!({ "client": { "clientName": "WEB", "clientVersion": "2.20250310.01.00", "hl": "en", "gl": "VN" } }),
        ),
    ];
    for (origin, url, ctx) in tries {
        let body = json!({ "context": ctx, "videoId": vid, "contentCheckOk": true, "racyCheckOk": true });
        let Ok(resp) = client
            .post(url)
            .header("origin", origin)
            .header("referer", format!("{origin}/"))
            .json(&body)
            .timeout(Duration::from_secs(10))
            .send()
            .await
        else {
            continue;
        };
        if !resp.status().is_success() {
            continue;
        }
        let Ok(v) = resp.json::<Value>().await else { continue };
        let day = ["publishDate", "uploadDate"]
            .iter()
            .find_map(|k| v.pointer(&format!("/microformat/playerMicroformatRenderer/{k}")).and_then(|x| x.as_str()).and_then(parse_ymd_days));
        if let Some(d) = day {
            let mut c = pubday_cache().lock().unwrap();
            if c.len() > 5000 {
                c.clear();
            }
            c.insert(vid.to_string(), d);
            return Some(d);
        }
    }
    None
}

// Các thẻ album/single (musicTwoRowItemRenderer có pageType = ALBUM) trên trang nghệ sĩ: (browseId, năm, tên, ảnh).
fn yt_album_cards(v: &Value, out: &mut Vec<(String, i64, String, String)>) {
    match v {
        Value::Object(m) => {
            if let Some(r) = m.get("musicTwoRowItemRenderer") {
                if let Some(be) = r.pointer("/navigationEndpoint/browseEndpoint") {
                    let pt = be
                        .pointer("/browseEndpointContextSupportedConfigs/browseEndpointContextMusicConfig/pageType")
                        .and_then(|x| x.as_str())
                        .unwrap_or("");
                    let id = be.get("browseId").and_then(|x| x.as_str()).unwrap_or("");
                    if pt == "MUSIC_PAGE_TYPE_ALBUM" && !id.is_empty() {
                        let sub = r.get("subtitle").map(yt_text).unwrap_or_default();
                        let year = sub
                            .split(|c: char| !c.is_ascii_digit())
                            .filter(|w| w.len() == 4)
                            .filter_map(|w| w.parse::<i64>().ok())
                            .find(|y| (1990..=2100).contains(y))
                            .unwrap_or(0);
                        let title = r.get("title").map(yt_text).unwrap_or_default();
                        let img = r
                            .pointer("/thumbnailRenderer/musicThumbnailRenderer/thumbnail/thumbnails")
                            .and_then(|a| a.as_array())
                            .and_then(|a| a.last())
                            .and_then(|t| t.get("url"))
                            .and_then(|u| u.as_str())
                            .unwrap_or("")
                            .to_string();
                        out.push((id.to_string(), year, title, img));
                    }
                }
            } else {
                for x in m.values() {
                    yt_album_cards(x, out);
                }
            }
        }
        Value::Array(a) => {
            for x in a {
                yt_album_cards(x, out);
            }
        }
        _ => {}
    }
}

// GET /api/yt/fresh?artist=Tên → { artist, tracks: [{..., age: số ngày kể từ ngày phát hành}] }  (mới nhất trước, chỉ giữ bài ≤ 60 ngày)
async fn yt_fresh(State(st): State<AppState>, headers: HeaderMap, Query(q): Query<YtFreshQ>) -> R<Value> {
    let (allow_explicit, use_proxy) = net_prefs(&st, &headers).await;
    let client = pick_client(&st, use_proxy);
    let name: String = q.artist.unwrap_or_default().trim().chars().take(80).collect();
    if name.is_empty() {
        return Err(bad("Thiếu tên nghệ sĩ"));
    }
    let lname = name.to_lowercase();
    let ckey = format!("{}|{}|{}", lname, allow_explicit as u8, use_proxy as u8);
    if let Some((t, v)) = fresh_cache().lock().unwrap().get(&ckey) {
        if now_secs().saturating_sub(*t) < 1800 {
            return Ok(Json(v.clone()));
        }
    }
    let empty = json!({ "artist": name, "tracks": [] });
    let today = (now_secs() / 86400) as i64;
    let cur_year = 1970 + (today as f64 / 365.2425) as i64;

    // 1) tìm nghệ sĩ
    let Some(sv) = yt_post(&client, "search", json!({ "context": yt_ctx(), "query": name, "params": yt_filter_params("artists").unwrap_or("") })).await else {
        return Err((StatusCode::BAD_GATEWAY, "Không kết nối được YouTube Music".to_string()));
    };
    let mut tr0: Vec<Value> = Vec::new();
    let mut ents: Vec<Value> = Vec::new();
    yt_collect_all(&sv, &mut tr0, &mut ents);
    let is_artist = |e: &&Value| e.get("kind").and_then(|x| x.as_str()) == Some("artist");
    let exact = ents
        .iter()
        .filter(is_artist)
        .find(|e| e.get("title").and_then(|x| x.as_str()).map(|t| t.to_lowercase() == lname).unwrap_or(false));
    let nname = norm_name(&name);
    let near = ents.iter().filter(is_artist).find(|e| {
        let t = norm_name(e.get("title").and_then(|x| x.as_str()).unwrap_or(""));
        !t.is_empty() && !nname.is_empty() && (t.contains(&nname) || nname.contains(&t))
    });
    let Some(aid) = exact.or(near).and_then(|e| e.get("id")).and_then(|x| x.as_str()).map(|s| s.to_string()) else {
        return Ok(Json(empty));
    };

    // 2) trang nghệ sĩ → single/album của năm nay và năm ngoái
    let Some(bv) = yt_post(&client, "browse", json!({ "context": yt_ctx(), "browseId": aid })).await else {
        return Ok(Json(empty));
    };
    let mut cards: Vec<(String, i64, String, String)> = Vec::new();
    yt_album_cards(&bv, &mut cards);
    let mut seen_a: HashSet<String> = HashSet::new();
    cards.retain(|c| c.1 >= cur_year - 1 && seen_a.insert(c.0.clone()));
    cards.sort_by_key(|c| std::cmp::Reverse(c.1));   // năm mới trước (sắp xếp ổn định giữ thứ tự trên trang)
    cards.truncate(10);
    if cards.is_empty() {
        return Ok(Json(empty));
    }

    // 3) bài trong từng album/single
    let pages = futures_util::future::join_all(cards.iter().map(|c| {
        let cl = client.clone();
        let id = c.0.clone();
        async move { yt_post(&cl, "browse", json!({ "context": yt_ctx(), "browseId": id })).await }
    }))
    .await;
    let mut cand: Vec<Value> = Vec::new();
    let mut seen_t: HashSet<String> = HashSet::new();
    for (c, pg) in cards.iter().zip(pages.into_iter()) {
        let Some(pg) = pg else { continue };
        let mut ts: Vec<Value> = Vec::new();
        yt_collect(&pg, &mut ts);
        ts.retain(|t| !yt_is_non_music(t));
        for mut t in ts {
            let a = t.get("artist").and_then(|x| x.as_str()).unwrap_or("").to_string();
            if a.is_empty() || a.to_lowercase().contains("plays") {
                t["artist"] = json!(name);
            }
            if !c.3.is_empty() {
                t["image"] = json!(c.3);
            }
            if !allow_explicit && t.get("explicit").and_then(|x| x.as_bool()).unwrap_or(false) {
                continue;
            }
            let id = t.get("id").and_then(|x| x.as_str()).unwrap_or("").to_string();
            if !id.is_empty() && seen_t.insert(id) {
                cand.push(t);
            }
        }
    }
    cand.truncate(30);

    // 4) ngày đăng thật của từng bài
    let days = futures_util::future::join_all(cand.iter().map(|t| {
        let cl = client.clone();
        let id = t.get("id").and_then(|x| x.as_str()).unwrap_or("").to_string();
        async move { yt_pubday(&cl, &id).await }
    }))
    .await;
    let mut out: Vec<(i64, Value)> = Vec::new();
    for (mut t, d) in cand.into_iter().zip(days.into_iter()) {
        let Some(d) = d else { continue };   // không biết ngày thì không coi là mới
        let age = today - d;
        if (-1..=60).contains(&age) {
            t["age"] = json!(age.max(0));
            out.push((age, t));
        }
    }
    out.sort_by_key(|x| x.0);
    let tracks: Vec<Value> = out.into_iter().map(|x| x.1).collect();
    let val = json!({ "artist": name, "tracks": tracks });
    {
        let mut c = fresh_cache().lock().unwrap();
        if c.len() > 200 {
            c.clear();
        }
        c.insert(ckey, (now_secs(), val.clone()));
    }
    Ok(Json(val))
}

// ---------- Nghệ sĩ nổi bật ("Best of artists") + danh sách "This Is …" ----------
// Không có danh sách nghệ sĩ viết sẵn: server đọc bảng xếp hạng YouTube Music (toàn cầu + khu vực của người dùng; nếu bảng lỗi thì
// dùng kết quả tìm video "đang thịnh hành"), lấy vị trí xếp hạng, LƯỢT XEM của từng bài, rồi hỏi thêm LƯỢT THÍCH (Return YouTube Dislike)
// cho các bài đứng đầu. Từ đó chấm điểm "độ hot" của từng nghệ sĩ (nhiều bài hot + bài hot nhiều lượt xem/thích + đứng cao trên
// bảng nghệ sĩ + đang tăng hạng + lượng khán giả). Kết quả giữ 30 phút trong cache.

// "1.2M views" / "523K" / "1,234,567 plays" -> số. None nếu không đọc được.
fn parse_count(s: &str) -> Option<f64> {
    let tok = s.trim().split_whitespace().next()?;
    let mut num = String::new();
    let mut mult = 1.0f64;
    for c in tok.chars() {
        if c.is_ascii_digit() || c == '.' {
            num.push(c);
        } else if c == ',' {
            continue;
        } else {
            mult = match c.to_ascii_lowercase() {
                'k' => 1e3,
                'm' => 1e6,
                'b' => 1e9,
                _ => return None,
            };
            break;
        }
    }
    if num.is_empty() {
        return None;
    }
    num.parse::<f64>().ok().map(|n| n * mult)
}

// Lượt xem / lượt nghe trong các phần của dòng phụ ("Artist • 12M views • 3:45").
fn parse_views(parts: &[String]) -> f64 {
    for p in parts {
        let l = p.trim().to_lowercase();
        if l.contains("views") || l.contains("plays") {
            if let Some(n) = parse_count(&l) {
                return n;
            }
        }
    }
    0.0
}

// Số người theo dõi / khán giả hằng tháng trong dòng phụ của một nghệ sĩ ("Artist • 12M monthly audience", "1.2M subscribers").
fn parse_audience(sub: &str) -> f64 {
    for p in sub.split('\u{2022}') {
        let l = p.trim().to_lowercase();
        if l.contains("subscriber") || l.contains("audience") || l.contains("listener") || l.contains("follower") {
            if let Some(n) = parse_count(&l) {
                return n;
            }
        }
    }
    0.0
}

// Tách chuỗi nghệ sĩ của một bài ("A, B & C feat. D") thành các tên (giữ nguyên chữ hoa/thường để hiển thị).
fn split_credits(s: &str) -> Vec<String> {
    let mut t = s.to_string();
    for sep in [" feat. ", " feat ", " ft. ", " ft ", " x ", " X ", " × "] {
        t = t.replace(sep, ",");
    }
    let v: Vec<String> = t
        .split(|c: char| matches!(c, ',' | '&' | ';' | '/' | '、' | '・'))
        .map(|x| x.trim().trim_end_matches("- Topic").trim().to_string())
        .filter(|x| x.chars().count() >= 2 && x.chars().any(|c| c.is_alphabetic()))
        .collect();
    v
}

// log10(x) quy về 0..1 giữa hai mốc (lo, hi là số mũ của 10).
fn norm_log(x: f64, lo: f64, hi: f64) -> f64 {
    if x <= 0.0 {
        return 0.0;
    }
    ((x.log10() - lo) / (hi - lo)).clamp(0.0, 1.0)
}

// Độ hot của MỘT bài: vị trí trên bảng (0..1) + lượt xem + lượt thích. Thiếu thành phần nào thì chia lại trọng số cho phần còn lại.
fn track_heat(rank: f64, views: f64, likes: f64) -> f64 {
    let mut w = 0.45;
    let mut s = 0.45 * rank;
    if views > 0.0 {
        s += 0.35 * norm_log(views, 4.0, 9.0); // 10 nghìn -> 0, 1 tỷ -> 1
        w += 0.35;
    }
    if likes > 0.0 {
        s += 0.20 * norm_log(likes, 2.5, 7.0); // 300 -> 0, 10 triệu -> 1
        w += 0.20;
    }
    s / w
}

// Độ hot của một nghệ sĩ: các bài hot nhất cộng dồn (bài thứ hai, ba… tính nhẹ dần), thêm điểm đứng trên bảng nghệ sĩ,
// đang tăng/giảm hạng và quy mô khán giả.
fn artist_heat(mut heats: Vec<f64>, chart_rank: f64, trend: i32, audience: f64) -> f64 {
    heats.sort_by(|a, b| b.partial_cmp(a).unwrap_or(std::cmp::Ordering::Equal));
    let mut s = 0.0;
    let mut w = 1.0;
    for h in heats.iter().take(6) {
        s += h * w;
        w *= 0.6;
    }
    s + 0.9 * chart_rank + 0.08 * trend as f64 + 0.25 * norm_log(audience, 4.0, 8.0)
}

struct ChartTrack {
    id: String,
    artists: Vec<String>,
    shelf: usize,
    views: f64,
    trend: i32,
}
struct ChartArtist {
    name: String,
    id: String,
    image: String,
    shelf: usize,
    audience: f64,
    trend: i32,
}
#[derive(Default)]
struct ChartAcc {
    shelf: usize,
    tracks: Vec<ChartTrack>,
    artists: Vec<ChartArtist>,
}
struct HotTrack {
    id: String,
    artists: Vec<String>,
    rank: f64,
    views: f64,
    likes: f64,
    trend: i32,
}
struct HotArtist {
    name: String,
    id: String,
    image: String,
    rank: f64,
    audience: f64,
    trend: i32,
}
struct ArtistAgg {
    name: String,
    id: String,
    image: String,
    heats: Vec<f64>,
    chart_rank: f64,
    trend: i32,
    audience: f64,
}

fn yt_trend(item: &Value) -> i32 {
    let s = item.to_string();
    if s.contains("ARROW_DROP_UP") {
        1
    } else if s.contains("ARROW_DROP_DOWN") {
        -1
    } else {
        0
    }
}

fn yt_thumb_last(v: Option<&Value>) -> String {
    v.and_then(|a| a.as_array())
        .and_then(|a| a.last())
        .and_then(|t| t.get("url"))
        .and_then(|u| u.as_str())
        .unwrap_or("")
        .to_string()
}

// Các phần ("Artist", "Album", "3:45"…) của cột chữ thứ i trong một dòng danh sách của YouTube Music.
fn yt_flex_parts(item: &Value, i: usize) -> Vec<String> {
    let txt: String = item
        .get("flexColumns")
        .and_then(|c| c.as_array())
        .and_then(|c| c.get(i))
        .and_then(|c| c.pointer("/musicResponsiveListItemFlexColumnRenderer/text"))
        .map(yt_text)
        .unwrap_or_default();
    let v: Vec<String> = txt.split(" \u{2022} ").map(|x| x.trim().to_string()).filter(|x| !x.is_empty()).collect();
    v
}

// Duyệt một trang (bảng xếp hạng hoặc kết quả tìm kiếm), gom bài hát và nghệ sĩ kèm số "kệ" (shelf) để sau này suy ra thứ hạng trong kệ.
fn yt_chart_walk(v: &Value, acc: &mut ChartAcc) {
    match v {
        Value::Object(m) => {
            if let Some(item) = m.get("musicResponsiveListItemRenderer") {
                let trend = yt_trend(item);
                if let Some(t) = yt_track(item) {
                    let id = t.get("id").and_then(|x| x.as_str()).unwrap_or("").to_string();
                    let artist = t.get("artist").and_then(|x| x.as_str()).unwrap_or("").to_string();
                    let views = parse_views(&yt_flex_parts(item, 1));
                    acc.tracks.push(ChartTrack { id, artists: split_credits(&artist), shelf: acc.shelf, views, trend });
                } else if let Some(e) = yt_entity(item) {
                    if e.get("kind").and_then(|x| x.as_str()) == Some("artist") {
                        let name = e.get("title").and_then(|x| x.as_str()).unwrap_or("").to_string();
                        let id = e.get("id").and_then(|x| x.as_str()).unwrap_or("").to_string();
                        let image = e.get("image").and_then(|x| x.as_str()).unwrap_or("").to_string();
                        let sub = e.get("sub").and_then(|x| x.as_str()).unwrap_or("").to_string();
                        if !name.is_empty() && !id.is_empty() {
                            acc.artists.push(ChartArtist { name, id, image, shelf: acc.shelf, audience: parse_audience(&sub), trend });
                        }
                    }
                }
                return;
            }
            if let Some(r) = m.get("musicTwoRowItemRenderer") {
                let title = r.get("title").map(yt_text).unwrap_or_default();
                let sub = r.get("subtitle").map(yt_text).unwrap_or_default();
                let parts: Vec<String> = sub.split(" \u{2022} ").map(|x| x.trim().to_string()).filter(|x| !x.is_empty()).collect();
                if let Some(vid) = r.pointer("/navigationEndpoint/watchEndpoint/videoId").and_then(|x| x.as_str()) {
                    let artist = parts.iter().find(|p| !yt_junk_part(p)).cloned().unwrap_or_default();
                    acc.tracks.push(ChartTrack { id: vid.to_string(), artists: split_credits(&artist), shelf: acc.shelf, views: parse_views(&parts), trend: yt_trend(r) });
                } else if let Some(be) = r.pointer("/navigationEndpoint/browseEndpoint") {
                    let pt = be
                        .pointer("/browseEndpointContextSupportedConfigs/browseEndpointContextMusicConfig/pageType")
                        .and_then(|x| x.as_str())
                        .unwrap_or("");
                    let id = be.get("browseId").and_then(|x| x.as_str()).unwrap_or("").to_string();
                    if pt == "MUSIC_PAGE_TYPE_ARTIST" && !id.is_empty() && !title.is_empty() {
                        let image = yt_thumb_last(r.pointer("/thumbnailRenderer/musicThumbnailRenderer/thumbnail/thumbnails"));
                        acc.artists.push(ChartArtist { name: title, id, image, shelf: acc.shelf, audience: parse_audience(&sub), trend: yt_trend(r) });
                    }
                }
                return;
            }
            if m.contains_key("musicCarouselShelfRenderer") || m.contains_key("musicShelfRenderer") {
                acc.shelf += 1;
            }
            for x in m.values() {
                yt_chart_walk(x, acc);
            }
        }
        Value::Array(a) => {
            for x in a {
                yt_chart_walk(x, acc);
            }
        }
        _ => {}
    }
}

// Đổi vị trí trong kệ thành điểm 0..1 (đứng đầu = 1) rồi gộp vào danh sách chung.
fn chart_finish(acc: ChartAcc, tr: &mut Vec<HotTrack>, ar: &mut Vec<HotArtist>) {
    let ChartAcc { tracks, artists, .. } = acc;
    let mut tcount: HashMap<usize, usize> = HashMap::new();
    for t in &tracks {
        *tcount.entry(t.shelf).or_insert(0) += 1;
    }
    let mut tseen: HashMap<usize, usize> = HashMap::new();
    for t in tracks {
        let n = *tcount.get(&t.shelf).unwrap_or(&1) as f64;
        let i = tseen.entry(t.shelf).or_insert(0);
        let rank = 1.0 - (*i as f64) / n.max(1.0);
        *i += 1;
        tr.push(HotTrack { id: t.id, artists: t.artists, rank, views: t.views, likes: 0.0, trend: t.trend });
    }
    let mut acount: HashMap<usize, usize> = HashMap::new();
    for a in &artists {
        *acount.entry(a.shelf).or_insert(0) += 1;
    }
    let mut aseen: HashMap<usize, usize> = HashMap::new();
    for a in artists {
        let n = *acount.get(&a.shelf).unwrap_or(&1) as f64;
        let i = aseen.entry(a.shelf).or_insert(0);
        let rank = 1.0 - (*i as f64) / n.max(1.0);
        *i += 1;
        ar.push(HotArtist { name: a.name, id: a.id, image: a.image, rank, audience: a.audience, trend: a.trend });
    }
}

async fn yt_chart_page(client: &reqwest::Client, region: &str) -> Option<Value> {
    let mut body = json!({ "context": yt_ctx(), "browseId": "FEmusic_charts" });
    if !region.is_empty() {
        body["formData"] = json!({ "selectedValues": [region] });
    }
    yt_post(client, "browse", body).await
}

static RYD_CACHE: std::sync::OnceLock<Mutex<HashMap<String, (u64, f64, f64)>>> = std::sync::OnceLock::new();
fn ryd_cache() -> &'static Mutex<HashMap<String, (u64, f64, f64)>> {
    RYD_CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

// Lượt thích + lượt xem của một video từ Return YouTube Dislike (không cần khoá API). Lỗi thì None, bài đó chỉ mất phần điểm "thích".
async fn ryd_votes(client: &reqwest::Client, id: &str) -> Option<(f64, f64)> {
    if let Some((t, l, v)) = ryd_cache().lock().unwrap().get(id) {
        if now_secs().saturating_sub(*t) < 6 * 3600 {
            return Some((*l, *v));
        }
    }
    let resp = client
        .get(format!("https://returnyoutubedislikeapi.com/votes?videoId={id}"))
        .timeout(Duration::from_secs(5))
        .send()
        .await
        .ok()?;
    if !resp.status().is_success() {
        return None;
    }
    let j: Value = resp.json().await.ok()?;
    let likes = j.get("likes").and_then(|x| x.as_f64()).unwrap_or(0.0);
    let views = j.get("viewCount").and_then(|x| x.as_f64()).unwrap_or(0.0);
    {
        let mut c = ryd_cache().lock().unwrap();
        if c.len() > 3000 {
            c.clear();
        }
        c.insert(id.to_string(), (now_secs(), likes, views));
    }
    Some((likes, views))
}

static ARTIST_CACHE: std::sync::OnceLock<Mutex<HashMap<String, (u64, Value)>>> = std::sync::OnceLock::new();
fn artist_cache() -> &'static Mutex<HashMap<String, (u64, Value)>> {
    ARTIST_CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}
static BEST_CACHE: std::sync::OnceLock<Mutex<HashMap<String, (u64, Value)>>> = std::sync::OnceLock::new();
fn best_cache() -> &'static Mutex<HashMap<String, (u64, Value)>> {
    BEST_CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}
static THISIS_CACHE: std::sync::OnceLock<Mutex<HashMap<String, (u64, Value)>>> = std::sync::OnceLock::new();
fn thisis_cache() -> &'static Mutex<HashMap<String, (u64, Value)>> {
    THISIS_CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

// Tìm trang nghệ sĩ theo tên (ưu tiên trùng khớp, rồi tên chứa nhau). Trả {kind,id,title,sub,image}.
async fn yt_find_artist(client: &reqwest::Client, name: &str) -> Option<Value> {
    let sv = yt_post(client, "search", json!({ "context": yt_ctx(), "query": name, "params": yt_filter_params("artists").unwrap_or("") })).await?;
    let mut tr0: Vec<Value> = Vec::new();
    let mut ents: Vec<Value> = Vec::new();
    yt_collect_all(&sv, &mut tr0, &mut ents);
    let lname = name.to_lowercase();
    let nname = norm_name(name);
    let artists: Vec<&Value> = ents.iter().filter(|e| e.get("kind").and_then(|x| x.as_str()) == Some("artist")).collect();
    let exact = artists
        .iter()
        .find(|e| e.get("title").and_then(|x| x.as_str()).map(|t| t.to_lowercase() == lname).unwrap_or(false));
    let near = artists.iter().find(|e| {
        let t = norm_name(e.get("title").and_then(|x| x.as_str()).unwrap_or(""));
        !t.is_empty() && !nname.is_empty() && (t.contains(&nname) || nname.contains(&t))
    });
    let found: Option<Value> = exact.or(near).map(|e| (**e).clone());
    found
}

// Như trên nhưng nhớ 24 giờ (chỉ nhớ khi tìm thấy; lỗi mạng hay không thấy thì lần sau thử lại).
async fn yt_find_artist_cached(client: &reqwest::Client, name: &str) -> Option<Value> {
    let k = norm_name(name);
    if k.is_empty() {
        return None;
    }
    if let Some((t, v)) = artist_cache().lock().unwrap().get(&k) {
        if now_secs().saturating_sub(*t) < 86400 {
            return Some(v.clone());
        }
    }
    let r = yt_find_artist(client, name).await;
    if let Some(v) = &r {
        let mut c = artist_cache().lock().unwrap();
        if c.len() > 2000 {
            c.clear();
        }
        c.insert(k, (now_secs(), v.clone()));
    }
    r
}

// GET /api/yt/artists?names=A|B|C → [{kind:"artist",id,title,sub,image}]  (ảnh + trang nghệ sĩ cho kệ "Nghệ sĩ yêu thích")
#[derive(Deserialize)]
struct YtArtistsQ {
    names: Option<String>,
}

async fn yt_artists(State(st): State<AppState>, headers: HeaderMap, Query(q): Query<YtArtistsQ>) -> R<Value> {
    let use_proxy = match require(&st, &headers) {
        Ok(user) => user_net_prefs(&st, user.id).1,
        Err(_) => false,
    };
    let client = pick_client(&st, use_proxy);
    let names: Vec<String> = q
        .names
        .unwrap_or_default()
        .split('|')
        .map(|x| x.trim().chars().take(80).collect::<String>())
        .filter(|x| !x.is_empty())
        .take(16)
        .collect();
    let found = futures_util::future::join_all(names.iter().map(|n| {
        let cl = client.clone();
        let n = n.clone();
        async move { yt_find_artist_cached(&cl, &n).await }
    }))
    .await;
    let mut out: Vec<Value> = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();
    for e in found.into_iter().flatten() {
        let id = e.get("id").and_then(|x| x.as_str()).unwrap_or("").to_string();
        if !id.is_empty() && seen.insert(id) {
            out.push(e);
        }
    }
    Ok(Json(Value::Array(out)))
}

// GET /api/yt/best-artists?region=VN → { artists: [{kind,id,title,sub,image,heat}], region, source }   (đã xếp theo độ hot, giảm dần)
#[derive(Deserialize)]
struct YtBestQ {
    region: Option<String>,
}

async fn yt_best_artists(State(st): State<AppState>, headers: HeaderMap, Query(q): Query<YtBestQ>) -> R<Value> {
    let use_proxy = match require(&st, &headers) {
        Ok(user) => user_net_prefs(&st, user.id).1,
        Err(_) => false,
    };
    let client = pick_client(&st, use_proxy);
    let region: String = q.region.unwrap_or_default().trim().to_uppercase().chars().filter(|c| c.is_ascii_alphabetic()).take(2).collect();
    let ckey = format!("{}|{}", region, use_proxy as u8);
    if let Some((t, v)) = best_cache().lock().unwrap().get(&ckey) {
        if now_secs().saturating_sub(*t) < 1800 {
            return Ok(Json(v.clone()));
        }
    }

    // 1) bảng xếp hạng YouTube Music: toàn cầu + khu vực của người dùng
    let mut regions: Vec<String> = vec!["ZZ".to_string()];
    if region.len() == 2 && region != "ZZ" {
        regions.push(region.clone());
    }
    let pages = futures_util::future::join_all(regions.iter().map(|r| {
        let cl = client.clone();
        let r = r.clone();
        async move { yt_chart_page(&cl, &r).await }
    }))
    .await;
    let mut tracks: Vec<HotTrack> = Vec::new();
    let mut charted: Vec<HotArtist> = Vec::new();
    let mut source = "charts";
    for pg in pages.into_iter().flatten() {
        let mut acc = ChartAcc::default();
        yt_chart_walk(&pg, &mut acc);
        chart_finish(acc, &mut tracks, &mut charted);
    }
    // 2) bảng không đọc được (YouTube đổi cấu trúc / bị chặn): dùng video đang thịnh hành từ tìm kiếm
    if charted.len() < 6 && tracks.len() < 10 {
        source = "search";
        let qs = ["top songs this week", "trending music videos"];
        let pages = futures_util::future::join_all(qs.iter().map(|text| {
            let cl = client.clone();
            let body = json!({ "context": yt_ctx(), "query": text, "params": yt_filter_params("videos").unwrap_or("") });
            async move { yt_post(&cl, "search", body).await }
        }))
        .await;
        for pg in pages.into_iter().flatten() {
            let mut acc = ChartAcc::default();
            yt_chart_walk(&pg, &mut acc);
            chart_finish(acc, &mut tracks, &mut charted);
        }
    }
    if tracks.is_empty() && charted.is_empty() {
        return Ok(Json(json!({ "artists": [], "region": region, "source": "none" })));
    }

    // 3) gộp bài trùng (một bài có thể nằm ở nhiều bảng): lấy thứ hạng/lượt xem cao nhất
    let mut tmap: HashMap<String, HotTrack> = HashMap::new();
    for t in tracks {
        if t.id.len() != 11 {
            continue;
        }
        if let Some(e) = tmap.get_mut(&t.id) {
            if t.rank > e.rank {
                e.rank = t.rank;
            }
            if t.views > e.views {
                e.views = t.views;
            }
            if t.trend > e.trend {
                e.trend = t.trend;
            }
            continue;
        }
        tmap.insert(t.id.clone(), t);
    }
    let mut tv: Vec<HotTrack> = tmap.into_values().collect();
    tv.sort_by(|a, b| b.rank.partial_cmp(&a.rank).unwrap_or(std::cmp::Ordering::Equal));

    // 4) lượt thích (và lượt xem nếu bảng không ghi) cho các bài đứng đầu
    let top_n = tv.len().min(30);
    let votes = futures_util::future::join_all(tv.iter().take(top_n).map(|t| {
        let cl = client.clone();
        let id = t.id.clone();
        async move { ryd_votes(&cl, &id).await }
    }))
    .await;
    for (t, v) in tv.iter_mut().zip(votes.into_iter()) {
        if let Some((likes, views)) = v {
            t.likes = likes;
            if t.views <= 0.0 {
                t.views = views;
            }
        }
    }

    // 5) chấm điểm từng nghệ sĩ
    let mut agg: HashMap<String, ArtistAgg> = HashMap::new();
    let skip_name = |k: &str| k.is_empty() || k == "variousartists" || k == "youtubemusic" || k == "unknown" || k == "various";
    for a in charted {
        let k = norm_name(&a.name);
        if skip_name(&k) {
            continue;
        }
        let e = agg.entry(k).or_insert_with(|| ArtistAgg {
            name: a.name.clone(),
            id: String::new(),
            image: String::new(),
            heats: Vec::new(),
            chart_rank: 0.0,
            trend: 0,
            audience: 0.0,
        });
        if a.rank > e.chart_rank {
            e.chart_rank = a.rank;
        }
        if e.id.is_empty() {
            e.id = a.id;
        }
        if e.image.is_empty() {
            e.image = a.image;
        }
        if a.audience > e.audience {
            e.audience = a.audience;
        }
        if a.trend > e.trend {
            e.trend = a.trend;
        }
    }
    for t in &tv {
        let heat = track_heat(t.rank, t.views, t.likes);
        for name in &t.artists {
            let k = norm_name(name);
            if skip_name(&k) {
                continue;
            }
            let e = agg.entry(k).or_insert_with(|| ArtistAgg {
                name: name.clone(),
                id: String::new(),
                image: String::new(),
                heats: Vec::new(),
                chart_rank: 0.0,
                trend: 0,
                audience: 0.0,
            });
            e.heats.push(heat);
            if t.trend > e.trend {
                e.trend = t.trend;
            }
        }
    }
    let mut ranked: Vec<(f64, ArtistAgg)> = agg
        .into_values()
        .map(|a| (artist_heat(a.heats.clone(), a.chart_rank, a.trend, a.audience), a))
        .collect();
    ranked.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
    ranked.truncate(24);

    // 6) lấy ảnh + trang nghệ sĩ cho người chưa có (bảng nghệ sĩ đã kèm sẵn thì khỏi tìm)
    let resolved = futures_util::future::join_all(ranked.iter().map(|(_, a)| {
        let cl = client.clone();
        let name = a.name.clone();
        let have = !a.id.is_empty() && !a.image.is_empty();
        async move {
            if have {
                None
            } else {
                yt_find_artist_cached(&cl, &name).await
            }
        }
    }))
    .await;
    let mut out: Vec<Value> = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();
    for ((heat, a), r) in ranked.into_iter().zip(resolved.into_iter()) {
        let mut id = a.id;
        let mut image = a.image;
        let mut title = a.name;
        if let Some(e) = r {
            id = e.get("id").and_then(|x| x.as_str()).unwrap_or("").to_string();
            image = e.get("image").and_then(|x| x.as_str()).unwrap_or("").to_string();
            let t = e.get("title").and_then(|x| x.as_str()).unwrap_or("");
            if !t.is_empty() {
                title = t.to_string();
            }
        }
        if id.is_empty() || image.is_empty() || !seen.insert(id.clone()) {
            continue;
        }
        out.push(json!({ "kind": "artist", "id": id, "title": title, "sub": "Nghệ sĩ", "image": image, "heat": (heat * 1000.0).round() / 1000.0 }));
    }
    let val = json!({ "artists": out, "region": region, "source": source });
    if out_len_ok(&val) {
        let mut c = best_cache().lock().unwrap();
        if c.len() > 50 {
            c.clear();
        }
        c.insert(ckey, (now_secs(), val.clone()));
    }
    Ok(Json(val))
}

fn out_len_ok(v: &Value) -> bool {
    v.get("artists").and_then(|a| a.as_array()).map(|a| a.len() >= 3).unwrap_or(false)
}

// Thêm lượt xem / album / thời lượng (cột cố định của trang playlist) vào bài hát lấy từ một trang YouTube Music.
fn yt_collect_meta(v: &Value, out: &mut Vec<Value>) {
    match v {
        Value::Object(m) => {
            if let Some(item) = m.get("musicResponsiveListItemRenderer") {
                if let Some(mut t) = yt_track(item) {
                    let p1 = yt_flex_parts(item, 1);
                    let p2 = yt_flex_parts(item, 2);
                    let views = parse_views(&p1).max(parse_views(&p2));
                    if views > 0.0 {
                        t["views"] = json!(views);
                    }
                    let alb2 = p2.into_iter().find(|p| !yt_junk_part(p));
                    let alb1 = p1.iter().skip(1).find(|p| !yt_junk_part(p)).cloned();
                    if let Some(a) = alb2.or(alb1) {
                        t["album"] = json!(a);
                    }
                    if t["dur"].as_u64().unwrap_or(0) == 0 {
                        let fixed = item
                            .pointer("/fixedColumns/0/musicResponsiveListItemFixedColumnRenderer/text")
                            .map(yt_text)
                            .unwrap_or_default();
                        if let Some(d) = yt_dur(&fixed) {
                            t["dur"] = json!(d);
                        }
                    }
                    out.push(t);
                }
            } else {
                for x in m.values() {
                    yt_collect_meta(x, out);
                }
            }
        }
        Value::Array(a) => {
            for x in a {
                yt_collect_meta(x, out);
            }
        }
        _ => {}
    }
}

// GET /api/yt/this-is?name=Tên&id=browseId → { title:"This Is …", name, id, image, tracks:[…tối đa 50 bài tiêu biểu…] }
// Bài lấy từ mục "Songs" của trang nghệ sĩ (YouTube Music xếp theo độ phổ biến, nút "See all" cho cả danh sách), thiếu thì bổ sung bằng tìm kiếm.
#[derive(Deserialize)]
struct YtThisQ {
    name: Option<String>,
    id: Option<String>,
}

async fn yt_this_is(State(st): State<AppState>, headers: HeaderMap, Query(q): Query<YtThisQ>) -> R<Value> {
    let (allow_explicit, use_proxy) = net_prefs(&st, &headers).await;
    let client = pick_client(&st, use_proxy);
    let name: String = q.name.unwrap_or_default().trim().chars().take(80).collect();
    if name.is_empty() {
        return Err(bad("Thiếu tên nghệ sĩ"));
    }
    let mut aid: String = q.id.unwrap_or_default().trim().chars().take(80).collect();
    if !aid.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_') {
        aid.clear();
    }
    let ckey = format!("{}|{}|{}|{}", name.to_lowercase(), aid, allow_explicit as u8, use_proxy as u8);
    if let Some((t, v)) = thisis_cache().lock().unwrap().get(&ckey) {
        if now_secs().saturating_sub(*t) < 3600 {
            return Ok(Json(v.clone()));
        }
    }
    let mut title = name.clone();
    let mut image = String::new();
    if aid.is_empty() {
        if let Some(e) = yt_find_artist_cached(&client, &name).await {
            aid = e.get("id").and_then(|x| x.as_str()).unwrap_or("").to_string();
            image = e.get("image").and_then(|x| x.as_str()).unwrap_or("").to_string();
            let t = e.get("title").and_then(|x| x.as_str()).unwrap_or("");
            if !t.is_empty() {
                title = t.to_string();
            }
        }
    }

    // 1) trang nghệ sĩ: mục Songs (5 bài đầu) + danh sách đầy đủ ở nút "See all"
    let mut cand: Vec<Value> = Vec::new();
    if !aid.is_empty() {
        if let Some(bv) = yt_post(&client, "browse", json!({ "context": yt_ctx(), "browseId": aid })).await {
            if image.is_empty() {
                let header = ["musicImmersiveHeaderRenderer", "musicVisualHeaderRenderer", "musicDetailHeaderRenderer"].iter().find_map(|k| yt_find(&bv, k));
                image = yt_thumb_last(header.and_then(|h| yt_find(h, "thumbnails")));
            }
            yt_collect_meta(&bv, &mut cand);
            let all_ep = yt_find(&bv, "musicShelfRenderer").and_then(|s| s.pointer("/bottomEndpoint/browseEndpoint").or_else(|| s.pointer("/title/runs/0/navigationEndpoint/browseEndpoint")));
            if let Some(be) = all_ep {
                let pid = be.get("browseId").and_then(|x| x.as_str()).unwrap_or("").to_string();
                if !pid.is_empty() && pid != aid {
                    let mut body = json!({ "context": yt_ctx(), "browseId": pid });
                    if let Some(p) = be.get("params").and_then(|x| x.as_str()) {
                        body["params"] = json!(p);
                    }
                    if let Some(pv) = yt_post(&client, "browse", body).await {
                        yt_collect_meta(&pv, &mut cand);
                    }
                }
            }
        }
    }
    for t in cand.iter_mut() {
        if t.get("artist").and_then(|x| x.as_str()).unwrap_or("").is_empty() {
            t["artist"] = json!(title);
        }
    }

    // 2) chưa đủ bài: bổ sung từ tìm kiếm, chỉ giữ bài thật sự có tên nghệ sĩ này
    if cand.len() < 15 {
        let qs = [name.clone(), format!("{name} best songs")];
        let found = futures_util::future::join_all(qs.iter().map(|text| {
            let cl = client.clone();
            let text = text.clone();
            async move { yt_songs(&cl, &text).await }
        }))
        .await;
        let mine: HashSet<String> = [norm_name(&name), norm_name(&title)].into_iter().filter(|x| !x.is_empty()).collect();
        for list in found {
            for t in list {
                let a = t.get("artist").and_then(|x| x.as_str()).unwrap_or("");
                if credit_names(a).iter().any(|n| mine.contains(n) || mine.iter().any(|m| m.chars().count() >= 3 && n.contains(m.as_str()))) {
                    cand.push(t);
                }
            }
        }
    }

    // 3) lọc trùng (cùng id hoặc cùng tên bài), bỏ bản dài bất thường và nội dung nhạy cảm nếu người dùng tắt
    let mut seen_id: HashSet<String> = HashSet::new();
    let mut seen_title: HashSet<String> = HashSet::new();
    let mut tracks: Vec<Value> = Vec::new();
    for t in cand {
        let id = t.get("id").and_then(|x| x.as_str()).unwrap_or("").to_string();
        if id.is_empty() || seen_id.contains(&id) {
            continue;
        }
        if !allow_explicit && t.get("explicit").and_then(|x| x.as_bool()).unwrap_or(false) {
            continue;
        }
        if t.get("dur").and_then(|x| x.as_u64()).unwrap_or(0) > 12 * 60 {
            continue;
        }
        let raw = t.get("title").and_then(|x| x.as_str()).unwrap_or("");
        let base = raw.split(|c: char| c == '(' || c == '[').next().unwrap_or(raw);
        let tk = norm_name(base);
        if !tk.is_empty() && !seen_title.insert(tk) {
            continue;
        }
        seen_id.insert(id);
        tracks.push(t);
        if tracks.len() >= 50 {
            break;
        }
    }
    if image.is_empty() {
        image = tracks.first().and_then(|t| t.get("image")).and_then(|x| x.as_str()).unwrap_or("").to_string();
    }
    let val = json!({ "title": format!("This Is {title}"), "name": title, "id": aid, "image": image, "tracks": tracks });
    if val.get("tracks").and_then(|a| a.as_array()).map(|a| a.len() >= 3).unwrap_or(false) {
        let mut c = thisis_cache().lock().unwrap();
        if c.len() > 200 {
            c.clear();
        }
        c.insert(ckey, (now_secs(), val.clone()));
    }
    Ok(Json(val))
}

// Mở một album / nghệ sĩ / danh sách phát / podcast từ kết quả tìm kiếm: trả {title, sub, image, tracks}.
#[derive(Deserialize)]
struct YtBrowseQ {
    id: Option<String>,
    kind: Option<String>,
}

async fn yt_browse_music(State(st): State<AppState>, headers: HeaderMap, Query(q): Query<YtBrowseQ>) -> R<Value> {
    let (allow_explicit, use_proxy) = net_prefs(&st, &headers).await;
    let client = pick_client(&st, use_proxy);
    let id: String = q.id.unwrap_or_default().trim().chars().take(80).collect();
    if id.is_empty() || !id.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_') {
        return Err(bad("Mục không hợp lệ"));
    }
    let kind = q.kind.unwrap_or_default();
    let fail = |m: &str| -> ApiErr { (StatusCode::BAD_GATEWAY, m.to_string()) };
    // Danh sách phát thường: đọc bằng đường InnerTube sẵn có của phần nhập playlist.
    if kind == "playlist" {
        let pid = id.strip_prefix("VL").unwrap_or(&id).to_string();
        let (title, mut tracks) = innertube_playlist(&client, &pid).await?;
        if !allow_explicit {
            tracks.retain(|t| !t.get("explicit").and_then(|x| x.as_bool()).unwrap_or(false));
        }
        return Ok(Json(json!({ "title": title, "sub": "Danh sách phát", "image": "", "tracks": tracks })));
    }
    let body = json!({
        "context": { "client": { "clientName": "WEB_REMIX", "clientVersion": "1.20250310.01.00", "hl": "en", "gl": "VN" } },
        "browseId": id,
    });
    let resp = client
        .post("https://music.youtube.com/youtubei/v1/browse?prettyPrint=false")
        .header("origin", "https://music.youtube.com")
        .header("referer", "https://music.youtube.com/")
        .json(&body)
        .timeout(Duration::from_secs(25))
        .send()
        .await
        .map_err(|_| fail("Không kết nối được YouTube Music"))?;
    if !resp.status().is_success() {
        return Err(fail("YouTube Music trả về lỗi"));
    }
    let v: Value = resp.json().await.map_err(|_| fail("YouTube Music trả dữ liệu lạ"))?;
    let header = ["musicResponsiveHeaderRenderer", "musicDetailHeaderRenderer", "musicImmersiveHeaderRenderer", "musicVisualHeaderRenderer", "musicEditablePlaylistDetailHeaderRenderer"]
        .iter()
        .find_map(|k| yt_find(&v, k));
    let title = header.and_then(|h| h.get("title")).map(yt_text).unwrap_or_default();
    let strap = header
        .and_then(|h| h.get("straplineTextOne").or_else(|| h.get("subtitle")))
        .map(yt_text)
        .unwrap_or_default();
    let image = header
        .and_then(|h| yt_find(h, "thumbnails"))
        .and_then(|a| a.as_array())
        .and_then(|a| a.last())
        .and_then(|t| t.get("url"))
        .and_then(|u| u.as_str())
        .unwrap_or("")
        .to_string();
    let mut tracks: Vec<Value> = Vec::new();
    yt_collect(&v, &mut tracks);
    if tracks.is_empty() || kind == "podcast" {
        yt_collect_episodes(&v, &mut tracks);
    }
    let is_album = kind == "album";
    let artist_hdr = if kind == "artist" { title.clone() } else { strap.split(" \u{2022} ").next().unwrap_or("").trim().to_string() };
    for t in tracks.iter_mut() {
        let a = t.get("artist").and_then(|x| x.as_str()).unwrap_or("").to_string();
        // Trang album: cột 2 thường là lượt nghe ("1.2M plays"), không phải tên nghệ sĩ.
        if a.is_empty() || a.to_lowercase().contains("plays") {
            t["artist"] = json!(artist_hdr);
        }
        if is_album && !image.is_empty() {
            t["image"] = json!(image);
        }
    }
    let mut seen: HashSet<String> = HashSet::new();
    tracks.retain(|t| seen.insert(t.get("id").and_then(|x| x.as_str()).unwrap_or("").to_string()));
    if !allow_explicit {
        tracks.retain(|t| !t.get("explicit").and_then(|x| x.as_bool()).unwrap_or(false));
    }
    if tracks.is_empty() {
        return Err(bad("Không mở được mục này"));
    }
    Ok(Json(json!({ "title": title, "sub": strap, "image": image, "tracks": tracks })))
}

// ===== Luồng âm thanh YouTube (để áp dụng Bộ chỉnh âm / Độ trễ / Độ vang bằng Web Audio) =====
// Server lấy địa chỉ luồng âm thanh qua InnerTube rồi chuyển tiếp từng đoạn (Range) cho trình duyệt,
// nhờ đó <audio> cùng nguồn gốc và Web Audio xử lý được (player iframe của YouTube thì không).
#[derive(Deserialize)]
struct AudQ {
    t: Option<String>,
    f: Option<String>,
    debug: Option<String>,
    h: Option<u32>, // chiều cao video yêu cầu (240/360/480/720/1080); không có thì dùng YT_VIDEO_HEIGHT hoặc 360
}

const AUDIO_CHUNK: u64 = 2 * 1024 * 1024;
// Đoạn đầu tăng lên 512 KB để đủ buffer cho kết nối chậm (256 KB hay bị stall trên Render free-tier
// do cold-start + độ trễ cao; 512 KB vẫn lên tiếng trong ~1 giây trên mạng bình thường).
const AUDIO_FIRST_CHUNK: u64 = 512 * 1024;
const AUDIO_TTL: u64 = 3 * 60 * 60;
// Thời gian tối đa cho resolve_race. Mỗi link giờ được tải thử (kể cả thử sâu trong file) và client cần PO token
// bắt đầu muộn hơn, nên cần rộng hơn bản cũ (12s). Vẫn có trần để không bắt người dùng chờ vô hạn.
const AUDIO_RACE_TIMEOUT_SECS: u64 = 30;
// Khi bật plugin wpc (mở Chrome để sinh PO token, mất vài giây mỗi lần) thì cuộc đua cần dài hơn.
fn audio_race_timeout_secs() -> u64 {
    let wpc = std::env::var("YT_WPC").map(|x| matches!(x.trim(), "1" | "true" | "yes")).unwrap_or(false)
        || std::env::var("YT_CHROME").map(|x| !x.trim().is_empty()).unwrap_or(false);
    if wpc { 75 } else { AUDIO_RACE_TIMEOUT_SECS }
}
// So le giữa các client để không bắn cùng lúc (giống NativeYt.kt bên Android).
const AUDIO_STAGGER_MS: u64 = 200;
// Client cần PO token (IOS, TV_SIMPLY) chỉ là dự phòng: chờ thêm để client không cần token kịp trả lời trước.
const AUDIO_LATE_DELAY_MS: u64 = 2500;
// YouTube cho tải ~1MB đầu của link thiếu PO token rồi trả 403 (nhạc "chạy được vài giây rồi dừng").
// Thử tải ở sâu trong file để loại link đó ngay từ đầu.
const AUDIO_DEEP_PROBE: u64 = 1536 * 1024;

// (clientName, header id, clientVersion, user-agent, phần context thêm)
// Danh sách và thứ tự giống NativeYt.kt (Android): thêm ANDROID_VR 1.65.10, IOS 21.26.4, TV_SIMPLY.
fn audio_clients() -> Vec<(&'static str, &'static str, &'static str, &'static str, Value)> {
    let vr = || json!({ "deviceMake": "Oculus", "deviceModel": "Quest 3", "androidSdkVersion": 32, "osName": "Android", "osVersion": "12L" });
    let ios = || json!({ "deviceMake": "Apple", "deviceModel": "iPhone16,2", "osName": "iPhone", "osVersion": "18.3.2.22D82" });
    vec![
        (
            "ANDROID_VR",
            "28",
            "1.60.19",
            "com.google.android.apps.youtube.vr.oculus/1.60.19 (Linux; U; Android 12L; eureka-user Build/SQ3A.220605.009.A1) gzip",
            vr(),
        ),
        (
            "ANDROID_VR",
            "28",
            "1.65.10",
            "com.google.android.apps.youtube.vr.oculus/1.65.10 (Linux; U; Android 12L; eureka-user Build/SQ3A.220605.009.A1) gzip",
            vr(),
        ),
        (
            "IOS",
            "5",
            "21.26.4",
            "com.google.ios.youtube/21.26.4 (iPhone16,2; U; CPU iOS 18_3_2 like Mac OS X;)",
            ios(),
        ),
        (
            "IOS",
            "5",
            "20.10.4",
            "com.google.ios.youtube/20.10.4 (iPhone16,2; U; CPU iOS 18_3_2 like Mac OS X;)",
            ios(),
        ),
        (
            "TVHTML5_SIMPLY",
            "75",
            "1.0",
            "Mozilla/5.0 (ChromiumStylePlatform) Cobalt/Version",
            json!({}),
        ),
    ]
}

// Client hay cần PO token (link có nhưng tải bị 403): chỉ chạy muộn làm dự phòng.
fn audio_client_late(ci: usize) -> bool {
    matches!(audio_clients().get(ci).map(|c| c.0), Some("IOS") | Some("TVHTML5_SIMPLY"))
}

// Xin một đoạn [start, end] bằng header Range, rồi bằng tham số &range= (cách yt-dlp/NewPipe dùng).
// Chỉ đọc khối dữ liệu đầu tiên rồi bỏ, không kéo cả file. Trả về (số byte đọc được, tổng dung lượng nếu có Content-Range).
async fn probe_range(client: &reqwest::Client, url: &str, ua: &str, start: u64, end: u64) -> Option<(usize, Option<u64>)> {
    for via_param in [false, true] {
        let req = if via_param {
            let sep = if url.contains('?') { '&' } else { '?' };
            client.get(format!("{url}{sep}range={start}-{end}"))
        } else {
            client.get(url).header("range", format!("bytes={start}-{end}"))
        };
        let Ok(mut r) = req.header("user-agent", ua).timeout(Duration::from_secs(8)).send().await else { continue };
        let code = r.status().as_u16();
        // Với header Range phải là 206; 200 nghĩa là máy chủ bỏ qua Range và sẽ gửi cả file (chỉ chấp nhận ở cách ?range=).
        if !(code == 206 || (via_param && code == 200)) {
            continue;
        }
        let total = r
            .headers()
            .get("content-range")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.rsplit('/').next())
            .and_then(|x| x.trim().parse::<u64>().ok());
        match r.chunk().await {
            Ok(Some(b)) if !b.is_empty() => return Some((b.len(), total)),
            Ok(_) => return Some((0, total)),
            Err(_) => continue,
        }
    }
    None
}

// Dò một đoạn nhỏ ở sâu trong file (link thiếu PO token thường chết ở đây).
async fn deep_check(client: &reqwest::Client, url: &str, ua: &str, size: u64, at: u64) -> Result<(), String> {
    if at <= 1024 || at + 16 >= size {
        return Ok(());
    }
    match probe_range(client, url, ua, at, at + 15).await {
        Some((m, _)) if m > 0 => Ok(()),
        Some(_) => Err(format!("link trả dữ liệu rỗng ở byte {at}")),
        None => Err(format!("link chỉ tải được phần đầu, bị chặn từ byte {at} (thiếu PO token)")),
    }
}

// Tải thử link trước khi tin nó (giống NativeYt.verified bên Android). Client cần PO token vẫn trả link nhưng link đó 403
// khi tải, hoặc chỉ tải được ~1MB đầu rồi chết. Bản cũ tin link đó nên cuộc đua chọn nhầm client hỏng và nhạc dừng giữa chừng.
// Trả về dung lượng thật của luồng (lấy từ Content-Range nếu client không báo).
async fn verify_link(client: &reqwest::Client, url: &str, ua: &str, size_hint: u64) -> Result<u64, String> {
    // YouTube thường báo sẵn contentLength: biết dung lượng rồi thì chạy CẢ 3 lần dò (đầu, sâu, cuối) cùng lúc
    // thay vì dò đầu xong mới dò tiếp – bớt hẳn một lượt chờ mạng (đáng kể khi IP nhà ở xa máy chủ Google).
    if size_hint > 0 {
        let size = size_hint;
        let (p0, r1, r2) = tokio::join!(
            probe_range(client, url, ua, 0, 1023),
            deep_check(client, url, ua, size, AUDIO_DEEP_PROBE),
            deep_check(client, url, ua, size, size.saturating_sub(64 * 1024))
        );
        let Some((n, _)) = p0 else {
            return Err("link không tải được (403/hết hạn/cần PO token)".to_string());
        };
        if n == 0 {
            return Err("link trả dữ liệu rỗng".to_string());
        }
        r1?;
        r2?;
        return Ok(size);
    }
    // Không có dung lượng báo sẵn: dò đầu để lấy dung lượng từ Content-Range, rồi mới dò sâu (hai lần dò sau chạy song song).
    let Some((n, total)) = probe_range(client, url, ua, 0, 1023).await else {
        return Err("link không tải được (403/hết hạn/cần PO token)".to_string());
    };
    if n == 0 {
        return Err("link trả dữ liệu rỗng".to_string());
    }
    let size = total.unwrap_or(0);
    if size == 0 {
        return Err("không biết dung lượng luồng".to_string());
    }
    let (r1, r2) = tokio::join!(
        deep_check(client, url, ua, size, AUDIO_DEEP_PROBE),
        deep_check(client, url, ua, size, size.saturating_sub(64 * 1024))
    );
    r1?;
    r2?;
    Ok(size)
}

async fn resolve_audio(client: &reqwest::Client, vid: &str, webm: bool, from: usize) -> Result<(String, u64, String, usize), String> {
    let mut last = "Không lấy được luồng âm thanh".to_string();
    for (ci, (name, hid, ver, ua, extra)) in audio_clients().into_iter().enumerate().skip(from).take(1) {
        let mut c = json!({ "clientName": name, "clientVersion": ver, "hl": "en", "gl": "VN" });
        if let (Some(o), Some(e)) = (c.as_object_mut(), extra.as_object()) {
            for (k, v) in e {
                o.insert(k.clone(), v.clone());
            }
        }
        let mut body = json!({ "context": { "client": c }, "videoId": vid, "contentCheckOk": true, "racyCheckOk": true });
        if name == "TVHTML5_SIMPLY_EMBEDDED_PLAYER" {
            body["context"]["thirdParty"] = json!({ "embedUrl": "https://www.youtube.com/" });
        }
        let resp = match client
            .post("https://youtubei.googleapis.com/youtubei/v1/player?prettyPrint=false")
            .header("user-agent", ua)
            .header("x-youtube-client-name", hid)
            .header("x-youtube-client-version", ver)
            .json(&body)
            .send()
            .await
        {
            Ok(r) if r.status().is_success() => r,
            Ok(r) => {
                last = format!("{name} {ver}: YouTube trả về HTTP {}", r.status().as_u16());
                eprintln!("[audio] {vid} {last}");
                continue;
            }
            Err(e) => {
                last = format!("{name} {ver}: không kết nối được YouTube ({e})");
                eprintln!("[audio] {vid} {last}");
                continue;
            }
        };
        let v: Value = match resp.json().await {
            Ok(v) => v,
            Err(e) => {
                last = format!("{name} {ver}: YouTube trả dữ liệu không đọc được ({e})");
                eprintln!("[audio] {vid} {last}");
                continue;
            }
        };
        if v.pointer("/playabilityStatus/status").and_then(|x| x.as_str()) != Some("OK") {
            last = format!(
                "{name} {ver}: {} ({})",
                v.pointer("/playabilityStatus/reason").and_then(|x| x.as_str()).unwrap_or("video không phát được"),
                v.pointer("/playabilityStatus/status").and_then(|x| x.as_str()).unwrap_or("?")
            );
            eprintln!("[audio] {vid} {last}");
            continue;
        }
        let Some(fmts) = v.pointer("/streamingData/adaptiveFormats").and_then(|x| x.as_array()) else {
            last = format!("{name} {ver}: không có danh sách định dạng");
            eprintln!("[audio] {vid} {last}");
            continue;
        };
        // Ưu tiên AAC/MP4 (itag 140): chạy được trên mọi trình duyệt; sau đó là bitrate cao nhất.
        let mut best: Option<(i64, &Value)> = None;
        for f in fmts {
            let mime = f.get("mimeType").and_then(|x| x.as_str()).unwrap_or("");
            if !mime.starts_with("audio/") || f.get("url").and_then(|x| x.as_str()).is_none() {
                continue;
            }
            let mut score = f.get("bitrate").and_then(|x| x.as_i64()).unwrap_or(0) / 1000;
            let itag = f.get("itag").and_then(|x| x.as_i64());
            if webm {
                if mime.starts_with("audio/webm") {
                    score += 100_000;
                }
                if itag == Some(251) {
                    score += 1_000_000;
                }
            } else {
                if mime.starts_with("audio/mp4") {
                    score += 100_000;
                }
                if itag == Some(140) {
                    score += 1_000_000;
                }
            }
            if best.map(|b| score > b.0).unwrap_or(true) {
                best = Some((score, f));
            }
        }
        let Some((_, f)) = best else {
            last = format!("{name} {ver}: không có luồng âm thanh nào có link trực tiếp (YouTube đòi PO token/SABR)");
            eprintln!("[audio] {vid} {last}");
            continue;
        };
        let url = f.get("url").and_then(|x| x.as_str()).unwrap_or("").to_string();
        let size: u64 = f
            .get("contentLength")
            .and_then(|x| x.as_str())
            .and_then(|x| x.parse().ok())
            .or_else(|| f.get("contentLength").and_then(|x| x.as_u64()))
            .unwrap_or(0);
        if url.is_empty() {
            last = format!("{name} {ver}: định dạng không có URL trực tiếp (có thể cần PO token)");
            eprintln!("[audio] {vid} {last}");
            continue;
        }
        let mime = f
            .get("mimeType")
            .and_then(|x| x.as_str())
            .unwrap_or("audio/mp4")
            .split(';')
            .next()
            .unwrap_or("audio/mp4")
            .to_string();
        // Chỉ nhận link thật sự tải được (xem verify_link); link chết thì bỏ để client kế tiếp thắng cuộc đua.
        match verify_link(client, &url, ua, size).await {
            Ok(real_size) => return Ok((url, real_size, mime, ci)),
            Err(e) => {
                last = format!("{name} {ver}: {e}");
                eprintln!("[audio] {vid} {last}");
                continue;
            }
        }
    }
    Err(last)
}

// Thứ tự thử các nguồn: client YouTube -> yt-dlp (90) -> Piped (99).
// Có YT_COOKIES thì yt-dlp (đăng nhập bằng cookies) được thử đầu tiên vì tránh được "Sign in to confirm you're not a bot".
fn audio_order() -> Vec<usize> {
    let mut v: Vec<usize> = (0..audio_clients().len()).collect();
    let has = |k: &str| std::env::var(k).map(|x| !x.trim().is_empty()).unwrap_or(false);
    // yt-dlp chỉ là phương án dự phòng TUỲ CHỌN: có sẵn thì dùng, không có thì bỏ qua (không bắt người dùng cài yt-dlp / Deno).
    let ytdlp_ok = ytdlp_ready() || ytdlp_in_path();
    if ytdlp_ok && (has("YT_COOKIES") || has("YT_BROWSER")) {
        v.insert(0, 90);
    } else if ytdlp_ok {
        v.push(90);
    }
    // Piped trả link gắn với IP của máy chủ Piped chứ không phải IP người dùng (Android cũng đã bỏ) nên ở chế độ IP nhà (desktop/Android) không dùng.
    if !home_ip_mode() {
        v.push(99);
    }
    v
}

// Server đang chạy trên máy người dùng (cổng nhà của desktop, hoặc AUDIO_ONLY của Android): mọi luồng nhạc phải đi bằng IP của họ.
fn home_ip_mode() -> bool {
    std::env::var("GATEWAY_REMOTE").map(|x| !x.trim().is_empty()).unwrap_or(false)
        || std::env::var("AUDIO_ONLY").map(|x| x.trim() == "1").unwrap_or(false)
}

// Có file yt-dlp trong PATH không (không chạy nó, chỉ tìm file).
fn ytdlp_in_path() -> bool {
    let Some(paths) = std::env::var_os("PATH") else { return false };
    std::env::split_paths(&paths).any(|d| d.join("yt-dlp.exe").is_file() || d.join("yt-dlp").is_file())
}

async fn resolve_source(client: &reqwest::Client, vid: &str, webm: bool, src: usize, use_proxy: bool) -> Result<(String, u64, String, usize), String> {
    match src {
        // Link yt-dlp -g hay bị 403 khi server tự tải, nên bỏ bước lấy link: cho yt-dlp tải thẳng file (chỉ chạy yt-dlp 1 lần).
        90 => {
            let _ = client;
            let (_p, total, mime) = ytdlp_local(vid, webm, use_proxy).await?;
            Ok((format!("local:{vid}"), total, mime, 90))
        }
        99 => resolve_piped(client, vid, webm).await,
        n => resolve_audio(client, vid, webm, n).await,
    }
}

// Nhớ 10 phút nguồn nào đã bị chặn cho video này để khỏi thử lại ở mỗi yêu cầu đoạn.
static AUDIO_BAD: std::sync::OnceLock<Mutex<HashMap<(String, usize), u64>>> = std::sync::OnceLock::new();
fn bad_map() -> &'static Mutex<HashMap<(String, usize), u64>> {
    AUDIO_BAD.get_or_init(|| Mutex::new(HashMap::new()))
}
fn audio_bad(id: &str, src: usize) -> bool {
    let m = bad_map().lock().unwrap();
    m.get(&(id.to_string(), src)).map(|t| now_secs().saturating_sub(*t) < 600).unwrap_or(false)
}
fn audio_mark_bad(id: &str, src: usize) {
    let mut m = bad_map().lock().unwrap();
    if m.len() > 2000 {
        m.clear();
    }
    m.insert((id.to_string(), src), now_secs());
}

// Hỏi các client YouTube (ANDROID_VR / TV / IOS ...) gần như cùng lúc, cái nào có link TẢI THỬ ĐƯỢC trước thì dùng.
// Các client bắt đầu so le nhau (AUDIO_STAGGER_MS); client cần PO token (IOS, TV_SIMPLY) chỉ làm dự phòng nên bắt đầu muộn hơn.
// Trước đây thử lần lượt: client đầu bị lỗi là mất thêm 1-2 lượt gọi mạng trước khi nhạc lên tiếng.
async fn resolve_race(client: &reqwest::Client, vid: &str, webm: bool, cands: &[usize]) -> Result<(String, u64, String, usize), String> {
    let mut set = tokio::task::JoinSet::new();
    // Các client "chính" (không phải yt-dlp, không phải client cần PO token). Khi tất cả đều đã báo lỗi thì không cần chờ
    // thêm nữa: client dự phòng và yt-dlp được thả chạy ngay (trước đây vẫn phải ngồi chờ 1,2s–2,5s vô ích).
    let is_early = |ci: usize| ci != 90 && !audio_client_late(ci);
    let early_total = cands.iter().filter(|&&ci| is_early(ci)).count();
    let (go_tx, go_rx) = tokio::sync::watch::channel(early_total == 0);
    let mut early_failed = 0usize;
    for (i, &ci) in cands.iter().enumerate() {
        let c = client.clone();
        let v = vid.to_string();
        let mut rx = go_rx.clone();
        let early = is_early(ci);
        // yt-dlp (nguồn 90) bắt đầu sau ~1,2s: client nhanh (ANDROID_VR) thắng thì khỏi chạy yt-dlp; nếu không thì yt-dlp đã chạy sẵn.
        let wait = if ci == 90 { 1200 } else { i as u64 * AUDIO_STAGGER_MS + if audio_client_late(ci) { AUDIO_LATE_DELAY_MS } else { 0 } };
        set.spawn(async move {
            if wait > 0 {
                if early {
                    tokio::time::sleep(Duration::from_millis(wait)).await;
                } else {
                    tokio::select! {
                        _ = tokio::time::sleep(Duration::from_millis(wait)) => {}
                        _ = async {
                            loop {
                                if *rx.borrow() {
                                    break;
                                }
                                if rx.changed().await.is_err() {
                                    break;
                                }
                            }
                        } => {}
                    }
                }
            }
            if ci == 90 {
                return (ci, resolve_source(&c, &v, webm, 90, false).await);
            }
            (ci, resolve_audio(&c, &v, webm, ci).await)
        });
    }
    // Gom lý do lỗi của TỪNG client (không chỉ cái cuối) để người dùng thấy vì sao bài không phát được.
    let mut errs: Vec<String> = Vec::new();
    let why = |errs: &Vec<String>| if errs.is_empty() { "Không lấy được luồng âm thanh".to_string() } else { errs.join(" | ") };
    let deadline = tokio::time::Instant::now() + Duration::from_secs(audio_race_timeout_secs());
    loop {
        match tokio::time::timeout_at(deadline, set.join_next()).await {
            Ok(Some(joined)) => match joined {
                Ok((_, Ok(x))) => {
                    set.abort_all();
                    return Ok(x);
                }
                Ok((ci, Err(e))) => {
                    eprintln!("[audio] {vid} nguồn {ci}: {e}");
                    audio_mark_bad(vid, ci);
                    errs.push(e);
                    if is_early(ci) {
                        early_failed += 1;
                        if early_failed >= early_total {
                            let _ = go_tx.send(true);
                        }
                    }
                }
                Err(_) => {}
            },
            Ok(None) => break, // tất cả task đã xong
            Err(_) => {
                // Hết thời gian AUDIO_RACE_TIMEOUT_SECS: huỷ các task còn lại, trả lỗi ngay.
                set.abort_all();
                return Err(format!("hết thời gian chờ ({}s): {}", audio_race_timeout_secs(), why(&errs)));
            }
        }
    }
    Err(why(&errs))
}

// Xoá dấu "đã hỏng" của các client YouTube (không đụng yt-dlp/Piped) cho một bài.
// Dùng khi MỌI client đều đang bị đánh dấu hỏng: thay vì báo lỗi ngay suốt 10 phút, cho thử lại tất cả từ đầu (giống NativeYt.kt).
fn audio_clear_bad(id: &str) {
    let mut m = bad_map().lock().unwrap();
    m.retain(|(vid, src), _| !(vid == id && *src < 90));
}

// Mỗi bài chỉ cho một yêu cầu đi lấy luồng tại một thời điểm: trình duyệt hay gửi nhiều yêu cầu Range cùng lúc
// (kèm yêu cầu "làm nóng" trước đó), các yêu cầu sau chờ rồi dùng kết quả đã có trong cache thay vì lặp lại việc lấy luồng.
static AUDIO_GATES: std::sync::OnceLock<Mutex<HashMap<String, std::sync::Arc<tokio::sync::Mutex<()>>>>> = std::sync::OnceLock::new();
fn audio_gate(key: &str) -> std::sync::Arc<tokio::sync::Mutex<()>> {
    let mut m = AUDIO_GATES.get_or_init(|| Mutex::new(HashMap::new())).lock().unwrap();
    if m.len() > 500 {
        m.clear();
    }
    m.entry(key.to_string()).or_insert_with(|| std::sync::Arc::new(tokio::sync::Mutex::new(()))).clone()
}

// yt-dlp (cài riêng, đặt trong PATH hoặc biến YTDLP_PATH). YT_COOKIES = đường dẫn file cookies.txt (định dạng Netscape).
fn ytdlp_fmt(webm: bool) -> &'static str {
    if webm {
        "251/bestaudio[ext=webm]/bestaudio"
    } else {
        "140/bestaudio[ext=m4a]/bestaudio"
    }
}

// Lệnh yt-dlp dùng chung (đường dẫn, Deno, cookies, proxy) cho cả lấy link lẫn tải file.
// yt-dlp đi kèm app desktop (nằm cạnh muse-server): ưu tiên YTDLP_PATH, rồi file cạnh chương trình, cuối cùng mới là PATH.
fn ytdlp_bundled() -> Option<PathBuf> {
    let exe = std::env::current_exe().ok()?;
    let dir = exe.parent()?;
    ["yt-dlp.exe", "yt-dlp"].iter().map(|n| dir.join(n)).find(|p| p.is_file())
}

// Có yt-dlp dùng được ngay không (YTDLP_PATH hoặc bản đi kèm app) → cho yt-dlp chạy song song với các client để nhạc lên nhanh.
fn ytdlp_ready() -> bool {
    std::env::var("YTDLP_PATH").map(|x| !x.trim().is_empty()).unwrap_or(false) || ytdlp_bundled().is_some()
}

fn ytdlp_base(use_proxy: bool) -> tokio::process::Command {
    let bin = std::env::var("YTDLP_PATH")
        .ok()
        .filter(|x| !x.trim().is_empty())
        .or_else(|| ytdlp_bundled().map(|p| p.to_string_lossy().into_owned()))
        .unwrap_or_else(|| "yt-dlp".to_string());
    let mut cmd = tokio::process::Command::new(&bin);
    cmd.arg("--no-playlist").arg("--no-warnings");
    // yt-dlp cần JS runtime (Deno) để giải mã YouTube: nếu deno.exe nằm cạnh server thì chỉ thẳng đường dẫn, khỏi phụ thuộc PATH.
    {
        let mut cands: Vec<std::path::PathBuf> = vec![std::path::PathBuf::from("deno.exe"), std::path::PathBuf::from("deno")];
        if let Ok(e) = std::env::current_exe() {
            if let Some(d) = e.parent() {
                cands.insert(0, d.join("deno.exe"));
            }
        }
        if let Some(f) = cands.into_iter().find(|f| f.is_file()) {
            if let Ok(abs) = std::fs::canonicalize(&f) {
                let p = abs.to_string_lossy().trim_start_matches(r"\\?\").to_string();
                cmd.arg("--js-runtimes").arg(format!("deno:{p}"));
            }
        }
    }
    // PO token: plugin bgutil (HTTP server, mặc định http://127.0.0.1:4416) và plugin wpc (Chrome).
    // YT_POT_URL = địa chỉ provider bgutil khi không nằm ở mặc định (vd. http://bgutil:4416 trong docker-compose).
    if let Ok(u) = std::env::var("YT_POT_URL") {
        let u = u.trim().to_string();
        if !u.is_empty() {
            cmd.arg("--extractor-args").arg(format!("youtubepot-bgutilhttp:base_url={u}"));
        }
    }
    // YT_CHROME = đường dẫn Chrome/Chromium cho plugin wpc (không đặt thì plugin tự tìm).
    if let Ok(c) = std::env::var("YT_CHROME") {
        let c = c.trim().to_string();
        if !c.is_empty() {
            cmd.arg("--extractor-args").arg(format!("youtubepot-wpc:browser_path={c}"));
        }
    }
    if let Ok(c) = std::env::var("YT_COOKIES") {
        let c = c.trim().to_string();
        if !c.is_empty() {
            cmd.arg("--cookies").arg(c);
        }
    }
    if let Ok(b) = std::env::var("YT_BROWSER") {
        // lấy cookies trực tiếp từ trình duyệt trên máy (firefox, edge, chrome, brave...), khỏi phải xuất file
        let b = b.trim().to_string();
        if !b.is_empty() {
            cmd.arg("--cookies-from-browser").arg(b);
        }
    }
    if use_proxy {
        if let Ok(p) = std::env::var("YT_PROXY") {
            let p = p.trim().to_string();
            if !p.is_empty() {
                cmd.arg("--proxy").arg(p);
            }
        }
    }
    cmd.kill_on_drop(true);
    #[cfg(windows)]
    cmd.creation_flags(0x0800_0000); // không bật cửa sổ console
    cmd
}

#[allow(dead_code)]
async fn resolve_ytdlp(client: &reqwest::Client, vid: &str, webm: bool, use_proxy: bool) -> Result<(String, u64, String, usize), String> {
    let mut cmd = ytdlp_base(use_proxy);
    cmd.arg("-g").arg("-f").arg(ytdlp_fmt(webm));
    cmd.arg(format!("https://www.youtube.com/watch?v={vid}"));
    let out = tokio::time::timeout(Duration::from_secs(45), cmd.output())
        .await
        .map_err(|_| "yt-dlp: quá thời gian".to_string())?
        .map_err(|e| format!("yt-dlp: không chạy được ({e}) – cài yt-dlp hoặc đặt YTDLP_PATH"))?;
    if !out.status.success() {
        let err = String::from_utf8_lossy(&out.stderr);
        let line: String = err.lines().rev().find(|l| !l.trim().is_empty()).unwrap_or("lỗi").chars().take(200).collect();
        return Err(format!("yt-dlp: {line}"));
    }
    let text = String::from_utf8_lossy(&out.stdout).to_string();
    let url = text.lines().map(|l| l.trim()).find(|l| l.starts_with("http")).ok_or_else(|| "yt-dlp: không trả về link".to_string())?.to_string();
    let q = |k: &str| -> Option<String> { url.split(['?', '&']).find_map(|p| p.strip_prefix(k).map(|x| x.to_string())) };
    let mut size: u64 = q("clen=").and_then(|x| x.parse().ok()).unwrap_or(0);
    let mime = q("mime=").map(|m| m.replace("%2F", "/").replace("%2f", "/")).unwrap_or_else(|| "audio/mp4".to_string());
    if size == 0 {
        if let Ok(r) = client.get(&url).header("range", "bytes=0-0").send().await {
            size = r
                .headers()
                .get("content-range")
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.rsplit('/').next())
                .and_then(|x| x.parse::<u64>().ok())
                .unwrap_or(0);
        }
    }
    if size == 0 {
        return Err("yt-dlp: không đọc được kích thước luồng".to_string());
    }
    Ok((url, size, mime, 90))
}

// Link yt-dlp trả về (-g) gắn với client / header / PO token của chính yt-dlp, nên khi server tự tải bằng reqwest
// thì googlevideo hay trả 403. Cách chắc nhất: cho yt-dlp tải file âm thanh về thư mục tạm rồi phát từ file đó.
static YTDLP_DL_LOCK: std::sync::OnceLock<tokio::sync::Mutex<()>> = std::sync::OnceLock::new();

fn audio_cache_dir() -> PathBuf {
    std::env::temp_dir().join("muse-audio")
}

fn find_local_audio(vid: &str, webm: bool) -> Option<(PathBuf, u64, String)> {
    let prefix = format!("{vid}_{}.", if webm { "w" } else { "m" });
    for e in std::fs::read_dir(audio_cache_dir()).ok()?.flatten() {
        let name = e.file_name().to_string_lossy().to_string();
        if !name.starts_with(&prefix) {
            continue;
        }
        let ext = name[prefix.len()..].to_lowercase();
        if ext.contains("part") || ext.contains("ytdl") || ext.contains("temp") {
            continue; // file đang tải dở
        }
        let Ok(md) = e.metadata() else { continue };
        if md.len() == 0 {
            continue;
        }
        let mime = match ext.as_str() {
            "webm" => "audio/webm",
            "opus" | "ogg" => "audio/ogg",
            _ => "audio/mp4",
        };
        return Some((e.path(), md.len(), mime.to_string()));
    }
    None
}

async fn ytdlp_local(vid: &str, webm: bool, use_proxy: bool) -> Result<(PathBuf, u64, String), String> {
    if let Some(x) = find_local_audio(vid, webm) {
        return Ok(x);
    }
    // Chỉ cho 1 lượt tải tại một thời điểm (trình duyệt gửi nhiều yêu cầu Range cùng lúc cho một bài).
    let lock = YTDLP_DL_LOCK.get_or_init(|| tokio::sync::Mutex::new(()));
    let _guard = lock.lock().await;
    if let Some(x) = find_local_audio(vid, webm) {
        return Ok(x);
    }
    let dir = audio_cache_dir();
    let _ = tokio::fs::create_dir_all(&dir).await;
    // Dọn file cũ hơn 6 giờ.
    if let Ok(rd) = std::fs::read_dir(&dir) {
        for e in rd.flatten() {
            let old = e
                .metadata()
                .ok()
                .and_then(|m| m.modified().ok())
                .and_then(|t| t.elapsed().ok())
                .map(|d| d.as_secs() > 6 * 3600)
                .unwrap_or(false);
            if old {
                let _ = std::fs::remove_file(e.path());
            }
        }
    }
    let template = dir.join(format!("{vid}_{}.%(ext)s", if webm { "w" } else { "m" }));
    let mut cmd = ytdlp_base(use_proxy);
    cmd.arg("-f").arg(ytdlp_fmt(webm)).arg("--no-mtime").arg("--no-progress").arg("-o").arg(&template);
    cmd.arg(format!("https://www.youtube.com/watch?v={vid}"));
    let out = tokio::time::timeout(Duration::from_secs(120), cmd.output())
        .await
        .map_err(|_| "yt-dlp: tải file quá thời gian".to_string())?
        .map_err(|e| format!("yt-dlp: không chạy được ({e}) – cài yt-dlp hoặc đặt YTDLP_PATH"))?;
    if !out.status.success() {
        let err = String::from_utf8_lossy(&out.stderr);
        let line: String = err.lines().rev().find(|l| !l.trim().is_empty()).unwrap_or("lỗi").chars().take(200).collect();
        return Err(format!("yt-dlp (tải file): {line}"));
    }
    find_local_audio(vid, webm).ok_or_else(|| "yt-dlp: tải xong nhưng không thấy file".to_string())
}

// Trả đoạn Range từ file cục bộ.
async fn serve_local(path: &PathBuf, total: u64, mime: String, start: u64, end_req: Option<u64>) -> Result<Response, String> {
    use tokio::io::{AsyncReadExt, AsyncSeekExt};
    if start >= total {
        return Ok((StatusCode::RANGE_NOT_SATISFIABLE, [(header::CONTENT_RANGE, format!("bytes */{total}"))]).into_response());
    }
    let end = end_req.unwrap_or(total - 1).min(total - 1).min(start + AUDIO_CHUNK - 1);
    let n = (end - start + 1) as usize;
    let mut f = tokio::fs::File::open(path).await.map_err(|e| format!("mở file lỗi: {e}"))?;
    f.seek(std::io::SeekFrom::Start(start)).await.map_err(|e| format!("seek lỗi: {e}"))?;
    let mut buf = vec![0u8; n];
    f.read_exact(&mut buf).await.map_err(|e| format!("đọc file lỗi: {e}"))?;
    Ok(Response::builder()
        .status(StatusCode::PARTIAL_CONTENT)
        .header(header::CONTENT_TYPE, mime)
        .header(header::ACCEPT_RANGES, "bytes")
        .header(header::CONTENT_RANGE, format!("bytes {start}-{end}/{total}"))
        .header(header::CONTENT_LENGTH, n.to_string())
        .header(header::CACHE_CONTROL, "private, max-age=600")
        .body(Body::from(buf))
        .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response()))
}

// ===== Video phát bằng thẻ <video> của app (không dùng khung YouTube) =====
// yt-dlp tải luồng video (không âm thanh, tối đa YT_VIDEO_HEIGHT, mặc định 480p) về thư mục tạm rồi phát lại theo Range.
// Âm thanh vẫn do engine âm thanh hiện có phát, phía trình duyệt chỉ đồng bộ hình theo thời gian của âm thanh.
// Mỗi video một khóa riêng: tải sẵn bài kế tiếp không chặn bài đang phát (trước đây dùng 1 khóa chung nên phải chờ nhau).
static YTDLP_VID_LOCKS: std::sync::OnceLock<std::sync::Mutex<std::collections::HashMap<String, std::sync::Arc<tokio::sync::Mutex<()>>>>> = std::sync::OnceLock::new();

fn find_local_video(vid: &str, h: u32) -> Option<(PathBuf, u64, String)> {
    let prefix = format!("{vid}_v{h}."); // mỗi độ phân giải một file riêng
    for e in std::fs::read_dir(audio_cache_dir()).ok()?.flatten() {
        let name = e.file_name().to_string_lossy().to_string();
        if !name.starts_with(&prefix) {
            continue;
        }
        let ext = name[prefix.len()..].to_lowercase();
        if ext.contains("part") || ext.contains("ytdl") || ext.contains("temp") {
            continue; // file đang tải dở
        }
        let Ok(md) = e.metadata() else { continue };
        if md.len() == 0 {
            continue;
        }
        let mime = if ext == "webm" { "video/webm" } else { "video/mp4" };
        return Some((e.path(), md.len(), mime.to_string()));
    }
    None
}

// Quy yêu cầu về các mức cố định để không sinh ra quá nhiều file cache khác nhau.
fn video_height(req: Option<u32>) -> u32 {
    let def = std::env::var("YT_VIDEO_HEIGHT")
        .ok()
        .and_then(|x| x.trim().parse::<u32>().ok())
        .filter(|n| *n >= 144 && *n <= 2160)
        .unwrap_or(360);
    match req.unwrap_or(def) {
        0..=240 => 240,
        241..=360 => 360,
        361..=480 => 480,
        481..=720 => 720,
        _ => 1080,
    }
}

async fn ytdlp_video(vid: &str, use_proxy: bool, h: u32) -> Result<(PathBuf, u64, String), String> {
    if let Some(x) = find_local_video(vid, h) {
        return Ok(x);
    }
    let lock = {
        let m = YTDLP_VID_LOCKS.get_or_init(|| std::sync::Mutex::new(std::collections::HashMap::new()));
        let mut g = m.lock().unwrap();
        g.entry(format!("{vid}_{h}")).or_insert_with(|| std::sync::Arc::new(tokio::sync::Mutex::new(()))).clone()
    };
    let _guard = lock.lock().await;
    if let Some(x) = find_local_video(vid, h) {
        return Ok(x);
    }
    let dir = audio_cache_dir();
    let _ = tokio::fs::create_dir_all(&dir).await;
    // Dọn file cũ hơn 6 giờ.
    if let Ok(rd) = std::fs::read_dir(&dir) {
        for e in rd.flatten() {
            let old = e
                .metadata()
                .ok()
                .and_then(|m| m.modified().ok())
                .and_then(|t| t.elapsed().ok())
                .map(|d| d.as_secs() > 6 * 3600)
                .unwrap_or(false);
            if old {
                let _ = std::fs::remove_file(e.path());
            }
        }
    }
    let fmt = format!("bv[height<={h}][ext=mp4][vcodec^=avc1]/bv[height<={h}][ext=mp4]/bv[height<={h}]/b[height<={h}]/bv/b");
    let template = dir.join(format!("{vid}_v{h}.%(ext)s"));
    let mut cmd = ytdlp_base(use_proxy);
    cmd.arg("-f").arg(fmt).arg("--no-mtime").arg("--no-progress").arg("-N").arg("4").arg("--socket-timeout").arg("15").arg("-o").arg(&template);
    cmd.arg(format!("https://www.youtube.com/watch?v={vid}"));
    let out = tokio::time::timeout(Duration::from_secs(120), cmd.output())
        .await
        .map_err(|_| "yt-dlp: tải video quá thời gian".to_string())?
        .map_err(|e| format!("yt-dlp: không chạy được ({e}) – cài yt-dlp hoặc đặt YTDLP_PATH"))?;
    if !out.status.success() {
        let err = String::from_utf8_lossy(&out.stderr);
        let line: String = err.lines().rev().find(|l| !l.trim().is_empty()).unwrap_or("lỗi").chars().take(200).collect();
        return Err(format!("yt-dlp (tải video): {line}"));
    }
    find_local_video(vid, h).ok_or_else(|| "yt-dlp: tải xong nhưng không thấy file video".to_string())
}

// GET /api/yt/video/:id?t=token  -> video (không có âm thanh), hỗ trợ Range
async fn yt_video(State(st): State<AppState>, headers: HeaderMap, Path(id): Path<String>, Query(q): Query<AudQ>) -> Response {
    let tok = q.t.clone().or_else(|| bearer(&headers));
    let uid = {
        let db = st.db.lock().unwrap();
        tok.and_then(|t| user_from_token(&db, &t)).map(|u| u.id)
    };
    if id.len() != 11 || !id.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_') {
        return StatusCode::BAD_REQUEST.into_response();
    }
    let use_proxy = match uid {
        Some(uid) => user_net_prefs(&st, uid).1,
        None => false,
    };
    let (start, end_req) = parse_range(&headers);
    let h = video_height(q.h);
    match ytdlp_video(&id, use_proxy, h).await {
        Ok((path, total, mime)) => match serve_local(&path, total, mime, start, end_req).await {
            Ok(r) => r,
            Err(e) => {
                eprintln!("[video] {id}: {e}");
                StatusCode::BAD_GATEWAY.into_response()
            }
        },
        Err(e) => {
            eprintln!("[video] {id}: {e}");
            (StatusCode::BAD_GATEWAY, e).into_response()
        }
    }
}

fn ua_for(ci: usize) -> &'static str {
    audio_clients().get(ci).map(|c| c.3).unwrap_or("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36")
}

// Máy chủ Piped (mã nguồn mở) trả về link luồng âm thanh đã được proxy, không bị khoá theo IP của bạn.
// Đặt PIPED_API="https://a,https://b" để dùng máy chủ riêng; mặc định thử vài máy chủ công cộng.
async fn resolve_piped(client: &reqwest::Client, vid: &str, webm: bool) -> Result<(String, u64, String, usize), String> {
    let bases: Vec<String> = std::env::var("PIPED_API")
        .ok()
        .map(|v| v.split(',').map(|x| x.trim().trim_end_matches('/').to_string()).filter(|x| !x.is_empty()).collect::<Vec<String>>())
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| {
            vec![
                "https://pipedapi.kavin.rocks".to_string(),
                "https://api.piped.private.coffee".to_string(),
                "https://pipedapi.adminforge.de".to_string(),
            ]
        });
    let mut last = String::from("Piped: không có máy chủ nào dùng được");
    for base in bases {
        let resp = match client.get(format!("{base}/streams/{vid}")).timeout(Duration::from_secs(15)).send().await {
            Ok(r) if r.status().is_success() => r,
            Ok(r) => {
                last = format!("Piped {base}: HTTP {}", r.status().as_u16());
                eprintln!("[audio] {vid} {last}");
                continue;
            }
            Err(e) => {
                last = format!("Piped {base}: {e}");
                eprintln!("[audio] {vid} {last}");
                continue;
            }
        };
        let v: Value = match resp.json().await {
            Ok(v) => v,
            Err(_) => continue,
        };
        let Some(list) = v.get("audioStreams").and_then(|x| x.as_array()) else {
            last = format!("Piped {base}: không có audioStreams");
            continue;
        };
        let mut best: Option<(i64, &Value)> = None;
        for f in list {
            let mime = f.get("mimeType").and_then(|x| x.as_str()).unwrap_or("");
            if !mime.starts_with("audio/") || f.get("url").and_then(|x| x.as_str()).is_none() {
                continue;
            }
            let mut score = f.get("bitrate").and_then(|x| x.as_i64()).unwrap_or(0) / 1000;
            if (webm && mime.starts_with("audio/webm")) || (!webm && mime.starts_with("audio/mp4")) {
                score += 100_000;
            }
            if best.map(|b| score > b.0).unwrap_or(true) {
                best = Some((score, f));
            }
        }
        let Some((_, f)) = best else { continue };
        let url = f.get("url").and_then(|x| x.as_str()).unwrap_or("").to_string();
        let mime = f.get("mimeType").and_then(|x| x.as_str()).unwrap_or("audio/mp4").split(';').next().unwrap_or("audio/mp4").to_string();
        let mut size: u64 = f.get("contentLength").and_then(|x| x.as_i64()).filter(|n| *n > 0).map(|n| n as u64).unwrap_or(0);
        if size == 0 {
            // Piped không báo kích thước: hỏi 1 byte đầu để đọc tổng từ Content-Range
            if let Ok(r) = client.get(&url).header("range", "bytes=0-0").send().await {
                size = r
                    .headers()
                    .get("content-range")
                    .and_then(|v| v.to_str().ok())
                    .and_then(|v| v.rsplit('/').next())
                    .and_then(|x| x.parse::<u64>().ok())
                    .unwrap_or(0);
            }
        }
        if url.is_empty() || size == 0 {
            last = format!("Piped {base}: không đọc được kích thước luồng");
            continue;
        }
        return Ok((url, size, mime, 99));
    }
    Err(last)
}

fn parse_range(h: &HeaderMap) -> (u64, Option<u64>) {
    let Some(v) = h.get("range").and_then(|x| x.to_str().ok()) else { return (0, None) };
    let Some(r) = v.strip_prefix("bytes=") else { return (0, None) };
    let r = r.split(',').next().unwrap_or("");
    let mut it = r.splitn(2, '-');
    let a = it.next().unwrap_or("").trim().parse::<u64>().unwrap_or(0);
    let b = it.next().and_then(|x| x.trim().parse::<u64>().ok());
    (a, b)
}

async fn yt_audio(State(st): State<AppState>, headers: HeaderMap, Path(id): Path<String>, Query(q): Query<AudQ>) -> Response {
    // <audio> không gửi được header Authorization nên nhận token qua ?t=
    let tok = q.t.clone().or_else(|| bearer(&headers));
    let uid = {
        let db = st.db.lock().unwrap();
        tok.and_then(|t| user_from_token(&db, &t)).map(|u| u.id)
    };
    // Không bắt buộc đăng nhập: khách (không có token / token hết hạn) vẫn nghe được, dùng cài đặt mặc định.
    if id.len() != 11 || !id.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_') {
        return StatusCode::BAD_REQUEST.into_response();
    }
    let use_proxy = match uid {
        Some(uid) => user_net_prefs(&st, uid).1,
        None => false,
    };
    let client = pick_client(&st, use_proxy);
    let webm = q.f.as_deref() == Some("webm");
    let ckey = format!("{id}|{}|{}", use_proxy as u8, webm as u8);
    let (start, end_req) = parse_range(&headers);
    let mut notes = String::new();

    // Thử lần lượt các nguồn (client YouTube -> yt-dlp -> Piped); nguồn nào bị chặn thì bỏ qua và thử nguồn kế tiếp.
    let order = audio_order();
    let mut pos = 0usize;
    // Mỗi nguồn được thử lại 1 lần (lấy link mới) trước khi bị coi là hỏng: link googlevideo hay hết hạn / lỗi thoáng qua.
    let mut retried: HashSet<usize> = HashSet::new();
    for _round in 0..=order.len() * 2 {
        let cached = {
            let c = st.audio.lock().unwrap();
            c.get(&ckey).filter(|e| now_secs().saturating_sub(e.3) < AUDIO_TTL).cloned()
        };
        let (url, size, mime, ci) = match cached {
            Some((u, s, m, _, ci)) => (u, s, m, ci),
            None => {
                // Chỉ một yêu cầu cho mỗi bài đi lấy luồng; yêu cầu đến sau chờ rồi dùng luôn kết quả trong cache.
                let gate = audio_gate(&ckey);
                let _gate_guard = gate.lock().await;
                let mut res = {
                    let c = st.audio.lock().unwrap();
                    c.get(&ckey).filter(|e| now_secs().saturating_sub(e.3) < AUDIO_TTL).map(|(u, s, m, _, ci)| (u.clone(), *s, m.clone(), *ci))
                };
                // Hỏi song song các client YouTube thay vì lần lượt: nhạc lên tiếng nhanh hơn khi client đầu bị chặn/chậm.
                if res.is_none() && pos == 0 {
                    // Có yt-dlp đi kèm (app desktop) thì cho nó chạy song song trong cuộc đua, không đợi các client chết hết rồi mới thử.
                    let ytdlp_in_race = ytdlp_ready();
                    let pick = |s: &usize| *s < 90 || (ytdlp_in_race && *s == 90);
                    let mut cands: Vec<usize> = order.iter().copied().filter(|s| pick(s) && !audio_bad(&id, *s)).collect();
                    if cands.is_empty() {
                        // Mọi client đều từng lỗi ở bài này (có thể chỉ là lỗi thoáng qua): cho thử lại tất cả từ đầu
                        // thay vì báo "bị YouTube chặn" suốt 10 phút.
                        audio_clear_bad(&id);
                        cands = order.iter().copied().filter(|s| pick(s)).collect();
                    }
                    if cands.len() > 1 {
                        let dbg_tr = std::time::Instant::now();
                        let dbg_r = resolve_race(&client, &id, webm, &cands).await;
                        match &dbg_r {
                            Ok(x) => dbg_log(&format!("[AUDIO] id={id} cache=MISS race_ok nguồn_thắng={} resolve={}ms ứng_viên={:?}", x.3, dbg_tr.elapsed().as_millis(), cands)),
                            Err(e) => dbg_log(&format!("[AUDIO] id={id} cache=MISS race_FAIL resolve={}ms ứng_viên={:?} lỗi={e}", dbg_tr.elapsed().as_millis(), cands)),
                        }
                        match dbg_r {
                            Ok(x) => res = Some(x),
                            Err(e) => {
                                notes.push_str(&format!("[{e}] "));
                                pos = order.iter().position(|s| *s >= 90).unwrap_or(order.len());
                            }
                        }
                    }
                }
                while res.is_none() && pos < order.len() {
                    let src = order[pos];
                    if audio_bad(&id, src) {
                        pos += 1;
                        continue;
                    }
                    let dbg_ts = std::time::Instant::now();
                    match resolve_source(&client, &id, webm, src, use_proxy).await {
                        Ok(x) => {
                            dbg_log(&format!("[AUDIO] id={id} nguồn={src} ok resolve={}ms", dbg_ts.elapsed().as_millis()));
                            res = Some(x);
                            break;
                        }
                        Err(e) => {
                            dbg_log(&format!("[AUDIO] id={id} nguồn={src} LỖI resolve={}ms {e}", dbg_ts.elapsed().as_millis()));
                            notes.push_str(&format!("[{e}] "));
                            audio_mark_bad(&id, src);
                            pos += 1;
                        }
                    }
                }
                match res {
                    Some((u, s, m, ci)) => {
                        let mut c = st.audio.lock().unwrap();
                        if c.len() > 300 {
                            c.clear();
                        }
                        c.insert(ckey.clone(), (u.clone(), s, m.clone(), now_secs(), ci));
                        (u, s, m, ci)
                    }
                    None => {
                        if notes.is_empty() {
                            notes = "mọi nguồn âm thanh đang bị YouTube chặn, thử lại sau vài phút".to_string();
                        }
                        return (StatusCode::BAD_GATEWAY, notes).into_response();
                    }
                }
            }
        };
        pos = order.iter().position(|x| *x == ci).map(|p| p + 1).unwrap_or(order.len());
        let ua = ua_for(ci);
        if q.debug.is_some() {
            // Chẩn đoán: tải thử 64 byte đầu và báo trạng thái/định dạng
            let up = client.get(&url).header("user-agent", ua).header("range", "bytes=0-63").send().await;
            let info = match up {
                Ok(r) => {
                    let st_code = r.status().as_u16();
                    let hv = |k: &str| r.headers().get(k).and_then(|v| v.to_str().ok()).unwrap_or("").to_string();
                    let (ct, cr) = (hv("content-type"), hv("content-range"));
                    match r.bytes().await {
                        Ok(b) => {
                            let hex: String = b.iter().take(16).map(|x| format!("{x:02x}")).collect();
                            format!(
                                "client={ci} fmt={} mime={mime} size={size} up={st_code} ct={ct} cr={cr} n={} head={hex}",
                                if webm { "webm" } else { "mp4" },
                                b.len()
                            )
                        }
                        Err(e) => format!("client={ci} up={st_code} lỗi đọc dữ liệu: {e}"),
                    }
                }
                Err(e) => format!("lỗi kết nối googlevideo: {e}"),
            };
            eprintln!("[audio] debug {id}: {info}");
            return (StatusCode::OK, info).into_response();
        }
        // Bài này đã được yt-dlp tải về file tạm (do link trực tiếp bị 403) thì phát thẳng từ file.
        if ci == 90 {
            if let Some((p, total, lm)) = find_local_audio(&id, webm) {
                if let Ok(r) = serve_local(&p, total, lm, start, end_req).await {
                    return r;
                }
            }
        }
        if start >= size {
            return (StatusCode::RANGE_NOT_SATISFIABLE, [(header::CONTENT_RANGE, format!("bytes */{size}"))]).into_response();
        }
        let last = size - 1;
        let chunk = if start == 0 { AUDIO_FIRST_CHUNK } else { AUDIO_CHUNK };
        let end = end_req.unwrap_or(last).min(last).min(start + chunk - 1);

        // Thử 2 cách xin đoạn: header Range, rồi tham số ?range= (một số link chỉ nhận cách này)
        let mut got: Option<Bytes> = None;
        for variant in 0..3 {
            let req = if variant == 0 {
                client.get(&url).header("range", format!("bytes={start}-{end}"))
            } else if variant == 2 {
                // googlevideo đôi khi chỉ chặn đoạn lớn: thử lại với đoạn 256KB
                let e2 = end.min(start + 256 * 1024 - 1);
                client.get(&url).header("range", format!("bytes={start}-{e2}"))
            } else {
                let sep = if url.contains('?') { '&' } else { '?' };
                client.get(format!("{url}{sep}range={start}-{end}"))
            };
            match req.header("user-agent", ua).timeout(Duration::from_secs(15)).send().await {
                Ok(r) if r.status().is_success() => {
                    let partial = r.status().as_u16() == 206 || variant == 1;
                    match r.bytes().await {
                        Ok(b) if !b.is_empty() => {
                            // Nếu YouTube bỏ qua Range và trả cả file (HTTP 200) thì tự cắt đúng đoạn cần
                            got = Some(if partial {
                                b
                            } else {
                                let s0 = (start as usize).min(b.len());
                                let e0 = ((end as usize) + 1).min(b.len());
                                b.slice(s0..e0)
                            });
                            break;
                        }
                        _ => notes.push_str(&format!("[client {ci} v{variant}: dữ liệu rỗng/đứt] ")),
                    }
                }
                Ok(r) => notes.push_str(&format!("[client {ci} v{variant}: HTTP {}] ", r.status().as_u16())),
                Err(e) => notes.push_str(&format!("[client {ci} v{variant}: {e}] ")),
            }
        }
        // Link do yt-dlp trả về bị googlevideo từ chối (403): cho yt-dlp tự tải file về rồi phát từ file.
        if got.is_none() && ci == 90 {
            eprintln!("[audio] {id} link yt-dlp bị từ chối, chuyển sang để yt-dlp tải file về...");
            match ytdlp_local(&id, webm, use_proxy).await {
                Ok((p, total, lm)) => match serve_local(&p, total, lm, start, end_req).await {
                    Ok(r) => return r,
                    Err(e) => notes.push_str(&format!("[{e}] ")),
                },
                Err(e) => {
                    eprintln!("[audio] {id} {e}");
                    notes.push_str(&format!("[{e}] "));
                }
            }
        }
        if let Some(b) = got {
            let len = b.len() as u64;
            if len == 0 {
                return (StatusCode::BAD_GATEWAY, "googlevideo trả về đoạn rỗng").into_response();
            }
            return Response::builder()
                .status(StatusCode::PARTIAL_CONTENT)
                .header(header::CONTENT_TYPE, mime)
                .header(header::ACCEPT_RANGES, "bytes")
                .header(header::CONTENT_RANGE, format!("bytes {start}-{}/{size}", start + len - 1))
                .header(header::CONTENT_LENGTH, len.to_string())
                // Tăng cache-control lên 1 giờ: link googlevideo sống ~6h, client có thể
                // tua lại mà không cần hỏi server lại. Vẫn private để không bị CDN cache chung.
                .header(header::CACHE_CONTROL, "private, max-age=3600")
                .body(Body::from(b))
                .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response());
        }
        eprintln!("[audio] {id} googlevideo từ chối đoạn {start}-{end}: {notes}");
        st.audio.lock().unwrap().remove(&ckey);
        if ci < 90 && retried.insert(ci) {
            // thử lại cùng nguồn với link mới trước khi loại nó (và tránh nhảy sang nguồn khác giữa chừng bài)
            pos = order.iter().position(|x| *x == ci).unwrap_or(0);
            continue;
        }
        audio_mark_bad(&id, ci);
    }
    (StatusCode::BAD_GATEWAY, format!("googlevideo từ chối: {notes}")).into_response()
}


// ===== Nhập playlist YouTube công khai bằng link (không cần đăng nhập) =====
// Dùng InnerTube (giống lúc tìm nhạc). Chỉ đọc được playlist công khai / không công khai (có link); playlist riêng tư thì không.
fn yt_playlist_id(input: &str) -> Option<String> {
    let s = input.trim();
    let ok = |x: &str| !x.is_empty() && x.len() <= 64 && x.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_');
    if let Some(i) = s.find("list=") {
        let rest = &s[i + 5..];
        let id: String = rest.chars().take_while(|c| c.is_ascii_alphanumeric() || *c == '-' || *c == '_').collect();
        return if ok(&id) { Some(id) } else { None };
    }
    // Dán thẳng ID playlist (PL..., OLAK5uy_..., UU...)
    if !s.contains('/') && !s.contains('.') && ok(s) && s.len() >= 12 {
        return Some(s.to_string());
    }
    None
}

fn yt_find<'a>(v: &'a Value, key: &str) -> Option<&'a Value> {
    match v {
        Value::Object(m) => {
            if let Some(x) = m.get(key) {
                return Some(x);
            }
            m.values().find_map(|x| yt_find(x, key))
        }
        Value::Array(a) => a.iter().find_map(|x| yt_find(x, key)),
        _ => None,
    }
}

#[allow(dead_code)]
fn yt_collect_videos(v: &Value, out: &mut Vec<Value>) {
    match v {
        Value::Object(m) => {
            if let Some(r) = m.get("playlistVideoRenderer") {
                out.push(r.clone());
                return;
            }
            for x in m.values() {
                yt_collect_videos(x, out);
            }
        }
        Value::Array(a) => a.iter().for_each(|x| yt_collect_videos(x, out)),
        _ => {}
    }
}

fn yt_rtext(v: Option<&Value>) -> String {
    let Some(v) = v else { return String::new() };
    if let Some(s) = v.get("simpleText").and_then(|x| x.as_str()) {
        return s.to_string();
    }
    v.get("runs")
        .and_then(|r| r.as_array())
        .map(|r| r.iter().filter_map(|x| x.get("text").and_then(|t| t.as_str())).collect::<String>())
        .unwrap_or_default()
}

async fn yt_browse(client: &reqwest::Client, body: Value) -> Result<Value, ApiErr> {
    let fail = |m: &str| -> ApiErr { (StatusCode::BAD_GATEWAY, m.to_string()) };
    let r = client
        .post("https://www.youtube.com/youtubei/v1/browse?prettyPrint=false")
        .header("origin", "https://www.youtube.com")
        .header("referer", "https://www.youtube.com/")
        .json(&body)
        .timeout(Duration::from_secs(25))
        .send()
        .await
        .map_err(|_| fail("Không kết nối được YouTube"))?;
    if !r.status().is_success() {
        return Err(fail("YouTube từ chối yêu cầu, thử lại sau"));
    }
    r.json().await.map_err(|_| fail("YouTube trả dữ liệu lạ"))
}

// Đổi "3:45" / "1:02:03" thành số giây (0 nếu không đọc được).
fn yt_clock_secs(s: &str) -> u64 {
    let mut sec = 0u64;
    for x in s.trim().split(':') {
        match x.trim().parse::<u64>() {
            Ok(n) => sec = sec * 60 + n,
            Err(_) => return 0,
        }
    }
    sec
}

// Thêm 1 bài vào danh sách nhập (bỏ qua bài trùng, video đã xóa/riêng tư không có thời lượng).
fn yt_push_track(tracks: &mut Vec<Value>, seen: &mut HashSet<String>, vid: &str, name: &str, artist: &str, secs: u64, allow_zero: bool, image: String) {
    if tracks.len() >= MAX_PL_TRACKS || vid.len() != 11 || name.is_empty() || (secs == 0 && !allow_zero) {
        return;
    }
    if !seen.insert(vid.to_string()) {
        return;
    }
    let artist = artist.strip_suffix(" - Topic").unwrap_or(artist);
    tracks.push(json!({ "src": "yt", "id": vid, "title": name, "artist": artist, "image": image, "url": "", "dur": secs, "explicit": false }));
}

// Dạng cũ của YouTube: playlistVideoRenderer.
fn yt_push_pl_renderer(r: &Value, tracks: &mut Vec<Value>, seen: &mut HashSet<String>) {
    let vid = r.get("videoId").and_then(|x| x.as_str()).unwrap_or("");
    let name = yt_rtext(r.get("title"));
    let secs: u64 = r.get("lengthSeconds").and_then(|x| x.as_str()).and_then(|x| x.parse().ok()).unwrap_or(0);
    let artist = yt_rtext(r.get("shortBylineText"));
    let image = r
        .pointer("/thumbnail/thumbnails")
        .and_then(|t| t.as_array())
        .and_then(|t| t.last())
        .and_then(|t| t.get("url"))
        .and_then(|x| x.as_str())
        .map(|x| x.split('?').next().unwrap_or(x).to_string())
        .unwrap_or_else(|| format!("https://i.ytimg.com/vi/{vid}/hqdefault.jpg"));
    yt_push_track(tracks, seen, vid, &name, &artist, secs, false, image);
}

// Dạng mới của YouTube: lockupViewModel (contentId = ID video).
fn yt_push_lockup(r: &Value, tracks: &mut Vec<Value>, seen: &mut HashSet<String>) {
    let vid = r.get("contentId").and_then(|x| x.as_str()).unwrap_or("");
    let name = r.pointer("/metadata/lockupMetadataViewModel/title/content").and_then(|x| x.as_str()).unwrap_or("");
    let artist = r
        .pointer("/metadata/lockupMetadataViewModel/metadata/contentMetadataViewModel/metadataRows/0/metadataParts/0/text/content")
        .and_then(|x| x.as_str())
        .unwrap_or("");
    let secs = yt_find(r, "thumbnailBadgeViewModel")
        .and_then(|b| b.get("text"))
        .and_then(|t| t.as_str())
        .map(yt_clock_secs)
        .unwrap_or(0);
    yt_push_track(tracks, seen, vid, name, artist, secs, false, format!("https://i.ytimg.com/vi/{vid}/hqdefault.jpg"));
}

fn yt_collect_playlist_tracks(v: &Value, tracks: &mut Vec<Value>, seen: &mut HashSet<String>) {
    match v {
        Value::Object(m) => {
            if let Some(r) = m.get("playlistVideoRenderer") {
                yt_push_pl_renderer(r, tracks, seen);
                return;
            }
            if let Some(r) = m.get("lockupViewModel") {
                yt_push_lockup(r, tracks, seen);
                return;
            }
            for x in m.values() {
                yt_collect_playlist_tracks(x, tracks, seen);
            }
        }
        Value::Array(a) => {
            for x in a {
                yt_collect_playlist_tracks(x, tracks, seen);
            }
        }
        _ => {}
    }
}

// Cách 1: đọc qua InnerTube. Err(400) = YouTube báo rõ lý do (riêng tư / không tồn tại); lỗi khác thì để đi cách 2.
async fn innertube_playlist(client: &reqwest::Client, pid: &str) -> Result<(String, Vec<Value>), ApiErr> {
    let ctx = json!({ "client": { "clientName": "WEB", "clientVersion": "2.20250310.01.00", "hl": "vi", "gl": "VN" } });
    let first = yt_browse(client, json!({ "context": ctx, "browseId": format!("VL{pid}") })).await?;
    {
        let s = first.to_string();
        eprintln!(
            "[yt-import] {pid}: InnerTube trả {} byte, playlistVideoRenderer={}, lockupViewModel={}, alerts={}",
            s.len(),
            s.contains("playlistVideoRenderer"),
            s.contains("lockupViewModel"),
            first.get("alerts").is_some()
        );
    }
    if first.get("alerts").is_some() && yt_find(&first, "playlistVideoRenderer").is_none() && yt_find(&first, "lockupViewModel").is_none() {
        let msg = yt_find(&first, "alertWithButtonRenderer")
            .or_else(|| yt_find(&first, "alertRenderer"))
            .map(|a| yt_rtext(a.get("text")))
            .unwrap_or_default();
        return Err(bad(if msg.is_empty() { "Không đọc được playlist (riêng tư hoặc không tồn tại)" } else { &msg }));
    }
    let title = yt_find(&first, "playlistHeaderRenderer")
        .map(|h| yt_rtext(h.get("title")))
        .filter(|t| !t.is_empty())
        .or_else(|| yt_find(&first, "playlistMetadataRenderer").and_then(|m| m.get("title")).and_then(|t| t.as_str()).map(|t| t.to_string()))
        .unwrap_or_default();
    let mut tracks: Vec<Value> = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();
    let mut cur = first;
    for _page in 0..(MAX_PL_TRACKS / 90 + 2) {
        yt_collect_playlist_tracks(&cur, &mut tracks, &mut seen);
        if tracks.len() >= MAX_PL_TRACKS {
            break;
        }
        let token = yt_find(&cur, "continuationCommand").and_then(|c| c.get("token")).and_then(|t| t.as_str()).map(|t| t.to_string());
        let Some(token) = token else { break };
        match yt_browse(client, json!({ "context": ctx, "continuation": token })).await {
            Ok(v) => cur = v,
            Err(_) => break,
        }
    }
    Ok((title, tracks))
}

// Cách 2 (dự phòng): dùng yt-dlp (đã có sẵn cho phần phát nhạc) để liệt kê playlist, không cần đăng nhập/cookies.
async fn ytdlp_playlist(pid: &str, use_proxy: bool) -> Result<(String, Vec<Value>), String> {
    let bin = std::env::var("YTDLP_PATH").ok().filter(|x| !x.trim().is_empty()).unwrap_or_else(|| "yt-dlp".to_string());
    let mut cmd = tokio::process::Command::new(&bin);
    cmd.arg("--flat-playlist").arg("-J").arg("--no-warnings").arg("--yes-playlist");
    if use_proxy {
        if let Ok(p) = std::env::var("YT_PROXY") {
            let p = p.trim().to_string();
            if !p.is_empty() {
                cmd.arg("--proxy").arg(p);
            }
        }
    }
    cmd.arg(format!("https://www.youtube.com/playlist?list={pid}"));
    cmd.kill_on_drop(true);
    #[cfg(windows)]
    cmd.creation_flags(0x0800_0000); // không bật cửa sổ console
    let out = tokio::time::timeout(Duration::from_secs(90), cmd.output())
        .await
        .map_err(|_| "yt-dlp: quá thời gian".to_string())?
        .map_err(|e| format!("yt-dlp: không chạy được ({e}) – cài yt-dlp hoặc đặt YTDLP_PATH"))?;
    if !out.status.success() {
        let err = String::from_utf8_lossy(&out.stderr);
        let line: String = err.lines().rev().find(|l| !l.trim().is_empty()).unwrap_or("lỗi").chars().take(200).collect();
        return Err(format!("yt-dlp: {line}"));
    }
    let v: Value = serde_json::from_slice(&out.stdout).map_err(|_| "yt-dlp: dữ liệu trả về không đọc được".to_string())?;
    let title = v.get("title").and_then(|x| x.as_str()).unwrap_or("").to_string();
    let entries = v.get("entries").and_then(|x| x.as_array()).cloned().unwrap_or_default();
    let mut tracks: Vec<Value> = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();
    for e in entries {
        let vid = e.get("id").and_then(|x| x.as_str()).unwrap_or("");
        let name = e.get("title").and_then(|x| x.as_str()).unwrap_or("");
        if name == "[Private video]" || name == "[Deleted video]" {
            continue;
        }
        let secs = e.get("duration").and_then(|x| x.as_f64()).unwrap_or(0.0).max(0.0) as u64;
        let artist = e.get("channel").and_then(|x| x.as_str()).filter(|s| !s.is_empty()).or_else(|| e.get("uploader").and_then(|x| x.as_str())).unwrap_or("");
        yt_push_track(&mut tracks, &mut seen, vid, name, artist, secs, true, format!("https://i.ytimg.com/vi/{vid}/hqdefault.jpg"));
    }
    Ok((title, tracks))
}

// Nhập 1 playlist: body {"url":"https://www.youtube.com/playlist?list=PL...","title":"(tùy chọn)"} (hoặc "urls": lấy link đầu tiên)
async fn yt_import_url(State(st): State<AppState>, headers: HeaderMap, Json(body): Json<Value>) -> R<Value> {
    let u = require(&st, &headers)?;
    let raw = body
        .get("url")
        .and_then(|x| x.as_str())
        .or_else(|| body.get("urls").and_then(|x| x.as_str()))
        .and_then(|s| s.lines().map(|l| l.trim()).find(|l| !l.is_empty()))
        .unwrap_or("");
    let Some(pid) = yt_playlist_id(raw) else {
        return Err(bad("Link playlist không hợp lệ. Hãy dán link dạng youtube.com/playlist?list=..."));
    };
    if pid.starts_with("RD") || pid == "LL" || pid == "WL" {
        return Err(bad("Không nhập được playlist này (Mix, Video đã thích hoặc Xem sau là danh sách riêng). Hãy tạo playlist công khai rồi dán link."));
    }
    let (_, use_proxy) = user_net_prefs(&st, u.id);
    let client = pick_client(&st, use_proxy);

    let mut note = String::new();
    let (mut title, mut tracks) = match innertube_playlist(&client, &pid).await {
        Ok(x) => x,
        Err(e) if e.0 == StatusCode::BAD_REQUEST => return Err(e),
        Err(e) => {
            eprintln!("[yt-import] {pid}: InnerTube lỗi: {}", e.1);
            note = e.1.clone();
            (String::new(), Vec::new())
        }
    };
    if tracks.is_empty() {
        eprintln!("[yt-import] {pid}: InnerTube không có bài nào, thử yt-dlp...");
        match ytdlp_playlist(&pid, use_proxy).await {
            Ok((t, tr)) => {
                if title.is_empty() {
                    title = t;
                }
                tracks = tr;
            }
            Err(e) => {
                eprintln!("[yt-import] {pid}: {e}");
                note = e;
            }
        }
    }
    if tracks.is_empty() {
        let extra = if note.is_empty() { String::new() } else { format!(" [{note}]") };
        return Err(bad(&format!(
            "Không đọc được bài nào từ playlist này. Hãy mở link trong cửa sổ ẩn danh để chắc chắn playlist ở chế độ Công khai hoặc Không công khai và có video.{extra}"
        )));
    }
    if let Some(t) = body.get("title").and_then(|x| x.as_str()).map(|s| s.trim()).filter(|s| !s.is_empty()) {
        title = t.to_string();
    }
    if title.is_empty() {
        title = "Playlist YouTube".to_string();
    }
    let title: String = title.chars().take(100).collect();

    let db = st.db.lock().unwrap();
    let (np, nt) = insert_playlists(&db, u.id, &[json!({ "name": title, "tracks": tracks })])?;
    Ok(Json(json!({ "playlists": np, "tracks": nt, "name": title })))
}

// ===== Liên kết tài khoản Discord (OAuth2, scope "identify") =====
// Cần 3 biến môi trường: DISCORD_CLIENT_ID, DISCORD_CLIENT_SECRET, PUBLIC_URL (vd. https://muse.example.com).
// Trong discord.com/developers → OAuth2 → Redirects phải thêm đúng: {PUBLIC_URL}/api/discord/callback
fn discord_cfg() -> Option<(String, String, String)> {
    let id = std::env::var("DISCORD_CLIENT_ID").ok()?.trim().to_string();
    let secret = std::env::var("DISCORD_CLIENT_SECRET").ok()?.trim().to_string();
    let base = std::env::var("PUBLIC_URL").ok()?.trim().trim_end_matches('/').to_string();
    if id.is_empty() || secret.is_empty() || !(base.starts_with("https://") || base.starts_with("http://")) {
        return None;
    }
    Some((id, secret, format!("{base}/api/discord/callback")))
}

fn urlenc(s: &str) -> String {
    let mut o = String::with_capacity(s.len() * 2);
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || b"-._~".contains(&b) {
            o.push(b as char);
        } else {
            o.push_str(&format!("%{:02X}", b));
        }
    }
    o
}

async fn discord_status(State(st): State<AppState>, headers: HeaderMap) -> R<Value> {
    let u = require(&st, &headers)?;
    let db = st.db.lock().unwrap();
    let (id, name, avatar, refresh): (String, String, String, String) = db
        .query_row("SELECT discord_id, discord_name, discord_avatar, discord_refresh FROM users WHERE id=?1", [u.id], |r| {
            Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?))
        })
        .map_err(|_| e500())?;
    // sdk=true: đã có token OAuth để dùng Discord Social SDK (Rich Presence không cần mở app Discord)
    Ok(Json(json!({ "linked": !id.is_empty(), "name": name, "avatar": avatar, "sdk": !refresh.is_empty() })))
}

// Trả về URL đăng nhập Discord; client điều hướng tới đó. `state` dùng 1 lần, gắn với tài khoản đang đăng nhập.
async fn discord_link(State(st): State<AppState>, headers: HeaderMap) -> R<Value> {
    let u = require(&st, &headers)?;
    let Some((cid, _, redirect)) = discord_cfg() else {
        return Err(bad("Server chưa cấu hình đăng nhập Discord"));
    };
    let state = new_token();
    {
        let db = st.db.lock().unwrap();
        let _ = db.execute("DELETE FROM discord_states WHERE created_at < datetime('now','-10 minutes')", []);
        db.execute("INSERT INTO discord_states(state,user_id) VALUES(?1,?2)", params![state, u.id])
            .map_err(|_| e500())?;
    }
    let url = format!(
        "https://discord.com/oauth2/authorize?response_type=code&client_id={}&scope=identify%20openid%20sdk.social_layer_presence&redirect_uri={}&state={}",
        urlenc(&cid),
        urlenc(&redirect),
        state
    );
    Ok(Json(json!({ "url": url })))
}

async fn discord_unlink(State(st): State<AppState>, headers: HeaderMap) -> Result<StatusCode, ApiErr> {
    let u = require(&st, &headers)?;
    let db = st.db.lock().unwrap();
    db.execute("UPDATE users SET discord_id='', discord_name='', discord_avatar='', discord_access='', discord_refresh='', discord_expires=0 WHERE id=?1", [u.id])
        .map_err(|_| e500())?;
    // Hủy liên kết thì tắt luôn Rich Presence.
    let mut cur = load_settings(&db, u.id);
    cur.insert("discord_rpc".into(), json!(false));
    let _ = db.execute(
        "UPDATE users SET settings=?1 WHERE id=?2",
        params![Value::Object(cur).to_string(), u.id],
    );
    Ok(StatusCode::OK)
}

#[derive(Deserialize)]
struct DiscordCb {
    code: Option<String>,
    state: Option<String>,
    error: Option<String>,
}

async fn discord_callback(State(st): State<AppState>, Query(q): Query<DiscordCb>) -> Response {
    let back = |ok: bool| Redirect::to(if ok { "/?discord=ok" } else { "/?discord=err" }).into_response();
    if q.error.is_some() {
        return back(false);
    }
    let (Some(code), Some(state)) = (q.code, q.state) else { return back(false) };
    let uid: Option<i64> = {
        let db = st.db.lock().unwrap();
        let r = db
            .query_row(
                "SELECT user_id FROM discord_states WHERE state=?1 AND created_at > datetime('now','-10 minutes')",
                [&state],
                |r| r.get(0),
            )
            .ok();
        let _ = db.execute("DELETE FROM discord_states WHERE state=?1", [&state]);
        r
    };
    let Some(uid) = uid else { return back(false) };
    let Some((cid, secret, redirect)) = discord_cfg() else { return back(false) };

    let body = format!(
        "grant_type=authorization_code&code={}&redirect_uri={}&client_id={}&client_secret={}",
        urlenc(&code),
        urlenc(&redirect),
        urlenc(&cid),
        urlenc(&secret)
    );
    let tok: Value = match st
        .http
        .post("https://discord.com/api/oauth2/token")
        .header("Content-Type", "application/x-www-form-urlencoded")
        .body(body)
        .send()
        .await
    {
        Ok(r) if r.status().is_success() => r.json().await.unwrap_or(Value::Null),
        _ => return back(false),
    };
    let Some(access) = tok["access_token"].as_str() else { return back(false) };
    let me: Value = match st.http.get("https://discord.com/api/users/@me").bearer_auth(access).send().await {
        Ok(r) if r.status().is_success() => r.json().await.unwrap_or(Value::Null),
        _ => return back(false),
    };
    let id = me["id"].as_str().unwrap_or("");
    if id.is_empty() || !id.chars().all(|c| c.is_ascii_digit()) {
        return back(false);
    }
    let name: String = me["global_name"]
        .as_str()
        .filter(|s| !s.is_empty())
        .or_else(|| me["username"].as_str())
        .unwrap_or("Discord")
        .chars()
        .take(40)
        .collect();
    let avatar = match me["avatar"].as_str() {
        Some(h) if !h.is_empty() && h.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') => {
            format!("https://cdn.discordapp.com/avatars/{id}/{h}.png?size=64")
        }
        _ => String::new(),
    };
    let db = st.db.lock().unwrap();
    if db
        .execute(
            "UPDATE users SET discord_id=?1, discord_name=?2, discord_avatar=?3, discord_access=?4, discord_refresh=?5, discord_expires=?6 WHERE id=?7",
            params![
                id,
                name,
                avatar,
                access,
                tok["refresh_token"].as_str().unwrap_or(""),
                (now_secs() + tok["expires_in"].as_u64().unwrap_or(604800)) as i64,
                uid
            ],
        )
        .is_err()
    {
        return back(false);
    }
    back(true)
}

// Trả access token Discord còn hạn (tự làm mới bằng refresh token) cho client dùng với Discord Social SDK.
async fn discord_token(State(st): State<AppState>, headers: HeaderMap) -> R<Value> {
    let u = require(&st, &headers)?;
    let Some((cid, secret, _)) = discord_cfg() else {
        return Err(bad("Server chưa cấu hình đăng nhập Discord"));
    };
    let (access, refresh, exp): (String, String, i64) = {
        let db = st.db.lock().unwrap();
        db.query_row("SELECT discord_access, discord_refresh, discord_expires FROM users WHERE id=?1", [u.id], |r| {
            Ok((r.get(0)?, r.get(1)?, r.get(2)?))
        })
        .map_err(|_| e500())?
    };
    if refresh.is_empty() {
        return Err(bad("Hãy hủy liên kết rồi đăng nhập Discord lại để cấp quyền Rich Presence"));
    }
    if !access.is_empty() && exp as u64 > now_secs() + 300 {
        return Ok(Json(json!({ "token": access, "app_id": cid })));
    }
    let body = format!(
        "grant_type=refresh_token&refresh_token={}&client_id={}&client_secret={}",
        urlenc(&refresh),
        urlenc(&cid),
        urlenc(&secret)
    );
    let tok: Value = match st
        .http
        .post("https://discord.com/api/oauth2/token")
        .header("Content-Type", "application/x-www-form-urlencoded")
        .body(body)
        .send()
        .await
    {
        Ok(r) if r.status().is_success() => r.json().await.unwrap_or(Value::Null),
        _ => return Err(bad("Phiên Discord hết hạn, hãy hủy liên kết rồi đăng nhập Discord lại")),
    };
    let Some(new_access) = tok["access_token"].as_str() else {
        return Err(bad("Discord không trả token"));
    };
    {
        let db = st.db.lock().unwrap();
        let _ = db.execute(
            "UPDATE users SET discord_access=?1, discord_refresh=?2, discord_expires=?3 WHERE id=?4",
            params![
                new_access,
                tok["refresh_token"].as_str().unwrap_or(&refresh),
                (now_secs() + tok["expires_in"].as_u64().unwrap_or(604800)) as i64,
                u.id
            ],
        );
    }
    Ok(Json(json!({ "token": new_access, "app_id": cid })))
}
