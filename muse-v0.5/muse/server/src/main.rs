use argon2::{
    password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString},
    Argon2,
};
use axum::{
    body::{Body, Bytes},
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        DefaultBodyLimit, Path, Query, State,
    },
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Redirect, Response},
    routing::{get, post},
    Json, Router,
};
use futures_util::{SinkExt, StreamExt};
use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::broadcast;
use tower_http::services::ServeDir;

struct Room {
    tx: broadcast::Sender<String>,
    last_state: Option<String>,
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
    app_dir: PathBuf,
    http: reqwest::Client,
    // Client đi qua proxy (biến môi trường YT_PROXY), dùng khi người dùng bật "Proxy" trong Cài đặt.
    http_proxy: Option<reqwest::Client>,
    // Cache địa chỉ luồng âm thanh YouTube: khoá -> (url, kích thước, mime, thời điểm tạo - giây)
    audio: Arc<Mutex<HashMap<String, (String, u64, String, u64, usize)>>>,
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
}

const USER_COLS: &str = "id, username, display_name, avatar, accent, bg, bg_url, code, is_admin, banner, banner_color, status, crop";

fn row_to_user(r: &rusqlite::Row) -> rusqlite::Result<User> {
    Ok(User {
        id: r.get(0)?,
        username: r.get(1)?,
        display_name: r.get(2)?,
        avatar: r.get(3)?,
        accent: r.get(4)?,
        bg: r.get(5)?,
        bg_url: r.get(6)?,
        code: r.get(7)?,
        is_admin: r.get::<_, i64>(8)? != 0,
        banner: r.get(9)?,
        banner_color: r.get(10)?,
        status: r.get(11)?,
        crop: r.get(12)?,
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

fn user_from_token(db: &Connection, token: &str) -> Option<User> {
    let sql = "SELECT u.id,u.username,u.display_name,u.avatar,u.accent,u.bg,u.bg_url,u.code,u.is_admin,u.banner,u.banner_color,u.status,u.crop \
               FROM users u JOIN sessions s ON s.user_id=u.id \
               WHERE s.token=?1 AND s.created_at > datetime('now','-30 days')";
    db.query_row(sql, [token], row_to_user).ok()
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
        "PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON;
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
         CREATE TABLE IF NOT EXISTS favorites(
           user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
           track_key TEXT NOT NULL, track_json TEXT NOT NULL,
           PRIMARY KEY(user_id, track_key));
         CREATE TABLE IF NOT EXISTS history(
           user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
           track_key TEXT NOT NULL, track_json TEXT NOT NULL,
           PRIMARY KEY(user_id, track_key));
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
    ] {
        let _ = db.execute(ddl, []);
    }
    // Trạng thái tạm (dùng 1 lần) cho luồng đăng nhập Discord OAuth2.
    let _ = db.execute(
        "CREATE TABLE IF NOT EXISTS discord_states(state TEXT PRIMARY KEY, user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE, created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)",
        [],
    );
    // Dọn dữ liệu SoundCloud cũ (nguồn "sc" đã bị gỡ).
    for t in ["favorites", "history", "playlist_tracks"] {
        let _ = db.execute(&format!("DELETE FROM {t} WHERE track_key LIKE 'sc:%'"), []);
    }
    db
}

// Tạo/cấp quyền admin từ biến môi trường ADMIN_USER (+ ADMIN_PASS nếu tài khoản chưa tồn tại).
fn bootstrap_admin(db: &Connection) {
    let Ok(user) = std::env::var("ADMIN_USER") else { return };
    let user = user.trim().to_lowercase();
    if user.len() < 3 || user.len() > 24 || !user.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        eprintln!("ADMIN_USER không hợp lệ (3-24 ký tự: chữ, số, _)");
        return;
    }
    let existing: Option<(i64, String)> = db
        .query_row("SELECT id, pass_hash FROM users WHERE username=?1", [&user], |r| Ok((r.get(0)?, r.get(1)?)))
        .ok();
    match existing {
        Some((id, hash)) => {
            // Tài khoản đã có: chỉ cấp admin khi ADMIN_PASS khớp mật khẩu hiện tại (chống người khác đăng ký trước tên này).
            match std::env::var("ADMIN_PASS") {
                Ok(pw) if verify_pw(&pw, &hash) => {
                    let _ = db.execute("UPDATE users SET is_admin=1 WHERE id=?1", [id]);
                    println!("Tài khoản {user} có quyền admin");
                }
                _ => eprintln!("Bỏ qua cấp admin cho {user}: cần ADMIN_PASS khớp mật khẩu hiện tại của tài khoản này"),
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
            let res = db.execute(
                "INSERT INTO users(username,pass_hash,display_name,code,is_admin) VALUES(?1,?2,?3,?4,1)",
                params![user, hash, "Admin", code],
            );
            match res {
                Ok(_) => println!("Đã tạo tài khoản admin: {user}"),
                Err(e) => eprintln!("Không tạo được admin: {e}"),
            }
        }
    }
}

#[tokio::main]
async fn main() {
    let data_dir = PathBuf::from(std::env::var("DATA_DIR").unwrap_or_else(|_| "data".into()));
    let avatar_dir = data_dir.join("avatars");
    let bg_dir = data_dir.join("backgrounds");
    let banner_dir = data_dir.join("banners");
    let app_dir = data_dir.join("app");
    std::fs::create_dir_all(&avatar_dir).unwrap();
    std::fs::create_dir_all(&bg_dir).unwrap();
    std::fs::create_dir_all(&banner_dir).unwrap();
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
        app_dir: app_dir.clone(),
        http: reqwest::Client::builder()
            .timeout(Duration::from_secs(15))
            .user_agent("Muse/0.3")
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
    };

    let router = Router::new()
        .route("/api/config", get(config))
        .route("/api/register", post(register))
        .route("/api/login", post(login))
        .route("/api/logout", post(logout))
        .route("/api/me", get(me))
        .route("/api/profile", post(update_profile))
        .route("/api/account/name", post(account_name))
        .route("/api/account/password", post(account_password))
        .route("/api/settings", get(settings_get).post(settings_set))
        .route("/api/discord/status", get(discord_status))
        .route("/api/discord/link", post(discord_link).delete(discord_unlink))
        .route("/api/discord/callback", get(discord_callback))
        .route("/api/discord/token", get(discord_token))
        .route("/api/backup", get(backup_export))
        .route("/api/restore", post(backup_restore).layer(DefaultBodyLimit::max(10 * 1024 * 1024)))
        .route("/api/import", post(import_playlists).layer(DefaultBodyLimit::max(10 * 1024 * 1024)))
        .route("/api/avatar", post(upload_avatar))
        .route("/api/background", post(upload_bg).layer(DefaultBodyLimit::max(10 * 1024 * 1024)))
        .route("/api/banner", post(upload_banner).delete(remove_banner).layer(DefaultBodyLimit::max(10 * 1024 * 1024)))
        .route("/api/admin/icon", post(admin_icon).delete(admin_icon_reset))
        .route("/app-icon.png", get(serve_icon))
        .route("/api/yt/search", get(yt_search))
        .route("/api/yt/audio/:id", get(yt_audio))
        .route("/api/friends", get(list_friends).post(add_friend))
        .route("/api/favorites", get(fav_list).post(fav_toggle))
        .route("/api/history", get(hist_list).post(hist_add).delete(hist_clear))
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
        .fallback_service(ServeDir::new(static_dir))
        .layer(DefaultBodyLimit::max(10 * 1024 * 1024))
        .with_state(st);

    let port = std::env::var("PORT").unwrap_or_else(|_| "3000".into());
    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{port}")).await.unwrap();
    println!("Muse server: http://localhost:{port}");
    axum::serve(listener, router).await.unwrap();
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

async fn register(State(st): State<AppState>, Json(r): Json<AuthReq>) -> R<AuthResp> {
    let username = r.username.trim().to_lowercase();
    if username.len() < 3 || username.len() > 24 || !username.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        return Err(bad("Tên đăng nhập 3-24 ký tự: chữ, số hoặc _"));
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
    db.execute("INSERT INTO sessions(token,user_id) VALUES(?1,?2)", params![token, id]).map_err(|_| e500())?;
    let user = db
        .query_row(&format!("SELECT {USER_COLS} FROM users WHERE id=?1"), [id], row_to_user)
        .map_err(|_| e500())?;
    Ok(Json(AuthResp { token, user }))
}

async fn login(State(st): State<AppState>, Json(r): Json<AuthReq>) -> R<AuthResp> {
    let username = r.username.trim().to_lowercase();
    let row = {
        let db = st.db.lock().unwrap();
        let x = db
            .query_row("SELECT id, pass_hash FROM users WHERE username=?1", [&username], |r| {
                Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?))
            })
            .ok();
        x
    };
    let Some((id, hash)) = row else { return Err(unauth_login()) };
    let pw = r.password.clone();
    let ok = tokio::task::spawn_blocking(move || verify_pw(&pw, &hash)).await.unwrap_or(false);
    if !ok {
        return Err(unauth_login());
    }
    let db = st.db.lock().unwrap();
    let _ = db.execute("DELETE FROM sessions WHERE created_at < datetime('now','-30 days')", []);
    let token = new_token();
    db.execute("INSERT INTO sessions(token,user_id) VALUES(?1,?2)", params![token, id]).map_err(|_| e500())?;
    let user = db
        .query_row(&format!("SELECT {USER_COLS} FROM users WHERE id=?1"), [id], row_to_user)
        .map_err(|_| e500())?;
    Ok(Json(AuthResp { token, user }))
}
fn unauth_login() -> ApiErr { (StatusCode::UNAUTHORIZED, "Sai tên đăng nhập hoặc mật khẩu".into()) }

async fn logout(State(st): State<AppState>, headers: HeaderMap) -> StatusCode {
    if let Some(t) = bearer(&headers) {
        let db = st.db.lock().unwrap();
        let _ = db.execute("DELETE FROM sessions WHERE token=?1", [t]);
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
    db.execute(
        "UPDATE users SET display_name=?1, accent=?2, bg=?3, bg_url=?4 WHERE id=?5",
        params![name, r.accent, r.bg, r.bg_url, user.id],
    )
    .map_err(|_| e500())?;
    if let Some(c) = r.banner_color.as_ref() {
        if !(c.is_empty() || (c.len() == 7 && c.starts_with('#') && c[1..].chars().all(|x| x.is_ascii_hexdigit()))) {
            return Err(bad("Màu banner không hợp lệ"));
        }
        db.execute("UPDATE users SET banner_color=?1 WHERE id=?2", params![c, user.id]).map_err(|_| e500())?;
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
        db.execute("UPDATE users SET crop=?1 WHERE id=?2", params![Value::Object(out).to_string(), user.id]).map_err(|_| e500())?;
    }
    if let Some(stt) = r.status.as_ref() {
        let clean: String = stt.chars().filter(|c| !c.is_control()).collect::<String>().trim().chars().take(60).collect();
        db.execute("UPDATE users SET status=?1 WHERE id=?2", params![clean, user.id]).map_err(|_| e500())?;
    }
    if let Some(em) = r.avatar_emoji.filter(|e| !e.trim().is_empty() && e.chars().count() <= 4) {
        db.execute("UPDATE users SET avatar=?1 WHERE id=?2", params![em.trim(), user.id]).map_err(|_| e500())?;
    }
    let out = db
        .query_row(&format!("SELECT {USER_COLS} FROM users WHERE id=?1"), [user.id], row_to_user)
        .map_err(|_| e500())?;
    Ok(Json(out))
}

fn sniff_image(b: &[u8]) -> Option<&'static str> {
    if b.len() > 12 && (b.starts_with(b"GIF87a") || b.starts_with(b"GIF89a")) { Some("gif") }
    else if b.starts_with(&[0x89, b'P', b'N', b'G']) { Some("png") }
    else if b.starts_with(&[0xFF, 0xD8, 0xFF]) { Some("jpg") }
    else if b.len() > 12 && &b[0..4] == b"RIFF" && &b[8..12] == b"WEBP" { Some("webp") }
    else { None }
}

// Lưu nguyên file gốc (không re-encode) để GIF giữ được chuyển động.
async fn upload_avatar(State(st): State<AppState>, headers: HeaderMap, body: Bytes) -> R<User> {
    let user = require(&st, &headers)?;
    if body.len() > 3 * 1024 * 1024 {
        return Err(bad("Avatar tối đa 3MB"));
    }
    let ext = sniff_image(&body).ok_or_else(|| bad("Chỉ nhận ảnh PNG, JPG, GIF, WEBP (tối đa 3MB)"))?;
    let name = format!("{}.{}", uuid::Uuid::new_v4().simple(), ext);
    tokio::fs::write(st.avatar_dir.join(&name), &body).await.map_err(|_| e500())?;
    let url = format!("/avatars/{name}");
    let old: String;
    {
        let db = st.db.lock().unwrap();
        old = db.query_row("SELECT avatar FROM users WHERE id=?1", [user.id], |r| r.get(0)).unwrap_or_default();
        db.execute("UPDATE users SET avatar=?1 WHERE id=?2", params![url, user.id]).map_err(|_| e500())?;
    }
    if let Some(f) = old.strip_prefix("/avatars/") {
        if !f.contains('/') && !f.contains("..") {
            let _ = tokio::fs::remove_file(st.avatar_dir.join(f)).await;
        }
    }
    Ok(Json(User { avatar: url, ..user }))
}

#[derive(Deserialize)]
struct FriendReq {
    code: String,
}

async fn add_friend(State(st): State<AppState>, headers: HeaderMap, Json(r): Json<FriendReq>) -> Result<StatusCode, ApiErr> {
    let user = require(&st, &headers)?;
    let db = st.db.lock().unwrap();
    let fid: Option<i64> = db
        .query_row("SELECT id FROM users WHERE code=?1", [r.code.trim().to_uppercase()], |r| r.get(0))
        .ok();
    let Some(fid) = fid else { return Err((StatusCode::NOT_FOUND, "Không tìm thấy mã bạn bè".into())) };
    if fid == user.id {
        return Err(bad("Không thể tự kết bạn với chính mình"));
    }
    for (a, b) in [(user.id, fid), (fid, user.id)] {
        db.execute("INSERT OR IGNORE INTO friends(user_id,friend_id) VALUES(?1,?2)", params![a, b])
            .map_err(|_| e500())?;
    }
    Ok(StatusCode::OK)
}

async fn list_friends(State(st): State<AppState>, headers: HeaderMap) -> R<Vec<Friend>> {
    let user = require(&st, &headers)?;
    let online: HashSet<i64> = st.notify.lock().unwrap().keys().cloned().collect();
    let db = st.db.lock().unwrap();
    let mut stmt = db
        .prepare("SELECT u.id, u.display_name, u.avatar, u.accent, u.banner, u.banner_color, u.status, u.crop FROM friends f JOIN users u ON u.id=f.friend_id WHERE f.user_id=?1")
        .map_err(|_| e500())?;
    let rows = stmt
        .query_map([user.id], |r| {
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
            })
        })
        .map_err(|_| e500())?;
    let list: Vec<Friend> = rows.filter_map(|x| x.ok()).collect();
    Ok(Json(list))
}

#[derive(Deserialize)]
struct WsQ {
    token: String,
}

async fn ws_handler(ws: WebSocketUpgrade, Path(room): Path<String>, Query(q): Query<WsQ>, State(st): State<AppState>) -> Response {
    let user = {
        let db = st.db.lock().unwrap();
        let u = user_from_token(&db, &q.token);
        u
    };
    let Some(user) = user else { return StatusCode::UNAUTHORIZED.into_response() };
    if room.is_empty() || room.len() > 40 {
        return StatusCode::BAD_REQUEST.into_response();
    }
    ws.on_upgrade(move |s| handle_socket(s, room, user, st))
}

async fn handle_socket(socket: WebSocket, room: String, user: User, st: AppState) {
    let (mut sender, mut receiver) = socket.split();
    let (tx, mut rx, last) = {
        let mut g = st.rooms.lock().unwrap();
        let r = g.entry(room.clone()).or_insert_with(|| Room { tx: broadcast::channel(64).0, last_state: None });
        (r.tx.clone(), r.tx.subscribe(), r.last_state.clone())
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
    let mut recv_task = tokio::spawn(async move {
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
    let user = {
        let db = st.db.lock().unwrap();
        let u = user_from_token(&db, &q.token);
        u
    };
    let Some(user) = user else { return StatusCode::UNAUTHORIZED.into_response() };
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
    let _ = db.execute("DELETE FROM sessions WHERE user_id=?1 AND token<>?2", params![user.id, token]);
    Ok(StatusCode::OK)
}

// ===== Cài đặt người dùng (lưu dạng JSON, chỉ nhận các khóa đã biết) =====
const BOOL_SETTINGS: [&str; 10] = [
    "track_history", "save_state", "crossfade", "explicit", "offline_pl", "proxy", "eq_on", "delay_on", "reverb_on", "discord_rpc",
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
    Ok(Json(json!({
        "app": "muse",
        "version": 1,
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
    let v: Vec<Value> = rows.filter_map(|x| x.ok()).collect();
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
    if body.len() > 10 * 1024 * 1024 {
        return Err(bad("Ảnh nền tối đa 10MB"));
    }
    let ext = sniff_image(&body).ok_or_else(|| bad("Chỉ nhận ảnh PNG, JPG, GIF, WEBP"))?;
    let name = format!("{}.{}", uuid::Uuid::new_v4().simple(), ext);
    tokio::fs::write(st.bg_dir.join(&name), &body).await.map_err(|_| e500())?;
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
            let _ = tokio::fs::remove_file(st.bg_dir.join(f)).await;
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
    let name = format!("{}.{}", uuid::Uuid::new_v4().simple(), ext);
    tokio::fs::write(st.banner_dir.join(&name), &body).await.map_err(|_| e500())?;
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
            let _ = tokio::fs::remove_file(st.banner_dir.join(f)).await;
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
}

fn yt_text(v: &Value) -> String {
    v.get("runs")
        .and_then(|r| r.as_array())
        .map(|a| a.iter().filter_map(|x| x.get("text").and_then(|t| t.as_str())).collect::<String>())
        .unwrap_or_default()
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
    let mut parts: Vec<String> = col(1)
        .split(" \u{2022} ")
        .map(|x| x.trim().to_string())
        .filter(|x| !x.is_empty())
        .collect();
    if parts.len() > 1 && (parts[0] == "Song" || parts[0] == "Video") {
        parts.remove(0);
    }
    let artist = parts.first().cloned().unwrap_or_default();
    let dur: u64 = parts
        .last()
        .map(|p| {
            let mut sec = 0u64;
            for x in p.split(':') {
                match x.parse::<u64>() {
                    Ok(n) => sec = sec * 60 + n,
                    Err(_) => return 0,
                }
            }
            sec
        })
        .unwrap_or(0);
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
    Some(json!({ "src": "yt", "id": id, "title": title, "artist": artist, "image": image, "url": "", "dur": dur, "explicit": explicit }))
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
    let user = require(&st, &headers)?;
    let (allow_explicit, use_proxy) = user_net_prefs(&st, user.id);
    let client = pick_client(&st, use_proxy);
    let text: String = q.q.unwrap_or_default().trim().chars().take(100).collect();
    if text.is_empty() {
        return Err(bad("Thiếu từ khoá tìm kiếm"));
    }
    let fail = |m: &str| -> ApiErr { (StatusCode::BAD_GATEWAY, m.to_string()) };
    let body = json!({
        "context": { "client": { "clientName": "WEB_REMIX", "clientVersion": "1.20250310.01.00", "hl": "en", "gl": "VN" } },
        "query": text,
        // Bộ lọc "Songs" của YouTube Music
        "params": "EgWKAQIIAWoKEAkQBRAKEAMQBA=="
    });
    let resp = client
        .post("https://music.youtube.com/youtubei/v1/search?prettyPrint=false")
        .header("origin", "https://music.youtube.com")
        .header("referer", "https://music.youtube.com/")
        .json(&body)
        .send()
        .await
        .map_err(|_| fail("Không kết nối được YouTube Music"))?;
    if !resp.status().is_success() {
        return Err(fail("YouTube Music trả về lỗi"));
    }
    let v: Value = resp.json().await.map_err(|_| fail("YouTube Music trả dữ liệu lạ"))?;
    let mut out: Vec<Value> = Vec::new();
    yt_collect(&v, &mut out);
    if !allow_explicit {
        out.retain(|t| !t.get("explicit").and_then(|x| x.as_bool()).unwrap_or(false));
    }
    out.truncate(30);
    Ok(Json(Value::Array(out)))
}

// ===== Luồng âm thanh YouTube (để áp dụng Bộ chỉnh âm / Độ trễ / Độ vang bằng Web Audio) =====
// Server lấy địa chỉ luồng âm thanh qua InnerTube rồi chuyển tiếp từng đoạn (Range) cho trình duyệt,
// nhờ đó <audio> cùng nguồn gốc và Web Audio xử lý được (player iframe của YouTube thì không).
#[derive(Deserialize)]
struct AudQ {
    t: Option<String>,
    f: Option<String>,
    debug: Option<String>,
}

fn now_secs() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0)
}

const AUDIO_CHUNK: u64 = 2 * 1024 * 1024;
const AUDIO_TTL: u64 = 3 * 60 * 60;

// (clientName, header id, clientVersion, user-agent, phần context thêm)
fn audio_clients() -> Vec<(&'static str, &'static str, &'static str, &'static str, Value)> {
    vec![
        (
            "ANDROID_VR",
            "28",
            "1.60.19",
            "com.google.android.apps.youtube.vr.oculus/1.60.19 (Linux; U; Android 12L; eureka-user Build/SQ3A.220605.009.A1) gzip",
            json!({ "deviceMake": "Oculus", "deviceModel": "Quest 3", "androidSdkVersion": 32, "osName": "Android", "osVersion": "12L" }),
        ),
        (
            "IOS",
            "5",
            "20.10.4",
            "com.google.ios.youtube/20.10.4 (iPhone16,2; U; CPU iOS 18_3_2 like Mac OS X;)",
            json!({ "deviceMake": "Apple", "deviceModel": "iPhone16,2", "osName": "iPhone", "osVersion": "18.3.2.22D82" }),
        ),
        (
            "TVHTML5_SIMPLY_EMBEDDED_PLAYER",
            "85",
            "2.0",
            "Mozilla/5.0 (ChromiumStylePlatform) Cobalt/Version",
            json!({}),
        ),
    ]
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
                last = format!("{name}: YouTube trả về HTTP {}", r.status().as_u16());
                eprintln!("[audio] {vid} {last}");
                continue;
            }
            Err(e) => {
                last = format!("{name}: không kết nối được YouTube ({e})");
                eprintln!("[audio] {vid} {last}");
                continue;
            }
        };
        let v: Value = match resp.json().await {
            Ok(v) => v,
            Err(_) => continue,
        };
        if v.pointer("/playabilityStatus/status").and_then(|x| x.as_str()) != Some("OK") {
            last = format!(
                "{name}: {} ({})",
                v.pointer("/playabilityStatus/reason").and_then(|x| x.as_str()).unwrap_or("video không phát được"),
                v.pointer("/playabilityStatus/status").and_then(|x| x.as_str()).unwrap_or("?")
            );
            eprintln!("[audio] {vid} {last}");
            continue;
        }
        let Some(fmts) = v.pointer("/streamingData/adaptiveFormats").and_then(|x| x.as_array()) else {
            last = format!("{name}: không có danh sách định dạng");
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
        let Some((_, f)) = best else { continue };
        let url = f.get("url").and_then(|x| x.as_str()).unwrap_or("").to_string();
        let size: u64 = f
            .get("contentLength")
            .and_then(|x| x.as_str())
            .and_then(|x| x.parse().ok())
            .or_else(|| f.get("contentLength").and_then(|x| x.as_u64()))
            .unwrap_or(0);
        if url.is_empty() || size == 0 {
            last = format!("{name}: định dạng không có URL trực tiếp (có thể cần PO token)");
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
        return Ok((url, size, mime, ci));
    }
    Err(last)
}

// Thứ tự thử các nguồn: client YouTube -> yt-dlp (90) -> Piped (99).
// Có YT_COOKIES thì yt-dlp (đăng nhập bằng cookies) được thử đầu tiên vì tránh được "Sign in to confirm you're not a bot".
fn audio_order() -> Vec<usize> {
    let mut v: Vec<usize> = (0..audio_clients().len()).collect();
    let has = |k: &str| std::env::var(k).map(|x| !x.trim().is_empty()).unwrap_or(false);
    if has("YT_COOKIES") || has("YT_BROWSER") {
        v.insert(0, 90);
    } else {
        v.push(90);
    }
    v.push(99);
    v
}

async fn resolve_source(client: &reqwest::Client, vid: &str, webm: bool, src: usize, use_proxy: bool) -> Result<(String, u64, String, usize), String> {
    match src {
        90 => resolve_ytdlp(client, vid, webm, use_proxy).await,
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

// yt-dlp (cài riêng, đặt trong PATH hoặc biến YTDLP_PATH). YT_COOKIES = đường dẫn file cookies.txt (định dạng Netscape).
async fn resolve_ytdlp(client: &reqwest::Client, vid: &str, webm: bool, use_proxy: bool) -> Result<(String, u64, String, usize), String> {
    let bin = std::env::var("YTDLP_PATH").ok().filter(|x| !x.trim().is_empty()).unwrap_or_else(|| "yt-dlp".to_string());
    let fmt = if webm { "251/bestaudio[ext=webm]/bestaudio" } else { "140/bestaudio[ext=m4a]/bestaudio" };
    let mut cmd = tokio::process::Command::new(&bin);
    cmd.arg("-g").arg("--no-playlist").arg("--no-warnings").arg("-f").arg(fmt);
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
    cmd.arg(format!("https://www.youtube.com/watch?v={vid}"));
    cmd.kill_on_drop(true);
    #[cfg(windows)]
    cmd.creation_flags(0x0800_0000); // không bật cửa sổ console
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
    let Some(uid) = uid else { return (StatusCode::UNAUTHORIZED, "Phiên đăng nhập hết hạn").into_response() };
    if id.len() != 11 || !id.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_') {
        return StatusCode::BAD_REQUEST.into_response();
    }
    let (_, use_proxy) = user_net_prefs(&st, uid);
    let client = pick_client(&st, use_proxy);
    let webm = q.f.as_deref() == Some("webm");
    let ckey = format!("{id}|{}|{}", use_proxy as u8, webm as u8);
    let (start, end_req) = parse_range(&headers);
    let mut notes = String::new();

    // Thử lần lượt các nguồn (client YouTube -> yt-dlp -> Piped); nguồn nào bị chặn thì bỏ qua và thử nguồn kế tiếp.
    let order = audio_order();
    let mut pos = 0usize;
    for _round in 0..=order.len() {
        let cached = {
            let c = st.audio.lock().unwrap();
            c.get(&ckey).filter(|e| now_secs().saturating_sub(e.3) < AUDIO_TTL).cloned()
        };
        let (url, size, mime, ci) = match cached {
            Some((u, s, m, _, ci)) => (u, s, m, ci),
            None => {
                let mut res = None;
                while pos < order.len() {
                    let src = order[pos];
                    if audio_bad(&id, src) {
                        pos += 1;
                        continue;
                    }
                    match resolve_source(&client, &id, webm, src, use_proxy).await {
                        Ok(x) => {
                            res = Some(x);
                            break;
                        }
                        Err(e) => {
                            eprintln!("[audio] {id} nguồn {src}: {e}");
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
        if start >= size {
            return (StatusCode::RANGE_NOT_SATISFIABLE, [(header::CONTENT_RANGE, format!("bytes */{size}"))]).into_response();
        }
        let last = size - 1;
        let end = end_req.unwrap_or(last).min(last).min(start + AUDIO_CHUNK - 1);

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
            match req.header("user-agent", ua).timeout(Duration::from_secs(30)).send().await {
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
                .header(header::CACHE_CONTROL, "private, max-age=600")
                .body(Body::from(b))
                .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response());
        }
        eprintln!("[audio] {id} googlevideo từ chối đoạn {start}-{end}: {notes}");
        st.audio.lock().unwrap().remove(&ckey);
        audio_mark_bad(&id, ci);
    }
    (StatusCode::BAD_GATEWAY, format!("googlevideo từ chối: {notes}")).into_response()
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
