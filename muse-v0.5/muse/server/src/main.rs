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

mod pl_import;

struct Room {
    tx: broadcast::Sender<String>,
    last_state: Option<String>,
}

#[derive(Clone)]
struct AppState {
    db: Arc<Mutex<Connection>>,
    rooms: Arc<Mutex<HashMap<String, Room>>>,
    notify: Arc<Mutex<HashMap<i64, broadcast::Sender<String>>>>,
    avatar_dir: PathBuf,
    bg_dir: PathBuf,
    banner_dir: PathBuf,
    app_dir: PathBuf,
    http: reqwest::Client,
    http_proxy: Option<reqwest::Client>,
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
    let _ = db.execute("ALTER TABLE users ADD COLUMN is_admin INTEGER NOT NULL DEFAULT 0", []);
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
    let _ = db.execute(
        "CREATE TABLE IF NOT EXISTS user_sync(
           user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
           k TEXT NOT NULL, v TEXT NOT NULL, ts INTEGER NOT NULL DEFAULT 0,
           PRIMARY KEY(user_id, k))",
        [],
    );
    let _ = db.execute(
        "CREATE TABLE IF NOT EXISTS discord_states(state TEXT PRIMARY KEY, user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE, created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)",
        [],
    );
    for t in ["favorites", "history", "playlist_tracks"] {
        let _ = db.execute(&format!("DELETE FROM {t} WHERE track_key LIKE 'sc:%'"), []);
    }
    db
}

fn bootstrap_admin(db: &Connection) {
    let Ok(user) = std::env::var("ADMIN_USER") else { return };
    let user = user.trim().to_lowercase();
    if user.len() < 3 || user.len() > 24 || !user.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        eprintln!("ADMIN_USER không hợp lệ (3-24 ký tự: chữ, số, _)");
        return;
    }
    if std::env::var("ADMIN_PASS").map(|p| p.len() >= 8 && p.len() <= 128).unwrap_or(false) {
        let _ = db.execute("DELETE FROM users WHERE username=?1 AND is_admin=0", [&user]);
    }
    let existing: Option<(i64, String)> = db
        .query_row("SELECT id, pass_hash FROM users WHERE username=?1", [&user], |r| Ok((r.get(0)?, r.get(1)?)))
        .ok();
    match existing {
        Some((id, _)) => {
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
        .route("/api/background", post(upload_bg).layer(DefaultBodyLimit::max(10 * 1024 * 1024)))
        .route("/api/banner", post(upload_banner).delete(remove_banner).layer(DefaultBodyLimit::max(10 * 1024 * 1024)))
        .route("/api/admin/icon", post(admin_icon).delete(admin_icon_reset))
        .route("/app-icon.png", get(serve_icon))
        .route("/api/yt/search", get(yt_search))
        .route("/api/yt/browse", get(yt_browse_music))
        .route("/api/yt/similar", get(yt_similar))
        .route("/api/yt/fresh", get(yt_fresh))
        .route("/api/yt/artists", get(yt_artists))
        .route("/api/yt/best-artists", get(yt_best_artists))
        .route("/api/yt/this-is", get(yt_this_is))
        .route("/api/yt/audio/:id", get(yt_audio))
        .route("/api/yt/video/:id", get(yt_video))
        .route("/api/friends", get(list_friends).post(add_friend))
        .route("/api/friends/requests", get(list_requests))
        .route("/api/friends/respond", post(respond_request))
        .route("/api/friends/:id", axum::routing::delete(remove_friend))
        .route("/api/favorites", get(fav_list).post(fav_toggle))
        .route("/api/history", get(hist_list).post(hist_add).delete(hist_clear))
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

    let fit = format!("{max_px}x{max_px}");
    let mut best_gif: Vec<u8> = body.to_vec();
    let mut webp_input = s;
    let packed = run_tool(&gifsicle, &["-w", "--resize-fit", fit.as_str(), "-O3", "--lossy=40", s, "-o", o]).await
        || run_tool(&gifsicle, &["-w", "--resize-fit", fit.as_str(), "-O3", s, "-o", o]).await;
    if packed {
        if let Ok(b) = tokio::fs::read(opt).await {
            if matches!(sniff_image(&b), Some("gif")) && b.len() < best_gif.len() {
                best_gif = b;
                webp_input = o;
            }
        }
    }

    let mut video: Option<Vec<u8>> = None;
    if want_video {
        let made = run_tool(
            &ffmpeg,
            &[
                "-v", "error", "-y", "-i", webp_input, "-an", "-vf", "fps=25,scale=trunc(iw/2)*2:trunc(ih/2)*2",
                "-c:v", "libx264", "-profile:v", "main", "-pix_fmt", "yuv420p", "-preset", "veryfast", "-crf", "26",
                "-movflags", "+faststart", vp,
            ],
        )
        .await;
        if made {
            if let Ok(b) = tokio::fs::read(vid).await {
                if b.len() > 12 && &b[4..8] == b"ftyp" {
                    video = Some(b);
                }
            }
        }
    }

    let mut image: Option<(Vec<u8>, &'static str)> = None;
    let converted = run_tool(
        &gif2webp,
        &["-quiet", "-mixed", "-q", "80", "-m", "4", "-mt", "-kmin", "3", "-kmax", "5", webp_input, "-o", w],
    )
    .await;
    if converted {
        if let Ok(b) = tokio::fs::read(web).await {
            if matches!(sniff_image(&b), Some("webp")) && b.len() <= best_gif.len() + best_gif.len() / 2 {
                image = Some((b, "webp"));
            }
        }
    }
    if image.is_none() && best_gif.len() < body.len() {
        image = Some((best_gif, "gif"));
    }
    match (image, video) {
        (Some((b, e)), v) => Some((b, e, v)),
        (None, Some(v)) => Some((body.to_vec(), "gif", Some(v))),
        (None, None) => None,
    }
}

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
    let result = optimize_gif_files(&body, &src, &opt, &web, &vid, max_px, want_video).await;
    for p in [&src, &opt, &web, &vid] {
        let _ = tokio::fs::remove_file(p).await;
    }
    result.unwrap_or_else(|| (body.to_vec(), ext, None))
}

async fn save_upload(dir: &std::path::Path, img: &[u8], ext: &str, video: Option<Vec<u8>>) -> std::io::Result<String> {
    let stem = uuid::Uuid::new_v4().simple().to_string();
    let name = if video.is_some() { format!("{stem}.v.{ext}") } else { format!("{stem}.{ext}") };
    tokio::fs::write(dir.join(&name), img).await?;
    if let Some(v) = video {
        let _ = tokio::fs::write(dir.join(format!("{stem}.mp4")), &v).await;
    }
    Ok(name)
}

async fn remove_upload_file(dir: &std::path::Path, f: &str) {
    let _ = tokio::fs::remove_file(dir.join(f)).await;
    if let Some((stem, _)) = f.split_once(".v.") {
        let _ = tokio::fs::remove_file(dir.join(format!("{stem}.mp4"))).await;
    }
}

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
    })
}

const FRIEND_COLS: &str = "u.id, u.display_name, u.avatar, u.accent, u.banner, u.banner_color, u.status, u.crop";

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
    action: String,
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
    let _ = db.execute("DELETE FROM sessions WHERE user_id=?1 AND token<>?2", params![user.id, token]);
    Ok(StatusCode::OK)
}

#[derive(Deserialize)]
struct AccDel {
    password: String,
}

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

const BOOL_SETTINGS: [&str; 11] = [
    "track_history", "save_state", "crossfade", "explicit", "offline_pl", "proxy", "eq_on", "delay_on", "reverb_on", "discord_rpc", "show_errors",
];
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

async fn hist_clear(State(st): State<AppState>, headers: HeaderMap) -> Result<StatusCode, ApiErr> {
    let u = require(&st, &headers)?;
    let db = st.db.lock().unwrap();
    db.execute("DELETE FROM history WHERE user_id=?1", [u.id]).map_err(|_| e500())?;
    Ok(StatusCode::OK)
}

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

fn canon_artist(name: &str) -> String {
    let n = name.trim();
    alias_map().get(&art_norm(n)).cloned().unwrap_or_else(|| n.to_string())
}

fn split_artists(s: &str) -> Vec<String> {
    const SEPS: [&str; 11] = [",", "&", ";", "/", "、", "・", " x ", " ft. ", " ft ", " feat. ", " feat "];
    let lower = s.to_ascii_lowercase();
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

const SYNC_KEYS: [&str; 5] = ["listen", "plays", "dislike", "follow", "jump"];
const SYNC_MAX_BYTES: usize = 1_500_000;

fn jnum(v: &Value, k: &str) -> i64 {
    v.get(k).and_then(|x| x.as_i64().or_else(|| x.as_f64().map(|f| f as i64))).unwrap_or(0)
}

fn entry_ok(e: &Value) -> bool {
    e.is_object() && e.get("t").map(|t| valid_track(t).is_ok()).unwrap_or(false)
}

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

    let mut per_artist: HashMap<String, u32> = HashMap::new();
    let mut out: Vec<Value> = Vec::new();
    for (k, _) in ranked {
        let Some(t) = meta.get(k).and_then(|js| serde_json::from_str::<Value>(js).ok()) else { continue };
        let title = t.get("title").and_then(|x| x.as_str()).unwrap_or("").to_lowercase();
        let dur = t.get("dur").and_then(|x| x.as_u64()).unwrap_or(0);
        if dur > 720 || ["truyện", "audiobook", "audio book", "podcast", "full album", "chương "].iter().any(|w| title.contains(w)) {
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

async fn upload_bg(State(st): State<AppState>, headers: HeaderMap, body: Bytes) -> R<User> {
    let user = require(&st, &headers)?;
    if body.len() > 10 * 1024 * 1024 {
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

async fn serve_icon(State(st): State<AppState>) -> Response {
    match tokio::fs::read(st.app_dir.join("icon.png")).await {
        Ok(b) => ([(header::CONTENT_TYPE, "image/png"), (header::CACHE_CONTROL, "no-cache")], b).into_response(),
        Err(_) => StatusCode::NOT_FOUND.into_response(),
    }
}

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
    let parts: Vec<String> = col(1)
        .split(" \u{2022} ")
        .map(|x| x.trim().to_string())
        .filter(|x| !x.is_empty())
        .collect();
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
    let vt = {
        let s = item.to_string();
        let key = "\"musicVideoType\":\"MUSIC_VIDEO_TYPE_";
        s.find(key)
            .map(|i| s[i + key.len()..].chars().take_while(|c| c.is_ascii_uppercase() || *c == '_').collect::<String>())
            .unwrap_or_default()
    };
    Some(json!({ "src": "yt", "id": id, "title": title, "artist": artist, "image": image, "url": "", "dur": dur, "explicit": explicit, "vt": vt }))
}

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
                    out.push(json!({ "src": "yt", "id": id, "title": title, "artist": artist, "image": image, "url": "", "dur": 0, "explicit": false }));
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
    let (allow_explicit, use_proxy) = match require(&st, &headers) {
        Ok(user) => user_net_prefs(&st, user.id),
        Err(_) => (false, false),
    };
    let client = pick_client(&st, use_proxy);
    let text: String = q.q.unwrap_or_default().trim().chars().take(100).collect();
    if text.is_empty() {
        return Err(bad("Thiếu từ khoá tìm kiếm"));
    }
    let fail = |m: &str| -> ApiErr { (StatusCode::BAD_GATEWAY, m.to_string()) };
    let filt: Option<String> = q.f.as_deref().map(|x| x.trim().to_lowercase()).filter(|x| !x.is_empty());
    let ckey = format!("{}|{}|{}|{}", text.to_lowercase(), allow_explicit as u8, use_proxy as u8, filt.clone().unwrap_or_default());
    if let Some((t, v)) = search_cache().lock().unwrap().get(&ckey) {
        if now_secs().saturating_sub(*t) < 600 {
            return Ok(Json(v.clone()));
        }
    }
    let mut body = json!({
        "context": { "client": { "clientName": "WEB_REMIX", "clientVersion": "1.20250310.01.00", "hl": "en", "gl": "VN" } },
        "query": text,
    });
    match filt.as_deref() {
        None => body["params"] = json!("EgWKAQIIAWoKEAkQBRAKEAMQBA=="),
        Some(f) => {
            if let Some(p) = yt_filter_params(f) {
                body["params"] = json!(p);
            }
        }
    }
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
    let mut ents: Vec<Value> = Vec::new();
    if filt.is_some() {
        yt_collect_all(&v, &mut out, &mut ents);
    } else {
        yt_collect(&v, &mut out);
    }
    if !allow_explicit {
        out.retain(|t| !t.get("explicit").and_then(|x| x.as_bool()).unwrap_or(false));
    }
    out.truncate(30);
    ents.truncate(30);
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

#[derive(Deserialize)]
struct YtSimQ {
    artist: Option<String>,
    genre: Option<String>,
}

static SIMILAR_CACHE: std::sync::OnceLock<Mutex<HashMap<String, (u64, Value)>>> = std::sync::OnceLock::new();
fn similar_cache() -> &'static Mutex<HashMap<String, (u64, Value)>> {
    SIMILAR_CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

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

fn norm_name(s: &str) -> String {
    s.chars().filter(|c| c.is_alphanumeric()).flat_map(|c| c.to_lowercase()).collect()
}
fn credit_names(s: &str) -> Vec<String> {
    let t = s.to_lowercase().replace(" feat. ", ",").replace(" feat ", ",").replace(" ft. ", ",").replace(" ft ", ",").replace(" x ", ",");
    t.split(|c: char| matches!(c, ',' | '&' | ';' | '/' | '、' | '・')).map(norm_name).filter(|x| !x.is_empty()).collect()
}
fn is_blocked_track(t: &Value, block: &HashSet<String>) -> bool {
    let a = t.get("artist").and_then(|x| x.as_str()).unwrap_or("");
    credit_names(a).iter().any(|n| block.contains(n) || block.iter().any(|b| b.chars().count() >= 4 && n.contains(b.as_str())))
}

fn yt_ctx() -> Value {
    json!({ "client": { "clientName": "WEB_REMIX", "clientVersion": "1.20250310.01.00", "hl": "en", "gl": "VN" } })
}

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

async fn yt_songs(client: &reqwest::Client, query: &str) -> Vec<Value> {
    let body = json!({ "context": yt_ctx(), "query": query, "params": "EgWKAQIIAWoKEAkQBRAKEAMQBA==" });
    let mut out: Vec<Value> = Vec::new();
    if let Some(v) = yt_post(client, "search", body).await {
        yt_collect(&v, &mut out);
    }
    out
}

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

async fn yt_similar(State(st): State<AppState>, headers: HeaderMap, Query(q): Query<YtSimQ>) -> R<Value> {
    let (allow_explicit, use_proxy) = match require(&st, &headers) {
        Ok(user) => user_net_prefs(&st, user.id),
        Err(_) => (false, false),
    };
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
    let seed_ent = exact.or(near);
    let aid: String = match seed_ent.and_then(|e| e.get("id")).and_then(|x| x.as_str()) {
        Some(s) => s.to_string(),
        None => {
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

    let mut block: HashSet<String> = HashSet::new();
    block.insert(norm_name(&name));
    if !seed_title.is_empty() {
        block.insert(norm_name(&seed_title));
    }
    block.retain(|x| !x.is_empty());

    let (bv_opt, seed_songs) = futures_util::future::join(
        yt_post(&client, "browse", json!({ "context": yt_ctx(), "browseId": aid })),
        yt_songs(&client, &name),
    )
    .await;
    let Some(bv) = bv_opt else {
        return Ok(Json(empty));
    };
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

    let lists = futures_util::future::join_all(rel.iter().map(|(n, _)| {
        let qs = match spec {
            Some((kw, _)) => format!("{n} {kw}"),
            None => n.clone(),
        };
        let c = client.clone();
        async move { yt_songs(&c, &qs).await }
    }))
    .await;
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
            mine.sort_by_key(|t| !genre_match(t, words));
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

#[derive(Deserialize)]
struct YtFreshQ {
    artist: Option<String>,
}

static FRESH_CACHE: std::sync::OnceLock<Mutex<HashMap<String, (u64, Value)>>> = std::sync::OnceLock::new();
fn fresh_cache() -> &'static Mutex<HashMap<String, (u64, Value)>> {
    FRESH_CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}
static PUBDAY_CACHE: std::sync::OnceLock<Mutex<HashMap<String, i64>>> = std::sync::OnceLock::new();
fn pubday_cache() -> &'static Mutex<HashMap<String, i64>> {
    PUBDAY_CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = (if y >= 0 { y } else { y - 399 }) / 400;
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}
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

async fn yt_fresh(State(st): State<AppState>, headers: HeaderMap, Query(q): Query<YtFreshQ>) -> R<Value> {
    let (allow_explicit, use_proxy) = match require(&st, &headers) {
        Ok(user) => user_net_prefs(&st, user.id),
        Err(_) => (false, false),
    };
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

    let Some(bv) = yt_post(&client, "browse", json!({ "context": yt_ctx(), "browseId": aid })).await else {
        return Ok(Json(empty));
    };
    let mut cards: Vec<(String, i64, String, String)> = Vec::new();
    yt_album_cards(&bv, &mut cards);
    let mut seen_a: HashSet<String> = HashSet::new();
    cards.retain(|c| c.1 >= cur_year - 1 && seen_a.insert(c.0.clone()));
    cards.sort_by_key(|c| std::cmp::Reverse(c.1));
    cards.truncate(10);
    if cards.is_empty() {
        return Ok(Json(empty));
    }

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

    let days = futures_util::future::join_all(cand.iter().map(|t| {
        let cl = client.clone();
        let id = t.get("id").and_then(|x| x.as_str()).unwrap_or("").to_string();
        async move { yt_pubday(&cl, &id).await }
    }))
    .await;
    let mut out: Vec<(i64, Value)> = Vec::new();
    for (mut t, d) in cand.into_iter().zip(days.into_iter()) {
        let Some(d) = d else { continue };
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

fn norm_log(x: f64, lo: f64, hi: f64) -> f64 {
    if x <= 0.0 {
        return 0.0;
    }
    ((x.log10() - lo) / (hi - lo)).clamp(0.0, 1.0)
}

fn track_heat(rank: f64, views: f64, likes: f64) -> f64 {
    let mut w = 0.45;
    let mut s = 0.45 * rank;
    if views > 0.0 {
        s += 0.35 * norm_log(views, 4.0, 9.0);
        w += 0.35;
    }
    if likes > 0.0 {
        s += 0.20 * norm_log(likes, 2.5, 7.0);
        w += 0.20;
    }
    s / w
}

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

#[derive(Deserialize)]
struct YtThisQ {
    name: Option<String>,
    id: Option<String>,
}

async fn yt_this_is(State(st): State<AppState>, headers: HeaderMap, Query(q): Query<YtThisQ>) -> R<Value> {
    let (allow_explicit, use_proxy) = match require(&st, &headers) {
        Ok(user) => user_net_prefs(&st, user.id),
        Err(_) => (false, false),
    };
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

#[derive(Deserialize)]
struct YtBrowseQ {
    id: Option<String>,
    kind: Option<String>,
}

async fn yt_browse_music(State(st): State<AppState>, headers: HeaderMap, Query(q): Query<YtBrowseQ>) -> R<Value> {
    let (allow_explicit, use_proxy) = match require(&st, &headers) {
        Ok(user) => user_net_prefs(&st, user.id),
        Err(_) => (false, false),
    };
    let client = pick_client(&st, use_proxy);
    let id: String = q.id.unwrap_or_default().trim().chars().take(80).collect();
    if id.is_empty() || !id.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_') {
        return Err(bad("Mục không hợp lệ"));
    }
    let kind = q.kind.unwrap_or_default();
    let fail = |m: &str| -> ApiErr { (StatusCode::BAD_GATEWAY, m.to_string()) };
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
const AUDIO_FIRST_CHUNK: u64 = 256 * 1024;
const AUDIO_TTL: u64 = 3 * 60 * 60;

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
        90 => {
            let _ = client;
            let (_p, total, mime) = ytdlp_local(vid, webm, use_proxy).await?;
            Ok((format!("local:{vid}"), total, mime, 90))
        }
        99 => resolve_piped(client, vid, webm).await,
        n => resolve_audio(client, vid, webm, n).await,
    }
}

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

async fn resolve_race(client: &reqwest::Client, vid: &str, webm: bool, cands: &[usize]) -> Result<(String, u64, String, usize), String> {
    let mut set = tokio::task::JoinSet::new();
    for &ci in cands {
        let c = client.clone();
        let v = vid.to_string();
        set.spawn(async move { (ci, resolve_audio(&c, &v, webm, ci).await) });
    }
    let mut last = "Không lấy được luồng âm thanh".to_string();
    while let Some(joined) = set.join_next().await {
        match joined {
            Ok((_, Ok(x))) => {
                set.abort_all();
                return Ok(x);
            }
            Ok((ci, Err(e))) => {
                eprintln!("[audio] {vid} nguồn {ci}: {e}");
                audio_mark_bad(vid, ci);
                last = e;
            }
            Err(_) => {}
        }
    }
    Err(last)
}

static AUDIO_GATES: std::sync::OnceLock<Mutex<HashMap<String, std::sync::Arc<tokio::sync::Mutex<()>>>>> = std::sync::OnceLock::new();
fn audio_gate(key: &str) -> std::sync::Arc<tokio::sync::Mutex<()>> {
    let mut m = AUDIO_GATES.get_or_init(|| Mutex::new(HashMap::new())).lock().unwrap();
    if m.len() > 500 {
        m.clear();
    }
    m.entry(key.to_string()).or_insert_with(|| std::sync::Arc::new(tokio::sync::Mutex::new(()))).clone()
}

fn ytdlp_fmt(webm: bool) -> &'static str {
    if webm {
        "251/bestaudio[ext=webm]/bestaudio"
    } else {
        "140/bestaudio[ext=m4a]/bestaudio"
    }
}

fn ytdlp_base(use_proxy: bool) -> tokio::process::Command {
    let bin = std::env::var("YTDLP_PATH").ok().filter(|x| !x.trim().is_empty()).unwrap_or_else(|| "yt-dlp".to_string());
    let mut cmd = tokio::process::Command::new(&bin);
    cmd.arg("--no-playlist").arg("--no-warnings");
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
    cmd.creation_flags(0x0800_0000);
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
            continue;
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
    let lock = YTDLP_DL_LOCK.get_or_init(|| tokio::sync::Mutex::new(()));
    let _guard = lock.lock().await;
    if let Some(x) = find_local_audio(vid, webm) {
        return Ok(x);
    }
    let dir = audio_cache_dir();
    let _ = tokio::fs::create_dir_all(&dir).await;
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

static YTDLP_VID_LOCKS: std::sync::OnceLock<std::sync::Mutex<std::collections::HashMap<String, std::sync::Arc<tokio::sync::Mutex<()>>>>> = std::sync::OnceLock::new();

fn find_local_video(vid: &str) -> Option<(PathBuf, u64, String)> {
    let prefix = format!("{vid}_v.");
    for e in std::fs::read_dir(audio_cache_dir()).ok()?.flatten() {
        let name = e.file_name().to_string_lossy().to_string();
        if !name.starts_with(&prefix) {
            continue;
        }
        let ext = name[prefix.len()..].to_lowercase();
        if ext.contains("part") || ext.contains("ytdl") || ext.contains("temp") {
            continue;
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

async fn ytdlp_video(vid: &str, use_proxy: bool) -> Result<(PathBuf, u64, String), String> {
    if let Some(x) = find_local_video(vid) {
        return Ok(x);
    }
    let lock = {
        let m = YTDLP_VID_LOCKS.get_or_init(|| std::sync::Mutex::new(std::collections::HashMap::new()));
        let mut g = m.lock().unwrap();
        g.entry(vid.to_string()).or_insert_with(|| std::sync::Arc::new(tokio::sync::Mutex::new(()))).clone()
    };
    let _guard = lock.lock().await;
    if let Some(x) = find_local_video(vid) {
        return Ok(x);
    }
    let dir = audio_cache_dir();
    let _ = tokio::fs::create_dir_all(&dir).await;
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
    let h: u32 = std::env::var("YT_VIDEO_HEIGHT")
        .ok()
        .and_then(|x| x.trim().parse::<u32>().ok())
        .filter(|n| *n >= 144 && *n <= 2160)
        .unwrap_or(360);
    let fmt = format!("bv[height<={h}][ext=mp4][vcodec^=avc1]/bv[height<={h}][ext=mp4]/bv[height<={h}]/b[height<={h}]/bv/b");
    let template = dir.join(format!("{vid}_v.%(ext)s"));
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
    find_local_video(vid).ok_or_else(|| "yt-dlp: tải xong nhưng không thấy file video".to_string())
}

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
    match ytdlp_video(&id, use_proxy).await {
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
    let client = pick_client(&st, use_proxy);
    let webm = q.f.as_deref() == Some("webm");
    let ckey = format!("{id}|{}|{}", use_proxy as u8, webm as u8);
    let (start, end_req) = parse_range(&headers);
    let mut notes = String::new();

    let order = audio_order();
    let mut pos = 0usize;
    let mut retried: HashSet<usize> = HashSet::new();
    for _round in 0..=order.len() * 2 {
        let cached = {
            let c = st.audio.lock().unwrap();
            c.get(&ckey).filter(|e| now_secs().saturating_sub(e.3) < AUDIO_TTL).cloned()
        };
        let (url, size, mime, ci) = match cached {
            Some((u, s, m, _, ci)) => (u, s, m, ci),
            None => {
                let gate = audio_gate(&ckey);
                let _gate_guard = gate.lock().await;
                let mut res = {
                    let c = st.audio.lock().unwrap();
                    c.get(&ckey).filter(|e| now_secs().saturating_sub(e.3) < AUDIO_TTL).map(|(u, s, m, _, ci)| (u.clone(), *s, m.clone(), *ci))
                };
                if res.is_none() && pos == 0 {
                    let cands: Vec<usize> = order.iter().copied().filter(|s| *s < 90 && !audio_bad(&id, *s)).collect();
                    if cands.len() > 1 {
                        match resolve_race(&client, &id, webm, &cands).await {
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

        let mut got: Option<Bytes> = None;
        for variant in 0..3 {
            let req = if variant == 0 {
                client.get(&url).header("range", format!("bytes={start}-{end}"))
            } else if variant == 2 {
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
                .header(header::CACHE_CONTROL, "private, max-age=600")
                .body(Body::from(b))
                .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response());
        }
        eprintln!("[audio] {id} googlevideo từ chối đoạn {start}-{end}: {notes}");
        st.audio.lock().unwrap().remove(&ckey);
        if ci < 90 && retried.insert(ci) {
            pos = order.iter().position(|x| *x == ci).unwrap_or(0);
            continue;
        }
        audio_mark_bad(&id, ci);
    }
    (StatusCode::BAD_GATEWAY, format!("googlevideo từ chối: {notes}")).into_response()
}

fn yt_playlist_id(input: &str) -> Option<String> {
    let s = input.trim();
    let ok = |x: &str| !x.is_empty() && x.len() <= 64 && x.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_');
    if let Some(i) = s.find("list=") {
        let rest = &s[i + 5..];
        let id: String = rest.chars().take_while(|c| c.is_ascii_alphanumeric() || *c == '-' || *c == '_').collect();
        return if ok(&id) { Some(id) } else { None };
    }
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
    cmd.creation_flags(0x0800_0000);
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
    Ok(Json(json!({ "linked": !id.is_empty(), "name": name, "avatar": avatar, "sdk": !refresh.is_empty() })))
}

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
