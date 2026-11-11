
    
  
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

