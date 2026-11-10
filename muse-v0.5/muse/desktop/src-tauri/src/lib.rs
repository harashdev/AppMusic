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

// Địa chỉ server Muse, cố định lúc build: MUSE_URL=https://muse.example.com cargo tauri build
const SERVER_URL: &str = match option_env!("MUSE_URL") {
    Some(u) => u,
    None => "http://localhost:3000",
};

#[cfg(desktop)]
mod discord_rpc {
use super::*;
// ===== Discord Rich Presence qua IPC cục bộ (không cần thư viện ngoài) =====
// Giao thức: mỗi khung = opcode (u32 LE) + độ dài (u32 LE) + JSON. Windows: named pipe, Linux/macOS: unix socket.
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
        return Err(v.pointer("/data/message").and_then(|x| x.as_str()).unwrap_or("Discord từ chối kết nối").to_string());
    }
    let user = v
        .pointer("/data/user/global_name")
        .and_then(|x| x.as_str())
        .or_else(|| v.pointer("/data/user/username").and_then(|x| x.as_str()))
        .unwrap_or("Discord")
        .to_string();
    d.conn = Some(c);
    d.user = user.clone();
    Ok(user)
}

fn set_activity(d: &mut Discord, activity: Option<Value>) -> Result<(), String> {
    let nonce = format!("{}", SystemTime::now().duration_since(UNIX_EPOCH).map(|t| t.as_nanos()).unwrap_or(0));
    let payload = json!({ "cmd": "SET_ACTIVITY", "args": { "pid": std::process::id(), "activity": activity }, "nonce": nonce });
    for attempt in 0..2 {
        if d.conn.is_none() {
            handshake(d)?;
        }
        let c = d.conn.as_mut().unwrap();
        let ok = send_frame(&mut **c, 1, &payload).is_ok() && read_frame(&mut **c).is_ok();
        if ok {
            return Ok(());
        }
        d.conn = None; // Discord đã đóng/khởi động lại -> kết nối lại một lần
        if attempt == 1 {
            break;
        }
    }
    Err("Mất kết nối với Discord".into())
}

#[tauri::command]
pub async fn discord_connect(state: State<'_, DiscordState>, app_id: String) -> Result<String, String> {
    if app_id.trim().is_empty() || !app_id.chars().all(|c| c.is_ascii_digit()) {
        return Err("DISCORD_CLIENT_ID không hợp lệ".into());
    }
    let mut d = state.0.lock().map_err(|_| "lỗi nội bộ".to_string())?;
    d.conn = None;
    d.app_id = app_id;
    handshake(&mut d)
}

#[tauri::command]
pub async fn discord_activity(
    state: State<'_, DiscordState>,
    title: String,
    artist: String,
    image: String,
    start_ms: i64,
    end_ms: i64,
) -> Result<(), String> {
    let mut d = state.0.lock().map_err(|_| "lỗi nội bộ".to_string())?;
    if d.app_id.is_empty() {
        return Err("Chưa kết nối Discord".into());
    }
    let cut = |s: &str, n: usize| -> String { s.chars().take(n).collect() };
    let mut a = json!({
        "type": 2,
        "details": cut(&title, 120),
        "state": if artist.is_empty() { "Muse".to_string() } else { cut(&artist, 120) },
    });
    let mut ts = serde_json::Map::new();
    if start_ms > 0 {
        ts.insert("start".into(), json!(start_ms / 1000));
    }
    if end_ms > 0 {
        ts.insert("end".into(), json!(end_ms / 1000));
    }
    a["timestamps"] = Value::Object(ts);
    if image.starts_with("https://") {
        a["assets"] = json!({ "large_image": image, "large_text": cut(&title, 120) });
    }
    set_activity(&mut d, Some(a))
}

#[tauri::command]
pub async fn discord_clear(state: State<'_, DiscordState>) -> Result<(), String> {
    let mut d = state.0.lock().map_err(|_| "lỗi nội bộ".to_string())?;
    if d.conn.is_none() {
        return Ok(());
    }
    set_activity(&mut d, None)
}
}


#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    let builder = tauri::Builder::default();

    #[cfg(desktop)]
    let builder = builder
        .manage(discord_rpc::DiscordState(std::sync::Mutex::new(discord_rpc::Discord::default())))
        .invoke_handler(tauri::generate_handler![
            discord_rpc::discord_connect,
            discord_rpc::discord_activity,
            discord_rpc::discord_clear
        ]);

    builder
        .setup(|app| {
            let raw = if SERVER_URL.is_empty() { "http://localhost:3000" } else { SERVER_URL };
            let url = raw.parse().expect("MUSE_URL không hợp lệ");
            let win = WebviewWindowBuilder::new(app, "main", WebviewUrl::External(url)).title("Muse");
            #[cfg(desktop)]
            let win = win.inner_size(1200.0, 800.0).min_inner_size(420.0, 600.0);
            win.build()?;
            Ok(())
        })
        .run(tauri::generate_context!())
        .expect("lỗi khi chạy Muse");
}
