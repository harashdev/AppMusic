
    
  
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

