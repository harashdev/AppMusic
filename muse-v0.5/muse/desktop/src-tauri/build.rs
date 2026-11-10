use std::{env, fs, path::Path};

// Tạo capability cho phép trang Muse (tải từ MUSE_URL) gọi các lệnh Discord Rich Presence của app.
fn main() {
    let url = env::var("MUSE_URL").unwrap_or_default();
    let url = if url.trim().is_empty() { "http://localhost:3000".to_string() } else { url.trim().trim_end_matches('/').to_string() };
    let cap = format!(
        r#"{{"identifier":"remote-muse","description":"Cho phép trang Muse từ server gọi lệnh Discord","windows":["main"],"remote":{{"urls":["{u}","{u}/*"]}},"permissions":["core:default"]}}"#,
        u = url
    );
    let dir = Path::new("capabilities");
    fs::create_dir_all(dir).expect("không tạo được thư mục capabilities");
    fs::write(dir.join("remote-muse.json"), cap).expect("không ghi được capability");
    println!("cargo:rerun-if-env-changed=MUSE_URL");
    tauri_build::build()
}
