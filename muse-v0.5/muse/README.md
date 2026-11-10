# Muse – nghe nhạc cùng bạn bè (Rust)

```
muse/
├─ server/   Axum + SQLite: đăng nhập, avatar GIF, bạn bè, playlist, yêu thích, lịch sử, phòng nghe chung + giao diện web
└─ desktop/  Tauri v2: app desktop kết nối tới server
```

## Chạy server
```bash
cd server
export DONATE_URL=https://ko-fi.com/ban    # tuỳ chọn
cargo run --release                        # http://localhost:3000
```
Dữ liệu: `data/muse.db` (SQLite, chế độ WAL) + `data/avatars/`. Khi backup hãy chép **cả thư mục `data/`** (gồm `muse.db-wal` và `muse.db-shm`), nếu không sẽ mất dữ liệu mới nhất.
Đưa lên mạng: VPS + Caddy/Nginx có HTTPS (cần cho wss:// và YouTube player).

## Đóng gói desktop
```bash
cargo install tauri-cli --version "^2"
cd desktop/src-tauri
cargo tauri icon /duong/dan/logo.png
MUSE_URL=https://muse.example.com cargo tauri build
```

## Ra file .exe (Windows)
Xem hướng dẫn trong cuộc trò chuyện, hoặc dùng GitHub Actions: đẩy thư mục này lên GitHub,
đặt biến `MUSE_URL` (Settings → Secrets and variables → Actions → Variables), rồi chạy workflow
"Build desktop (Muse)" và tải file .exe ở mục Artifacts.

## Nguồn nhạc & tính năng (v0.3)
- Nguồn nhạc duy nhất: YouTube / YouTube Music (tìm qua InnerTube phía server, phát bằng player chính thức của YouTube). SoundCloud, Jamendo và Audius đã được gỡ; bài cũ của hai nguồn sau trong yêu thích/lịch sử/playlist vẫn phát được.
- Trang Khám phá và Thịnh hành lấy từ các truy vấn tìm kiếm YouTube Music (không phải bảng xếp hạng chính thức).
- **SponsorBlock**: luôn bật, tự bỏ qua đoạn quảng cáo/intro/outro trong video YouTube (không còn mục cài đặt riêng).
- **Return YouTube Dislike**: hiện số 👍/👎 của video YouTube cạnh tên nghệ sĩ.
- **Lời bài hát**: SimpMusic Lyrics (chính) → LRCLIB (dự phòng); ưu tiên bản có thời gian (karaoke).
- Chưa có: nhận diện bài hát bằng Shazam.


## Giao diện v0.4
- **Banner hồ sơ kiểu Discord**: tải ảnh PNG/JPG/WEBP hoặc **GIF động** (≤ 10MB) ở trang *Hồ sơ & giao diện*; chưa có ảnh thì dùng màu banner. Kèm dòng **trạng thái** hiện trong bong bóng cạnh avatar. Bấm avatar của mình (góc trên) hoặc của bạn bè để xem thẻ hồ sơ. Ảnh lưu ở `data/banners/` (nhớ backup cùng thư mục `data/`).
- **Nền hồ sơ** (trước đây là "Nền/skin"): chọn nền hoặc tải ảnh/GIF riêng, có khung **xem trước** ngay bên phải (chưa lưu thì chưa áp dụng) và nút *Xem thử toàn app*.
- **Bấm vào thanh nhạc** để mở panel *Đang phát* bên phải, với 3 tab: Đang phát · Lời (karaoke, bấm vào dòng để tua) · Tiếp theo (hàng đợi). Bấm lại để đóng.
- Trang chủ có mục **Chọn nhanh** (lưới bài gọn) lấy cảm hứng từ SimpMusic.
- DB cũ tự được nâng cấp (thêm cột `banner`, `banner_color`, `status`). API mới: `POST/DELETE /api/banner`.
- **Modal chỉnh ảnh** (kiểu Discord) hiện ngay khi chọn ảnh/GIF cho avatar hoặc banner: kéo để di chuyển, cuộn chuột hoặc kéo thanh để zoom, giữ thanh X/Y/Zoom; zoom nhỏ hơn mức phủ kín để thấy trọn ảnh (phần trống lấy màu banner). Ảnh gốc giữ nguyên (GIF vẫn động), thông số lưu trong cột `crop`.
- Thẻ hồ sơ: banner nằm phía sau avatar và tên.
- Thanh điều hướng trang chủ: nút **Tâm trạng** mở danh sách dài 17 tâm trạng (Thư giãn, Buồn, Lãng mạn, Tiệc tùng, Ngủ ngon…); chọn một mục để xem các mục nhạc theo tâm trạng đó.


## Cài đặt & Hồ sơ (API mới)
- `POST /api/account/name`, `POST /api/account/password` – đổi tên hiển thị / mật khẩu (đổi mật khẩu sẽ đăng xuất các thiết bị khác).
- `GET|POST /api/settings` – cài đặt theo tài khoản (`track_history`, `save_state`, `crossfade`, `crossfade_secs`).
- `DELETE /api/history` – xóa lịch sử nghe.
- `GET /api/backup`, `POST /api/restore`, `POST /api/import` – sao lưu, khôi phục, nhập danh sách phát (JSON).


## v0.5 – Cài đặt mới
Biến môi trường mới cho server (tuỳ chọn): `YT_PROXY` (vd. `http://host:port` hoặc `socks5://host:port`), `DISCORD_CLIENT_ID` (Application ID tạo ở discord.com/developers).

- **Bộ chỉnh âm 10 dải / Độ trễ (echo) / Độ vang (phòng, hội trường, tấm kim loại)**: xử lý bằng Web Audio. Vì player iframe của YouTube không cho can thiệp âm thanh, khi bật bất kỳ hiệu ứng nào nhạc YouTube sẽ phát qua `GET /api/yt/audio/:id` (server lấy luồng âm thanh qua InnerTube rồi chuyển tiếp từng đoạn Range). Nếu không lấy được luồng, app tự quay về player YouTube. Khi dùng luồng này sẽ không có chế độ xem video.
- **Phát bài hát chứa nội dung nhạy cảm**: mặc định tắt; khi tắt, bài có nhãn Explicit bị ẩn khỏi kết quả tìm kiếm và bị bỏ qua trong hàng đợi.
- **Giữ danh sách phát hiển thị khi offline**: lưu danh sách phát vào bộ nhớ trình duyệt; mất mạng vẫn mở được app và xem danh sách (kèm `sw.js` để mở giao diện offline).
- **Proxy**: bật thì tìm kiếm và luồng âm thanh của bạn đi qua `YT_PROXY` trên server (nhạc được phát qua luồng của server thay vì iframe).
- **Discord Rich Presence** (chỉ app desktop): cần `DISCORD_CLIENT_ID` trên server và Discord đang mở trên máy. Bấm *Kết nối Discord* rồi bật *Kích hoạt Rich Presence*. App desktop tự tạo capability cho `MUSE_URL` lúc build (`desktop/src-tauri/build.rs`).
- API: `/api/settings` có thêm `explicit, offline_pl, proxy, discord_rpc, eq_on, eq_gains[10], eq_pre, delay_on/ms/fb/mix, reverb_on/type/mix`.


## Android (app native: Kotlin + Jetpack Compose + Media3)
```
android/   app Android native, dùng chính server này làm backend (cùng tài khoản, yêu thích, playlist, lịch sử)
```
- Giao diện Compose (Material 3, nền tối): Đăng nhập → Tìm kiếm · Thư viện (Yêu thích / Playlist / Lịch sử) · Tôi; thanh mini-player, màn hình *Đang phát* (tua, trộn, lặp, hàng đợi).
- Phát nhạc bằng **ExoPlayer trong dịch vụ nền** (`PlaybackService`): tiếp tục phát khi tắt màn hình/thoát app, có thông báo điều khiển, màn hình khóa, nút tai nghe/Bluetooth. Nhạc YouTube đi qua `GET /api/yt/audio/:id?t=token` của server (không dùng iframe).
- `ResumingDataSource` tự nối các đoạn 2MB mà server trả (HTTP 206) thành một luồng liền mạch.
- Lần đầu mở app nhập **địa chỉ server** (vd. `https://muse.example.com` hoặc địa chỉ Tailscale `https://tên-máy.tên-tailnet.ts.net`). Có cho phép `http://` để thử trong mạng riêng.

**Lấy APK:** đẩy code lên GitHub → Actions → "Build desktop (Muse)" → tải `muse-android` ở Artifacts (APK debug-signed, cài thẳng được).
**Build trên máy:** mở thư mục `android/` bằng Android Studio (Hedgehog trở lên) → Run. Hoặc `gradle assembleDebug` (JDK 17 + Android SDK 34).
**Phát hành lên Google Play:** đổi `signingConfig` của bản `release` trong `app/build.gradle.kts` sang keystore riêng.

Chưa có trong app native (bản web/desktop vẫn có): phòng nghe chung + mời bạn bè (WebSocket), Bộ chỉnh âm/Echo/Vang, lời bài hát karaoke, Cài đặt, banner/avatar GIF, đăng nhập Discord.
(Phần `desktop/` Tauri vẫn dùng để đóng gói bản Windows.)

## Đăng nhập Discord (bắt buộc để bật Rich Presence)
Người dùng phải đăng nhập Discord (OAuth2, scope `identify`) ở *Cài đặt → Tích hợp Discord → Đăng nhập Discord* thì mới bật được Rich Presence. Server từ chối `discord_rpc=true` nếu tài khoản chưa liên kết; hủy liên kết sẽ tắt luôn Rich Presence.

Cấu hình (3 biến môi trường trên server):
- `DISCORD_CLIENT_ID` – Application ID
- `DISCORD_CLIENT_SECRET` – OAuth2 → Client Secret (giữ bí mật, không đưa vào app)
- `PUBLIC_URL` – địa chỉ công khai của server, vd. `https://muse.example.com` (giống `MUSE_URL` của app)

Ở discord.com/developers → ứng dụng của bạn → **OAuth2 → Redirects** thêm đúng: `https://muse.example.com/api/discord/callback`.

API: `GET /api/discord/status`, `POST /api/discord/link` (trả URL đăng nhập), `DELETE /api/discord/link`, `GET /api/discord/callback`. DB tự thêm cột `discord_id`, `discord_name`, `discord_avatar` và bảng `discord_states`.

Lưu ý: Rich Presence thật sự (hiện "Đang nghe…" trên hồ sơ) vẫn chỉ chạy ở app desktop (qua Discord IPC). Trên Android (app native) chưa có mục này; muốn hiện trạng thái từ điện thoại cần Discord Social SDK cho Android (code Kotlin/Java riêng).
