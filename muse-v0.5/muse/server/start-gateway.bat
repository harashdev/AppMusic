@echo off
chcp 65001 >nul
rem === Cong nha (gateway): chay tren may o nha de nhac/album/tim kiem YouTube dung IP nha ===
rem Server chinh (VPS/Render) chi con luu tai khoan, ban be, avatar, banner, phong nghe chung.
rem Chay file nay roi mo http://127.0.0.1:38417 bang trinh duyet. (App desktop BBeat tu bat cong nha, khong can file nay.)
rem Dia chi server chinh (giong MUSE_URL cua app):
set GATEWAY_REMOTE=https://appmusic-ur3z.onrender.com
set PORT=38417
rem Chi nhan ket noi tu chinh may nay. Doi thanh 0.0.0.0 neu muon dien thoai cung Wi-Fi mo http://IP-may:38417
set BIND=127.0.0.1
rem Neu YouTube bao "Sign in to confirm you're not a bot": de ten trinh duyet da dang nhap YouTube (firefox, edge, chrome, brave)
set YT_BROWSER=firefox
set YTDLP_PATH=
set YT_COOKIES=
if exist gateway.local.bat call gateway.local.bat
rem yt-dlp can mot JavaScript runtime (Deno) de giai ma YouTube
set "PATH=%CD%;%PATH%"
if not exist deno.exe powershell -NoProfile -Command "Invoke-WebRequest https://github.com/denoland/deno/releases/latest/download/deno-x86_64-pc-windows-msvc.zip -OutFile deno.zip; Expand-Archive -Force deno.zip -DestinationPath .; Remove-Item deno.zip"
if not exist yt-dlp.exe powershell -NoProfile -Command "Invoke-WebRequest https://github.com/yt-dlp/yt-dlp/releases/latest/download/yt-dlp.exe -OutFile yt-dlp.exe"
if exist yt-dlp.exe yt-dlp.exe -U >nul 2>&1
muse-server.exe
pause
