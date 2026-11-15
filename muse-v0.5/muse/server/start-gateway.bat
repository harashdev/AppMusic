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
rem (Tuy chon) Neu YouTube bao "Sign in to confirm you're not a bot": bo "rem" o dong duoi, de ten trinh duyet da dang nhap YouTube va dat yt-dlp.exe canh file nay
rem set YT_BROWSER=firefox
set YTDLP_PATH=
set YT_COOKIES=
if exist gateway.local.bat call gateway.local.bat
set "PATH=%CD%;%PATH%"
muse-server.exe
pause
