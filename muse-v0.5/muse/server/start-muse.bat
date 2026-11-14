@echo off
chcp 65001 >nul
rem === Cau hinh Muse (dien vao sau dau =, bo trong neu khong dung) ===
rem Application ID tao o discord.com/developers/applications -> bat Discord Rich Presence
set DISCORD_CLIENT_ID=1216803587678535791
rem Dang nhap Discord (OAuth): Client Secret + dia chi cong khai cua server (vd https://muse.example.com)
set DISCORD_CLIENT_SECRET=
set PUBLIC_URL=
rem Proxy cho YouTube, vi du socks5://127.0.0.1:40000 hoac http://host:port (thuong KHONG can)
set YT_PROXY=
rem yt-dlp (tuy chon, giup vuot loi "Sign in to confirm you're not a bot"): dat yt-dlp.exe canh muse-server.exe hoac ghi duong dan day
set YTDLP_PATH=
rem File cookies.txt (dinh dang Netscape) xuat tu trinh duyet da dang nhap YouTube, vi du C:\muse\cookies.txt
set YT_COOKIES=
rem Cach de nhat: de ten trinh duyet da dang nhap YouTube (firefox, edge, chrome, brave) - yt-dlp tu doc cookies, khong can xuat file
set YT_BROWSER=firefox
if exist admin.local.bat call admin.local.bat
rem Chi lang nghe tren chinh may nay (an IP mang LAN). Dung khi da co Tailscale Serve/Funnel hoac Caddy phia truoc. Xoa dong nay neu dien thoai can ket noi truc tiep qua Wi-Fi
set BIND=127.0.0.1
rem Link ung ho (tuy chon)
set DONATE_URL=
rem yt-dlp can mot JavaScript runtime (Deno) de giai ma YouTube; thieu thi bao "Requested format is not available"
set "PATH=%CD%;%PATH%"
if not exist deno.exe powershell -NoProfile -Command "Invoke-WebRequest https://github.com/denoland/deno/releases/latest/download/deno-x86_64-pc-windows-msvc.zip -OutFile deno.zip; Expand-Archive -Force deno.zip -DestinationPath .; Remove-Item deno.zip"
rem Tu tai yt-dlp neu chua co, roi tu cap nhat moi lan bat server
if not exist yt-dlp.exe powershell -NoProfile -Command "Invoke-WebRequest https://github.com/yt-dlp/yt-dlp/releases/latest/download/yt-dlp.exe -OutFile yt-dlp.exe"
if exist yt-dlp.exe yt-dlp.exe -U >nul 2>&1
rem GIF: server nen bang gifsicle roi doi sang WebP dong bang gif2webp (goi libwebp). Dat gifsicle.exe, gif2webp.exe va ffmpeg.exe canh muse-server.exe (hoac trong PATH).
rem Thieu thi van chay binh thuong, chi giu nguyen GIF goc. Co the chi duong dan bang GIFSICLE_PATH / GIF2WEBP_PATH.
set GIFSICLE_PATH=
set GIF2WEBP_PATH=
set FFMPEG_PATH=
rem Tu tai neu chua co (chi tai mot lan, file nam canh muse-server.exe). ffmpeg nang khoang 90 MB nen lan dau co the mat vai phut.
where gifsicle >nul 2>&1 || (echo Dang tai gifsicle... & powershell -NoProfile -Command "$ProgressPreference='SilentlyContinue'; Invoke-WebRequest https://eternallybored.org/misc/gifsicle/releases/gifsicle-1.95-win64.zip -OutFile gifsicle.zip; Expand-Archive -Force gifsicle.zip -DestinationPath gifsicle_tmp; Get-ChildItem gifsicle_tmp -Recurse -Filter gifsicle.exe | Select-Object -First 1 | Copy-Item -Destination .; Remove-Item -Recurse -Force gifsicle.zip, gifsicle_tmp" >nul 2>&1)
where gif2webp >nul 2>&1 || (echo Dang tai gif2webp ^(libwebp^)... & powershell -NoProfile -Command "$ProgressPreference='SilentlyContinue'; Invoke-WebRequest https://storage.googleapis.com/downloads.webmproject.org/releases/webp/libwebp-1.6.0-windows-x64.zip -OutFile libwebp.zip; Expand-Archive -Force libwebp.zip -DestinationPath libwebp_tmp; Get-ChildItem libwebp_tmp -Recurse -Filter gif2webp.exe | Select-Object -First 1 | Copy-Item -Destination .; Remove-Item -Recurse -Force libwebp.zip, libwebp_tmp" >nul 2>&1)
where ffmpeg >nul 2>&1 || (echo Dang tai ffmpeg ^(khoang 90 MB^)... & powershell -NoProfile -Command "$ProgressPreference='SilentlyContinue'; Invoke-WebRequest https://www.gyan.dev/ffmpeg/builds/ffmpeg-release-essentials.zip -OutFile ffmpeg.zip; Expand-Archive -Force ffmpeg.zip -DestinationPath ffmpeg_tmp; Get-ChildItem ffmpeg_tmp -Recurse -Filter ffmpeg.exe | Select-Object -First 1 | Copy-Item -Destination .; Remove-Item -Recurse -Force ffmpeg.zip, ffmpeg_tmp" >nul 2>&1)
where gifsicle >nul 2>&1 || echo [Luu y] Chua co gifsicle - GIF se khong duoc nen
where gif2webp >nul 2>&1 || echo [Luu y] Chua co gif2webp - GIF se khong duoc doi sang WebP dong
where ffmpeg >nul 2>&1 || echo [Luu y] Chua co ffmpeg - GIF avatar/banner se khong duoc doi sang video
muse-server.exe
pause
