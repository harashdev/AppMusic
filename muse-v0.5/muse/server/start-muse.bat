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
rem Link ung ho (tuy chon)
set DONATE_URL=
muse-server.exe
pause
