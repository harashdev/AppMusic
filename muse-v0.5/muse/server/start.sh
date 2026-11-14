#!/bin/sh
cd /tmp
[ -f wgcf-account.toml ] || wgcf register --accept-tos
wgcf generate
printf '\n[Socks5]\nBindAddress = 127.0.0.1:40000\n' >> wgcf-profile.conf
wireproxy -c wgcf-profile.conf &
cd /app
exec /app/muse-server
