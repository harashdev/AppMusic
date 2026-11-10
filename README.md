<div align="center">
  <img src="muse-v0.5/muse/server/static/icon-512.png" alt="BBeat logo" width="128" height="128">

# BBeat

**A free, self-hostable music app for listening together with friends.**
Search and play YouTube Music, share a listening room, and keep it all on your own server.

[![Stars](https://img.shields.io/github/stars/harashdev/appmusic?style=for-the-badge&logo=github)](https://github.com/harashdev/appmusic/stargazers)
[![Last commit](https://img.shields.io/github/last-commit/harashdev/appmusic?style=for-the-badge)](https://github.com/harashdev/appmusic/commits/main)
[![Issues](https://img.shields.io/github/issues/harashdev/appmusic?style=for-the-badge)](https://github.com/harashdev/appmusic/issues)
[![Rust](https://img.shields.io/badge/Rust-server-orange?style=for-the-badge&logo=rust)](https://www.rust-lang.org)
[![Platforms](https://img.shields.io/badge/Web%20%7C%20Windows%20%7C%20Android-1db954?style=for-the-badge)](#download)
[![Ko-fi](https://img.shields.io/badge/Support%20me-Ko--fi-ff5e5b?style=for-the-badge&logo=ko-fi&logoColor=white)](https://ko-fi.com/harashdev)

</div>

---

## About

BBeat (code name *Muse*) is a music app built around three ideas:

- **Listen together.** Create a room, share the link, and everyone hears the same song in sync, with shared controls and chat.
- **Your server, your data.** One small Rust server stores accounts, playlists, favorites and history in a local SQLite file. No tracking, no ads.
- **Works everywhere.** The same interface runs in any browser, as a Windows desktop app (Tauri), and as an Android app.

Music comes from YouTube / YouTube Music, played through the official YouTube player (or a server-relayed audio stream when you turn on audio effects).

<!--
## Screenshots

Put your screenshots in a `docs/` folder, then remove this comment and use a table like:

| Home | Now playing | Listening room |
| :--: | :--: | :--: |
| <img src="docs/home.png" width="260"> | <img src="docs/now-playing.png" width="260"> | <img src="docs/room.png" width="260"> |
-->

## Features

**Music**
- Search YouTube Music, or paste a YouTube link
- Home with quick picks, listen-again, mixes, trending charts and 17 moods
- Favorites, playlists, listening history, queue and shuffle / repeat
- Synced (karaoke-style) lyrics from SimpMusic Lyrics, with LRCLIB as a fallback
- [SponsorBlock](https://sponsor.ajay.app) support: skips sponsors, intros and outros automatically
- Like / dislike counts via Return YouTube Dislike
- Smooth crossfade between songs (beta)
- Offline playlist view and a media-session player (lock screen, headset buttons)

**Sound**
- 10-band equalizer with presets, preamp, delay (echo) and reverb (room, hall, plate)

**Social**
- Listening rooms with synced playback, shared control and room chat
- Friends list with invites by friend code or username
- Discord-style profiles: animated GIF banner and avatar with a built-in image cropper, status line, custom backgrounds and accent color
- Discord account linking and Rich Presence on the desktop app

**Import & backup**
- Import YouTube playlists by pasting their links. No Google sign-in needed (public / unlisted playlists)
- Backup and restore your favorites, history and playlists as JSON

**App**
- English and Tiếng Việt, switchable in Settings
- Desktop layout and a phone layout with a floating mini player
- Self-update check through GitHub Releases

## Download

> Pre-built files are published in [**Releases**](https://github.com/harashdev/appmusic/releases) when available. You can also build them yourself with GitHub Actions (see [Build](#build)).

| Platform | How to get it |
| --- | --- |
| **Server** (Windows) | Run the **Build server** workflow, or download the packaged `muse-server` from Releases |
| **Desktop** (Windows) | Run the **Build desktop** workflow, or download the installer from Releases |
| **Android** | Run the **Build Android** workflow, or download the APK from Releases |
| **Web** | Open your server's address in any modern browser (installable as a PWA) |

The desktop and Android apps are thin clients: they connect to a BBeat server, so you need one running somewhere.

## Run the server

Requirements: [Rust](https://rustup.rs) (stable). Optional but recommended: [yt-dlp](https://github.com/yt-dlp/yt-dlp) and [Deno](https://deno.com), which help when YouTube blocks audio streams.

```bash
git clone https://github.com/harashdev/appmusic.git
cd appmusic/muse-v0.5/muse/server
cargo run --release        # http://localhost:3000
```

Data lives in `data/` (SQLite in WAL mode plus uploaded avatars and banners). When you back up, copy the **whole** `data/` folder.

To expose it to friends, put it behind HTTPS (Caddy or Nginx). HTTPS is needed for secure WebSockets and for the YouTube player. A `Dockerfile`, `docker-compose.yml` and `Caddyfile` are included in `server/`, and on Windows you can use `start-muse.bat`.

### Configuration

All settings are optional environment variables:

| Variable | Purpose |
| --- | --- |
| `DONATE_URL` | Link shown on the Donate button (defaults to the author's Ko-fi) |
| `PUBLIC_URL` | Public address of your server, e.g. `https://muse.example.com` (needed for Discord sign-in) |
| `DISCORD_CLIENT_ID` / `DISCORD_CLIENT_SECRET` | Discord application, enables account linking and Rich Presence |
| `YT_PROXY` | HTTP or SOCKS5 proxy for YouTube requests |
| `YTDLP_PATH` / `YT_COOKIES` / `YT_BROWSER` | yt-dlp location and cookies, to get past "Sign in to confirm you're not a bot" |
| `ADMIN_USER` / `ADMIN_PASS` | Create or promote an admin account |

More details (in Vietnamese) are in [`muse-v0.5/muse/README.md`](muse-v0.5/muse/README.md).

## Build

```
muse-v0.5/muse/
├─ server/    Axum + SQLite server and the web interface (static/index.html)
├─ desktop/   Tauri v2 desktop app
├─ android/   Android app (WebView + background playback service)
└─ native/    Native Discord Rich Presence helper
```

**GitHub Actions (easiest).** Fork the repo, then go to *Settings → Secrets and variables → Actions → Variables* and add `MUSE_URL` with your server's address. Run the workflows from the *Actions* tab and download the files from the run's **Artifacts**.

**Desktop, locally**
```bash
cargo install tauri-cli --version "^2"
cd muse-v0.5/muse/desktop/src-tauri
MUSE_URL=https://muse.example.com cargo tauri build
```

**Android, locally.** Open `muse-v0.5/muse/android` in Android Studio, or run `gradle assembleDebug` (JDK 17, Android SDK 34).

## Contributing

Bug reports, ideas and pull requests are welcome. Please open an [issue](https://github.com/harashdev/appmusic/issues) first for larger changes.

**Translations:** the interface is written in Vietnamese and translated at runtime. To fix a translation or add a language, edit the `EN_EXACT` / `EN_RULES` tables at the top of `muse-v0.5/muse/server/static/index.html`.

## Support

BBeat is a personal, non-profit project. If you like it, you can buy the author a coffee. It is completely optional.

<a href="https://ko-fi.com/harashdev"><img src="https://ko-fi.com/img/githubbutton_sm.svg" alt="Support me on Ko-fi"></a>

## Disclaimer

BBeat is an independent project and is **not affiliated with, endorsed by, or sponsored by** YouTube, Google, Discord or any other service it connects to. All trademarks belong to their respective owners.

BBeat does not host or store any music. Songs are streamed from YouTube, and you are responsible for following YouTube's Terms of Service and the laws of your country. This project is provided for personal and educational use, **as is, without warranty of any kind**.

## Credits

BBeat is built on the work of many open-source projects and services:

- [YouTube IFrame Player API](https://developers.google.com/youtube/iframe_api_reference): official YouTube player
- [SponsorBlock](https://sponsor.ajay.app): skip-segment data, © Ajay Ramachandran and community, CC BY-NC-SA 4.0
- [Return YouTube Dislike](https://returnyoutubedislike.com): like / dislike counts
- [SimpMusic](https://github.com/maxrave-dev/SimpMusic): lyrics source and inspiration for the quick-picks layout
- [LRCLIB](https://lrclib.net): time-synced lyrics
- [yt-dlp](https://github.com/yt-dlp/yt-dlp) and [Piped](https://github.com/TeamPiped/Piped): fallback audio streams
- [Axum](https://github.com/tokio-rs/axum), [Tokio](https://tokio.rs), [tower-http](https://github.com/tower-rs/tower-http), [reqwest](https://github.com/seanmonstar/reqwest), [rusqlite](https://github.com/rusqlite/rusqlite), [argon2](https://github.com/RustCrypto/password-hashes), [serde](https://serde.rs): the Rust server
- [Tauri](https://tauri.app): desktop app
- [AndroidX](https://developer.android.com/jetpack/androidx): Android app

Made by **BaoBu**.
