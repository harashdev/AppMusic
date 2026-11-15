FROM rust:1-bookworm AS build
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates gifsicle webp ffmpeg && rm -rf /var/lib/apt/lists/*
ADD --chmod=755 https://github.com/yt-dlp/yt-dlp/releases/latest/download/yt-dlp_linux /usr/local/bin/yt-dlp
# Plugin bgutil (PO token): file zip thuần Python, yt-dlp bản đơn lẻ nạp được từ /etc/yt-dlp/plugins.
# Provider chạy ở container "bgutil" trong docker-compose; địa chỉ truyền qua YT_POT_URL.
ADD https://github.com/Brainicism/bgutil-ytdlp-pot-provider/releases/latest/download/bgutil-ytdlp-pot-provider.zip /etc/yt-dlp/plugins/bgutil-ytdlp-pot-provider.zip
ENV YTDLP_PATH=/usr/local/bin/yt-dlp
# (Tuỳ chọn, thử nghiệm) WITH_WPC=1: thêm plugin wpc dùng Chromium để sinh PO token. wpc cần yt-dlp cài bằng pip
# (bản đơn lẻ không nạp được thư viện ngoài như nodriver) và một màn hình ảo (xvfb) cho Chromium.
ARG WITH_WPC=0
RUN if [ "$WITH_WPC" = "1" ]; then \
      apt-get update && apt-get install -y --no-install-recommends python3 python3-venv chromium xvfb xauth \
      && rm -rf /var/lib/apt/lists/* \
      && python3 -m venv /opt/ytdlp \
      && /opt/ytdlp/bin/pip install --no-cache-dir "yt-dlp[default]" yt-dlp-getpot-wpc bgutil-ytdlp-pot-provider \
      && printf '#!/bin/sh\nexec xvfb-run -a /opt/ytdlp/bin/yt-dlp "$@"\n' > /usr/local/bin/yt-dlp-wpc \
      && chmod 755 /usr/local/bin/yt-dlp-wpc; \
    fi
COPY --from=denoland/deno:bin /deno /usr/local/bin/deno
WORKDIR /app
COPY --from=build /app/target/release/muse-server /app/muse-server
COPY static ./static
VOLUME /app/data
EXPOSE 3000
CMD ["/app/muse-server"]

ADD --chmod=755 https://github.com/ViRb3/wgcf/releases/download/v2.2.29/wgcf_2.2.29_linux_amd64 /usr/local/bin/wgcf
ADD https://github.com/pufferffish/wireproxy/releases/latest/download/wireproxy_linux_amd64.tar.gz /tmp/wp.tar.gz
RUN tar -xzf /tmp/wp.tar.gz -C /usr/local/bin wireproxy && rm /tmp/wp.tar.gz
COPY --chmod=755 start.sh /app/start.sh
