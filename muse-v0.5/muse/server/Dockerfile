FROM rust:1-bookworm AS build
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates gifsicle webp ffmpeg && rm -rf /var/lib/apt/lists/*
ADD --chmod=755 https://github.com/yt-dlp/yt-dlp/releases/latest/download/yt-dlp_linux /usr/local/bin/yt-dlp
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
