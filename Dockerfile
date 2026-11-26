# syntax=docker/dockerfile:1

# ---- build stage -------------------------------------------------------------
FROM ubuntu:24.04 AS build
ARG RUST_TOOLCHAIN=stable
ENV DEBIAN_FRONTEND=noninteractive \
    RUSTUP_HOME=/opt/rustup \
    CARGO_HOME=/opt/cargo \
    PATH=/opt/cargo/bin:$PATH
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl gcc libc6-dev \
    && rm -rf /var/lib/apt/lists/* \
    && curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
        | sh -s -- -y --profile minimal --default-toolchain "$RUST_TOOLCHAIN"
WORKDIR /src
# Resolve and compile dependencies first so source edits do not rebuild them.
COPY Cargo.toml Cargo.lock ./
RUN mkdir -p src/bin benches \
    && echo 'fn main() {}' > src/main.rs \
    && echo 'fn main() {}' > src/bin/ferrum-load.rs \
    && echo 'fn main() {}' > benches/parser.rs \
    && echo 'fn main() {}' > benches/store.rs \
    && touch src/lib.rs \
    && cargo build --release --locked -j 2 --bins \
    && rm -rf src benches
COPY src ./src
COPY benches ./benches
RUN touch src/lib.rs src/main.rs src/bin/ferrum-load.rs \
    && cargo build --release --locked -j 2 --bins

# ---- runtime stage -----------------------------------------------------------
FROM ubuntu:24.04
RUN groupadd --system --gid 10001 ferrum \
    && useradd --system --uid 10001 --gid ferrum --home-dir /data --shell /usr/sbin/nologin ferrum \
    && mkdir /data && chown ferrum:ferrum /data
COPY --from=build /src/target/release/ferrum-db /src/target/release/ferrum-load /usr/local/bin/
USER ferrum
WORKDIR /data
VOLUME /data
EXPOSE 6380
ENV FERRUM_BIND=0.0.0.0 \
    FERRUM_PORT=6380 \
    FERRUM_DIR=/data \
    RUST_LOG=info
HEALTHCHECK --interval=5s --timeout=3s --start-period=3s --retries=5 \
    CMD ["bash", "-c", "exec 3<>/dev/tcp/127.0.0.1/6380 && printf 'PING\\r\\n' >&3 && read -r reply <&3 && [[ $reply == +PONG* ]]"]
ENTRYPOINT ["ferrum-db"]
CMD ["--aof", "--aof-fsync", "everysec"]
