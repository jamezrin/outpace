# syntax=docker/dockerfile:1

FROM debian:bookworm-slim AS builder
WORKDIR /src

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        ca-certificates \
        curl \
        pkg-config \
    && rm -rf /var/lib/apt/lists/*

ENV RUSTUP_HOME=/usr/local/rustup \
    CARGO_HOME=/usr/local/cargo \
    PATH=/usr/local/cargo/bin:$PATH

RUN curl --proto '=https' --tlsv1.2 -fsSL https://sh.rustup.rs \
    | sh -s -- -y --default-toolchain none --profile minimal --no-modify-path

COPY rust-toolchain.toml ./
RUN rustup show

COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
# tools/ is copied so Cargo can resolve every workspace member's manifest; the
# `-p ace-engine` build below still only compiles the engine, not the tools.
COPY tools ./tools

RUN cargo build --locked --release -p ace-engine --bin outpace

FROM debian:bookworm-slim AS runtime

LABEL org.opencontainers.image.source="https://github.com/jamezrin/outpace" \
      org.opencontainers.image.licenses="AGPL-3.0-or-later"

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && groupadd --system outpace \
    && useradd --system --gid outpace --home-dir /var/lib/outpace --shell /usr/sbin/nologin outpace \
    && mkdir -p /var/lib/outpace \
    && chown outpace:outpace /var/lib/outpace \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /src/target/release/outpace /usr/local/bin/outpace
COPY LICENSE /usr/share/doc/outpace/LICENSE

ENV OUTPACE_BIND=0.0.0.0:6878 \
    OUTPACE_RTMP_BIND=0.0.0.0:1935 \
    OUTPACE_DATA_DIR=/var/lib/outpace

EXPOSE 6878/tcp 1935/tcp 8621/tcp
VOLUME ["/var/lib/outpace"]

USER outpace
ENTRYPOINT ["outpace"]
CMD ["serve"]
