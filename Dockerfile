# syntax=docker/dockerfile:1

ARG RUST_VERSION=1.96.0

FROM rust:${RUST_VERSION}-slim-bookworm AS builder
WORKDIR /src

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        ca-certificates \
        pkg-config \
    && rm -rf /var/lib/apt/lists/*

COPY Cargo.toml Cargo.lock ./
COPY crates ./crates

RUN cargo build --locked --release -p ace-engine --bin outpace

FROM debian:bookworm-slim AS runtime

LABEL org.opencontainers.image.source="https://github.com/jamezrin/outpace" \
      org.opencontainers.image.licenses="AGPL-3.0-or-later"

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
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

EXPOSE 6878/tcp 1935/tcp 8621/tcp 8621/udp
VOLUME ["/var/lib/outpace"]

USER outpace
ENTRYPOINT ["outpace"]
CMD ["serve"]
