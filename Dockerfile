# syntax=docker/dockerfile:1.7

ARG RUST_VERSION=1.97.1
ARG ALPINE_VERSION=3.22

FROM rust:${RUST_VERSION}-alpine${ALPINE_VERSION} AS builder
WORKDIR /build

COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY web ./web

RUN --mount=type=cache,id=northgate-cargo-registry,target=/usr/local/cargo/registry \
    --mount=type=cache,id=northgate-cargo-git,target=/usr/local/cargo/git \
    --mount=type=cache,id=northgate-target,target=/build/target \
    cargo build --release --locked && \
    cp /build/target/release/northgate /tmp/northgate

FROM alpine:${ALPINE_VERSION} AS runtime

RUN apk add --no-cache ca-certificates && \
    addgroup -S -g 10001 northgate && \
    adduser -S -D -H -u 10001 -G northgate -s /sbin/nologin northgate && \
    mkdir -p /data && \
    chown 10001:10001 /data && \
    chmod 0700 /data

COPY --from=builder --chown=northgate:northgate /tmp/northgate /usr/local/bin/northgate

ENV NORTHGATE_BIND=0.0.0.0:8787 \
    NORTHGATE_DATABASE_PATH=/data/northgate.db \
    RUST_LOG=northgate=info,tower_http=warn

USER 10001:10001
VOLUME ["/data"]
EXPOSE 8787

HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD wget -q -T 2 -O /dev/null http://127.0.0.1:8787/healthz || exit 1

ENTRYPOINT ["/usr/local/bin/northgate"]
