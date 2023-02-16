# -----------------
# Cargo Build Stage
# -----------------

FROM rust:latest as cargo-build

COPY . .
RUN apt-get update && apt-get install -y \
  cmake \
  clang \
  protobuf-compiler \
  && rm -rf /var/lib/apt/lists/*
RUN cargo vendor > .cargo/config

RUN cargo build --release  -p momento_proxy

# -----------------
# Run Momento Proxy
# -----------------

FROM debian:stable-slim

WORKDIR /app

ENV MOMENTO_AUTHENTICATION=""
ENV CONFIG="momento_proxy.toml"

RUN mkdir config

COPY --from=cargo-build ./target/release/momento_proxy .
COPY --from=cargo-build ./config/momento_proxy.toml ./config

RUN chmod +x ./momento_proxy
CMD ./momento_proxy ./config/${CONFIG}