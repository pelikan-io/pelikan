FROM rust:bookworm AS builder
WORKDIR /usr/src/pelikan
COPY . .
RUN cargo build --release --locked

FROM debian:bookworm-slim

RUN groupadd -r pelikan && useradd -r -g pelikan pelikan

COPY --from=builder /usr/src/pelikan/target/release/pelikan-pingserver /usr/local/bin/
COPY --from=builder /usr/src/pelikan/target/release/pelikan-pingproxy /usr/local/bin/
COPY --from=builder /usr/src/pelikan/target/release/pelikan-segcache /usr/local/bin/
COPY --from=builder /usr/src/pelikan/target/release/pelikan-rds /usr/local/bin/

# default config: override admin.host to make it reachable outside the container
COPY packages/docker/segcache.toml /etc/pelikan/segcache.toml

USER pelikan

# data port
EXPOSE 12321
# admin port
EXPOSE 9999

CMD ["pelikan-segcache", "/etc/pelikan/segcache.toml"]
