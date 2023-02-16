# -----------------
# Cargo Build Stage
# -----------------

FROM debian:latest as cargo-build

ENV RUSTUP_HOME=/usr/local/rustup \
    CARGO_HOME=/usr/local/cargo \
    PATH=/usr/local/cargo/bin:$PATH \
    RUST_VERSION=1.67.1

RUN apt-get update; \ 
    apt-get install -y --no-install-recommends ca-certificates curl netbase

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs > rustup-init
RUN chmod +x rustup-init
RUN sh rustup-init -y --no-modify-path --profile minimal --default-toolchain  stable
RUN chmod -R a+w $RUSTUP_HOME $CARGO_HOME; 
RUN rustup --version; \
    cargo --version; \
    rustc --version;

# vendored cargo dependencies
COPY ./src src
COPY Cargo.lock .
COPY Cargo.toml .
# RUN mkdir .cargo
# RUN cargo vendor > .cargo/config


RUN apt-get update; apt-get install -y cmake clang make protobuf-compiler

RUN cargo build --release
CMD ["./target/release/pelikan_segcache_rs"]

