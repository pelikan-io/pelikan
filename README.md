# Pelikan

Pelikan is a framework for developing cache services. It is:

- **Fast**: predictably low latency, not just high throughput — threads
  communicate over lockless queues, workers never block on each other, and
  the storage engine scales almost linearly with cores where others plateau
  ([Segcache blog]).

- **Efficient**: Segcache, the flagship storage engine, spends 5 bytes of
  metadata per object (Memcached: 56) and cuts cache memory requirements by
  22–60% against state-of-the-art designs ([NSDI'21 paper]).

- **Reliable**: the design is distilled from years of operating cache fleets
  at planet scale — control and data planes are separated at runtime, and
  every module ships its own metrics, so the server stays observable and
  manageable under load.

- **Modular**: every service is a thin composition of a protocol, a storage
  engine, and a runtime core drawn from shared libraries; new caching
  solutions reuse the low-level machinery instead of reimplementing it.

[![License: Apache-2.0][license-badge]][license-url]
[![Build Status][cargo-build-badge]][cargo-build-url]
[![Fuzz Status][cargo-fuzz-badge]][cargo-fuzz-url]

[Website](http://pelikan.io) |
[Chat][discord-url]

# Content

- [Overview](#overview)
  - [Products](#products)
  - [Features](#features)
- [Build](#building-pelikan)
- [Usage](#usage)
- [Community](#community)
  - [Stay in touch](#stay-in-touch)
  - [Contributing](#contributing)
- [Documentation](#documentation)
- [License](#license)

# Overview

After years of using and working on various cache services, we built a common
framework that reveals the inherent architectural similarity among them.

By creating well-defined modules, most of the low-level functionalities are
reused as we create different binaries. The implementation learns from our
operational experiences to improve performance and reliability, and leads to
software designed for large-scale deployment.

The framework approach allows us to develop new features and protocols quickly.

![Pelikan workspace architecture](docs/diagrams/architecture.svg)

Each service composes a protocol, a storage engine, and a runtime core from
the shared libraries below it; see [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)
for the full breakdown.

## Why Pelikan

**Memory efficiency.** Cache capacity is usually bought in DRAM, so metadata
overhead and dead objects are real money. Segcache groups objects by TTL into
segments, amortizing metadata to about 5 bytes per object (Memcached: 56) and
removing expired objects within a second of expiry ([Segcache blog]). In the
[NSDI'21 paper]'s
evaluation on production workloads, this reduced memory requirements by
40–90% versus Pelikan's own production slab storage and by 22–60% versus
state-of-the-art research systems.

**Predictable tail latency.** The data plane is built so that a worker
thread never waits on a peer: threads communicate over lockless queues, and
what locking storage needs is amortized ~10,000× by managing segments rather
than individual objects ([Segcache blog]). Latency SLOs at the tail are a
design target, not an aspiration — in a
[joint evaluation with Intel ADQ](https://pelikan.io/blog/benchmark-adq/),
stacked Pelikan instances upheld a p999 < 5 ms SLO at 1M QPS per host, and
the same architecture scaled to ~8× Memcached's throughput at 24 threads
([Segcache blog]).

**Velocity through composition.** Cache servers share most of their anatomy;
Pelikan makes that anatomy reusable. A new service picks a wire protocol, a
storage engine, and a threading model from existing crates and wires them
together — `pelikan-pingserver` is the minimal worked example, and the same
composition carries `pelikan-segcache` and `pelikan-rds`. Adding a protocol
or storage backend extends every service that wants it, not one binary.

## Products

Pelikan contains the following products:

- `pelikan-segcache`: a Memcached-like server with Segcache as the backing
  storage, a TTL-centric design offering extremely high memory efficiency and
  excellent core scalability. See our [NSDI'21 paper] for design
  and evaluation details.
- `pelikan-rds`: a server speaking the RESP (Redis Serialization Protocol)
  wire format.
- `pelikan-pingserver`: a minimal ping/pong server useful as a tutorial and
  for measuring baseline RPC performance.
- `pelikan-pingproxy`: a proxy for the ping protocol, useful as a starting
  point for building cache proxies.

## Legacy

Pelikan was initially implemented in C. The legacy codebase can be found at the
[pelikan-c](https://github.com/pelikan-io/pelikan-c) repo.
It offers the same design blueprint as the current mainline, and
implements multiple storage backend, data structures, and protocols. However, it only
builds single-threaded, plain-text backends. It remains as a reference, but
is not actively worked on. We do not recommend it for production deployments.

## Features

- runtime separation of control and data plane
- predictably low latencies via lockless data structures, worker never blocks
- per-module config options and metrics that can be composed easily
- multiple storage and protocol implementations, easy to further extend
- low-overhead command logger for hotkey and other important data analysis

# Building Pelikan

## Requirement

- Rust [stable toolchain](https://www.rust-lang.org/learn/get-started)
- C toolchain and `cmake`: used to build [AWS-LC](https://github.com/aws/aws-lc),
  the cryptography library backing our TLS support via `rustls`

## Build

```sh
git clone https://github.com/pelikan-io/pelikan
cd pelikan
cargo build --release
```

## Tests

```sh
cargo test
```

Integration tests bind the default ports (`12321` data, `9999` admin); stop
any running pelikan server instance before running the test suite.

# Usage

Using `pelikan-segcache` as an example, other executables are highly similar.

To get info of the service, including usage format and options, run:

```sh
target/release/pelikan-segcache --help
```

To launch the service with default settings, simply run:

```sh
target/release/pelikan-segcache
```

To launch the service with the sample config file, run:

```sh
target/release/pelikan-segcache config/segcache.toml
```

By default, the server listens on port `12321` for data commands and port
`9999` for admin commands. The sample config additionally enables an HTTP
admin endpoint on port `9998`.

To stop the server, use `Ctrl-C` in its terminal, or send it `SIGTERM`;
the server shuts down gracefully.

You should be able to try out the server using an existing memcached client,
or simply with `telnet`.

```sh
$ telnet localhost 12321
Trying 127.0.0.1...
Connected to localhost.
Escape character is '^]'.
set foo 0 0 3
bar
STORED
get foo
VALUE foo 0 3
bar
END
```

**Attention**: use `admin` port for all non-data commands.

```sh
$ telnet localhost 9999
Trying 127.0.0.1...
Connected to localhost.
Escape character is '^]'.
version
VERSION 0.3.2
stats
STAT add 0
STAT add_ex 0
STAT add_not_stored 0
STAT append 0
...
END
```

## Configuration

Pelikan is file-first when it comes to configurations, and currently is
config-file only. You can create a new config file following the examples
included under the `config` directory.

# Community

## Stay in touch

- Join our [Discord server][discord-url] for questions and discussions
- Visit <http://pelikan.io>
- Follow us on Twitter: [@pelikan_cache]

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for prerequisites, build and test
commands, and the checks CI enforces.

If you want to submit a patch, please follow these steps:

1. create a new issue
2. fork on github & clone your fork
3. create a feature branch on your fork
4. push your feature branch
5. create a pull request linked to the issue

# Documentation

- [Architecture](docs/ARCHITECTURE.md): how the workspace is layered, from
  reusable components to server products
- Example configs for every product live under [`config/`](config/)
- Design notes and engineering records live under `docs/`; more material is on
  our [website](http://pelikan.io)

## License

This software is licensed under the Apache 2.0 license, see [LICENSE](LICENSE) for details.

[@pelikan_cache]: https://twitter.com/pelikan_cache
[cargo-build-badge]: https://img.shields.io/github/actions/workflow/status/pelikan-io/pelikan/cargo.yml?branch=main
[cargo-build-url]: https://github.com/pelikan-io/pelikan/actions/workflows/cargo.yml?query=branch%3Amain+event%3Apush
[cargo-fuzz-badge]: https://img.shields.io/github/actions/workflow/status/pelikan-io/pelikan/fuzz.yml?branch=main
[cargo-fuzz-url]: https://github.com/pelikan-io/pelikan/actions/workflows/fuzz.yml?query=branch%3Amain+event%3Apush
[license-badge]: https://img.shields.io/badge/license-Apache%202.0-blue.svg
[license-url]: https://github.com/pelikan-io/pelikan/blob/main/LICENSE
[nsdi'21 paper]: https://www.usenix.org/conference/nsdi21/presentation/yang-juncheng
[segcache blog]: https://pelikan.io/blog/segcache/
[discord-url]: https://discord.gg/yUBWHqxGUR
