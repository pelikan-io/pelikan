# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Pelikan is a Rust framework for developing high-performance cache services. It emphasizes modularity through reusable components, enabling rapid development of different caching solutions. The project includes multiple server products that share common infrastructure.

## Build Commands

```bash
# Build entire workspace (release)
cargo build --workspace --release

# Build debug mode
cargo build --workspace

# Run all tests
cargo test --workspace

# Run tests for a specific package
cargo test -p segcache

# Format code
cargo fmt --all

# Lint with clippy
cargo clippy --all-targets --all-features

# Run benchmarks for a specific package
cargo bench -p segcache

# Run fuzz tests (requires nightly)
cargo +nightly fuzz run <target>
```

## Products

- `pelikan-segcache` - Memcached-compatible server with Segcache storage (TTL-centric, high memory efficiency)
- `pelikan-pingserver` - Ping server for testing and benchmarking
- `pelikan-rds` - RESP (Redis protocol) server
- `pelikan-pingproxy` - Ping protocol proxy

### Running Products

```bash
# Run with default settings
target/release/pelikan-segcache

# Run with config file
target/release/pelikan-segcache config/segcache.toml

# Get help and options
target/release/pelikan-segcache --help
```

## Architecture

### Workspace Structure

The workspace is organized in layers:

**Core Infrastructure** (`src/`)
- `common/` - Shared types and traits across servers
- `config/` - TOML-based configuration parsing
- `logger/` - Centralized logging with tracing
- `net/` - Networking abstractions, event loops, TLS support
- `session/` - Session management
- `entrystore/` - Entry storage type collection

**Protocol Layer** (`src/protocol/`)
- `admin/` - Admin ASCII protocol for stats and management
- `memcache/` - Memcache ASCII protocol
- `ping/` - Simple ping/pong protocol
- `resp/` - Redis RESP protocol with sorted set support
- `http/` - HTTP protocol parser
- `common/` - Shared protocol traits

**Storage Backends** (`src/storage/`)
- `segcache/` - Segment-based storage engine optimized for TTL workloads (NSDI'21 paper)
- `datatier/` - Byte storage pool abstractions (memmap-based)
- `bloom/` - Bloom filter implementations
- `types/` - Shared storage type definitions

**Server Core** (`src/core/`)
- `admin/` - Admin thread infrastructure
- `server/` - Event loops, thread management, signal handling
- `proxy/` - Proxy thread infrastructure

**Server Products** (`src/server/`)
- `segcache/` - Memcache-compatible server
- `pingserver/` - Multi-protocol ping server
- `rds/` - RESP protocol server

**Proxies** (`src/proxy/`)
- `ping/` - Ping protocol proxy

### Key Design Patterns

- **Lockless data structures**: Worker threads never block
- **Control/data plane separation**: Admin port (default 9999) for management, data port for cache operations
- **Per-module config and metrics**: Each component has independent configuration and observability
- **Protocol/storage pluggability**: Easy to add new protocols or storage backends

## Configuration

Configuration is TOML-based with example files in `config/`:
- `segcache.toml` - Segcache server config
- `pingserver.toml` - Pingserver config
- `pingserver-tls.toml` - TLS-enabled pingserver
- `rds.toml` - RDS server config
- `pingproxy.toml` - Ping proxy config

## Testing

### Test Organization

- Unit tests alongside source files
- Integration tests in `tests/` directories using custom harnesses
- `integration.rs` - Single-threaded instance testing
- `integration_multi.rs` - Multi-threaded instance testing
- Common test utilities in `tests/common.rs`

### Fuzz Testing

Fuzz targets exist for protocols and storage:
- `src/protocol/admin/fuzz/`
- `src/protocol/memcache/fuzz/`
- `src/protocol/ping/fuzz/`
- `src/protocol/resp/fuzz/`
- `src/storage/segcache/fuzz/`

## Dependencies

Key dependencies:
- **Networking**: mio (event loop), rustls (TLS)
- **Metrics**: metriken
- **Logging**: tracing
- **Parsing**: nom (parser combinators)
- **Serialization**: serde, toml

## Build Requirements

- Rust stable toolchain

## Platform Support

- Linux and macOS
- x86_64 and ARM64 architectures
