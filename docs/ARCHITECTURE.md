# Pelikan Architecture

## Workspace Structure

The workspace is organized in layers:

**Core Infrastructure** (`src/`)
- `common/` - Shared types and traits across servers
- `config/` - TOML-based configuration parsing
- `logger/` - Centralized logging with tracing
- `net/` - Networking abstractions, event loops, TLS support
- `queues/` - Inter-thread communication via queues and wakers
- `session/` - Session management
- `entrystore/` - Entry storage type collection; wraps the external `cache-rs` `segcache`
  crate (segment-based storage engine, NSDI'21 paper) as its backing store

**Protocol Layer** (`src/protocol/`)
- `admin/` - Admin ASCII protocol for stats and management
- `memcache/` - Memcache ASCII protocol
- `ping/` - Simple ping/pong protocol
- `resp/` - Redis RESP protocol with sorted set support
- `http/` - HTTP protocol parser
- `common/` - Shared protocol traits

**Storage Support** (`src/storage/`)
- `bloom/` - Bloom filter implementations
- `types/` - Shared storage type definitions
- The segment-based storage engine itself (formerly `src/storage/segcache/` and
  `src/storage/datatier/` in this repo) now lives in the external `cache-rs` repository
  and is pulled in as the `segcache` dependency by `src/entrystore/`.

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

## Key Design Patterns

- **Lockless data structures**: Worker threads never block
- **Control/data plane separation**: Admin port (default 9999) for management, data port
  for cache operations
- **Per-module config and metrics**: Each component has independent configuration and
  observability
- **Protocol/storage pluggability**: Easy to add new protocols or storage backends
