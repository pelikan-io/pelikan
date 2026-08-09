# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in
this repository.

## Project Overview

Pelikan is a Rust framework for developing high-performance cache services. It
emphasizes modularity through reusable components, enabling rapid development of
different caching solutions. The project includes multiple server products that share
common infrastructure.

## Products

- `pelikan-segcache` - Memcached-compatible server with Segcache storage (TTL-centric,
  high memory efficiency)
- `pelikan-pingserver` - Ping server for testing and benchmarking
- `pelikan-rds` - RESP (Redis protocol) server
- `pelikan-pingproxy` - Ping protocol proxy

See the `run` skill for how to launch them and the `test` skill for how to test them.

## Build Commands

```bash
cargo build --workspace --release   # release build
cargo build --workspace             # debug build
cargo test --workspace              # all tests
cargo fmt --all                     # format
cargo clippy --all-targets --all-features   # lint
```

## Architecture

Multi-crate Cargo workspace, layered: core infrastructure, protocol layer, storage
support, server core, server products, proxies. See `docs/ARCHITECTURE.md` for the full
breakdown.

## Skills

Essential (shipped in `.agent/skills/`, symlinked into `.claude/skills/`, always
available regardless of MCP setup):
- `run` — launch a product binary with its example config
- `test` — run unit, integration, and fuzz tests
- `journal` — scaffold a `docs/journal/` entry for a non-trivial engineering effort
  (nudged, non-blocking, by `.agent/hooks/pre-commit-check.sh`)
- `pr` — create a feature branch, commit, push, open a PR
- `release` — create a release PR with version bump and changelog update

Recommended (used only if your environment has `skills-mcp` connected; never assume
they're present):
- `catchup`, `sweep-comments`, `plan-feature` — general repo/dev habit skills
- knowledge-iop vault bundle (`engineering-journal`, `frame-problem`, `propose-design`,
  `record-decision`, `vault-search`) — additionally requires the user's personal vault

## Build Requirements

- Rust stable toolchain

## Platform Support

- Linux and macOS
- x86_64 and ARM64 architectures
