---
name: run
description: Launch a pelikan product binary with its example config
---

Launch one of pelikan's product binaries.

## Products

| Binary | Purpose | Example config |
|---|---|---|
| `pelikan-segcache` | Memcached-compatible server, Segcache storage | `config/segcache.toml` (or `config/twemcache-tls.toml` for TLS) |
| `pelikan-pingserver` | Ping protocol server for testing/benchmarking | `config/pingserver.toml` (or `config/pingserver-tls.toml` for TLS) |
| `pelikan-rds` | RESP (Redis protocol) server | `config/rds.toml` |
| `pelikan-pingproxy` | Ping protocol proxy | `config/pingproxy.toml` |

## Steps

1. Build if needed: `cargo build --workspace --release`
2. Run with a config file: `target/release/<binary> config/<name>.toml`
   - Without a config file argument, the binary starts with built-in defaults.
3. `target/release/<binary> --help` lists all flags and overrides.

## Notes

- Admin port defaults to 9999 (stats/management), separate from the data port.
- See the `test` skill for running the integration test harnesses instead of a live binary.
