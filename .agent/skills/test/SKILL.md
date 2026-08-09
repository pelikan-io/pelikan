---
name: test
description: Run pelikan's unit, integration, and fuzz tests
---

Run tests across the pelikan workspace.

## Unit tests

Alongside source files in each crate:

```bash
cargo test --workspace          # everything
cargo test -p pelikan-segcache  # one product crate, e.g. segcache
```

## Integration tests

Custom harnesses, not plain `#[test]` functions — currently exist for `segcache` and `rds`:
- `src/server/segcache/tests/integration.rs` — single-threaded instance
- `src/server/segcache/tests/integration_multi.rs` — multi-threaded instance
- `src/server/segcache/tests/common.rs` — shared test utilities
- `src/server/rds/tests/integration.rs`, `integration_multi.rs`, `common.rs` — same pattern for RDS

They build and run as part of `cargo test -p pelikan-segcache` / `-p pelikan-rds`.

## Fuzz tests (requires nightly)

Fuzz targets exist for the protocol parsers:
- `src/protocol/admin/fuzz/`
- `src/protocol/memcache/fuzz/`
- `src/protocol/ping/fuzz/`
- `src/protocol/resp/fuzz/`

```bash
cd src/protocol/<name>/fuzz
cargo +nightly fuzz list          # see available targets
cargo +nightly fuzz run <target>
```

## Benchmarks

Benchmarks exist for six crates:
- `protocol-admin`
- `protocol-memcache`
- `protocol-ping`
- `bloom`
- `pelikan-segcache`
- `pelikan-rds`

```bash
cargo bench -p <crate>  # e.g. cargo bench -p pelikan-segcache
```

## Notes

- The segment-based storage engine itself now lives in the external `cache-rs` repository (consumed as the `segcache` dependency by `src/entrystore/`) — it has no fuzz targets or tests in this repo.
