# Design: `version` command on the memcache data listener

**Date:** 2026-07-15
**Status:** Approved (pending spec review)

## Problem

Real memcached supports a `version` command on its data port (both the ASCII
text protocol and the binary protocol, opcode `0x0b`). Pelikan's memcache
protocol implementation does not: the `Request`/`Command` enums in
`src/protocol/memcache/src/request/mod.rs` have no `Version` variant, so the
data listener rejects `version` as an unknown command.

`VERSION` currently exists only on the **admin** listener (default port 9999),
handled in `src/core/admin/src/lib.rs`. This is a memcached-compatibility gap on
the data plane.

## Goal

Support `version` on the memcache data listener, in both the text and binary
protocols, replying with the server version in memcached's format.

## Version string source

All workspace crates share `version = "0.3.2"` (`Cargo.toml`
`[workspace.package]`), so `env!("CARGO_PKG_VERSION")` evaluated in the
entrystore crate equals what the binary and the admin port report. The handler
uses that constant directly — no plumbing of a version string down into `Seg` is
required. Reply format matches memcached: a bare version number, e.g.
`VERSION 0.3.2\r\n` (text) / body `0.3.2` (binary).

## Design

`version` is an argument-less, storage-independent command. It mirrors how
`quit` is already handled: parsed into a unit-struct request variant, dispatched
through `Execute`, and answered in the entrystore handler (like
`Request::Quit(_) => Response::hangup()`).

### Shared request/response types (`src/protocol/memcache/src/`)

- **`request/version.rs`** (new): `pub struct Version {}` + no-op `Klog` impl —
  a copy of `request/quit.rs`.
- **`request/mod.rs`**:
  - `mod version; pub use version::Version;`
  - `Request::Version(Version)` variant.
  - `Command::Version` variant.
  - `Display` arm → `"version"`.
  - `Klog` arm → delegate to `Version::klog` (no-op).
- **`response/version.rs`** (new): `pub struct Version { pub(crate) inner: String }`
  with a `Compose` impl writing `VERSION <inner>\r\n`, and a `parse` fn for the
  client side.
- **`response/mod.rs`**:
  - `mod version; pub use version::Version as VersionResponse;` (aliased to avoid
    colliding with the request `Version` re-export in `crate::*`).
  - `Response::Version(VersionResponse)` variant.
  - `Response::version<T: ToString>(s: T)` constructor.
  - `Display` arm → `"VERSION"`.
  - `Compose` arm → delegate. Not a hangup (`should_hangup` unchanged).
  - `ResponseType::Version` + `response_type` match on `b"VERSION"` +
    `response` dispatch arm (client-side round-trip completeness).

### Metrics (`src/protocol/memcache/src/metrics.rs`)

- Add `#[metric(name = "version")] pub static VERSION: Counter = Counter::new();`
  (before the `test_no_duplicates!()` macro).

### Text protocol (`src/protocol/memcache/src/text/`)

- **`text/request/version.rs`** (new): `parse_version_request` (consume
  `space0` then `crlf`, increment `VERSION` metric under
  `#[cfg(feature = "metrics")]`) + `_compose_version_request` writing
  `version\r\n`. Mirrors `text/request/quit.rs`. Unit test:
  `version\r\n` → `Request::Version(Version {})`.
- **`text/request/mod.rs`**: `mod version;`
- **`text/response/version.rs`** (new): `compose_version_response` +
  `parse_version_response`. Register in `text/response/mod.rs`.
- **`text/mod.rs`**:
  - `parse_command`: `b"version" | b"VERSION" => Command::Version`.
  - `_parse_request`: `Command::Version` → `parse_version_request` →
    `Request::Version`.
  - `_compose_request`: `Request::Version(_)` → `_compose_version_request`.
  - `_parse_response`: `Request::Version(_)` → `parse_version_response`.
  - `_compose_response`: `Request::Version(r)` → `compose_version_response`.

### Binary protocol (`src/protocol/memcache/src/binary/`)

- **`Opcode`** (`binary/mod.rs`): add `Version` ↔ `0x0b` in `from_u8`/`to_u8`.
- **`binary/request/header.rs`**: `RequestHeader::version()` constructor
  (opcode `Version`, all lengths 0).
- **`binary/request/version.rs`** (new): `parse_version_request` — validate
  `key_len == 0`, `extras_len == 0`, `total_body_len == 0`; return
  `Version {}`. `compose_version_request` writes a bare 24-byte header.
- **`binary/response/version.rs`** (new): `compose_version_response` — write a
  `ResponseHeader` (opcode `Version`, status `NoError`, no key/extras,
  `total_body_len = inner.len()`) followed by the version bytes;
  `parse_version_response` reads the body back into a `Response::Version`.
- **`binary/mod.rs`**:
  - `_parse_request`: `Opcode::Version` → `parse_version_request`.
  - `_compose_request`: `Request::Version(_)` arm.
  - `_parse_response`: `Request::Version` + `header.opcode == Version` arm.
  - `_compose_response`: `Request::Version(_)` arm.
- Register the two new modules in `binary/request/mod.rs` and
  `binary/response/mod.rs`.

### Handler (`src/entrystore/src/segcache/memcache.rs`)

- `Request::Version(_) => Response::version(env!("CARGO_PKG_VERSION"))` in the
  `Execute` match.

## Scope

- **In:** text + binary `version` on the memcache data listener; version metric.
- **Out:** RESP/`rds` server (separate protocol, no memcached `version`);
  changing the admin `VERSION` behavior.

## Testing

- **Unit:** text parser test (`version\r\n` → `Request::Version`); binary
  request parse/compose round-trip; response compose byte assertions (text
  `VERSION 0.3.2\r\n`; binary header + body).
- **Integration:** in the segcache server test harness (template:
  `src/server/segcache/tests/common.rs:346`, which already asserts the admin
  `VERSION {CARGO_PKG_VERSION}` reply), add a data-port case asserting
  `version\r\n` → `VERSION {CARGO_PKG_VERSION}\r\n`.

## Approach

TDD — write the failing parser/compose unit tests first, then implement each
layer (shared types → metrics → text → binary → handler → integration).
