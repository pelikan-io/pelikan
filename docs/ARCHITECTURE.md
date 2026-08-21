# Pelikan Architecture

Pelikan is a Cargo workspace that builds several cache services out of shared
libraries. This document explains the architecture in three views, each with a
generated chart:

1. [**What the code is**](#the-big-picture) — the layers, and how each shipped
   binary is composed from them.
2. [**What runs**](#how-a-service-runs) — the threads each binary spawns and
   the queues that connect them.
3. [**What happens to a request**](#life-of-a-request) — one request traced
   through those threads, in the code's own verbs.

The charts are generated from the build manifest and source assertions by
`cargo xtask diagrams` and are wider than this page — click any chart to open
it at full size (scroll/zoom in the browser). See
[Keeping this document honest](#keeping-this-document-honest) for how they
stay in sync with the code.

## The Big Picture

[![Pelikan workspace architecture](diagrams/architecture.svg)](diagrams/architecture.svg?raw=1)

Read it bottom-up: **Runtime libraries** (the server/proxy cores and the
foundation utilities) support **Cache libraries** (wire protocols and storage),
and every **Service** at the top is a thin box composed of one bar from each
column below it — its protocol, its storage engine, its runtime core. External
crates (underlined, linked) are placed by their role in the stack, not their
repository of origin: the Segcache engine sits with storage, `rustls` and
`metriken` sit in the foundation.

That composition is the whole trick. Every product directly depends on the
foundation crates (`common`, `config`, `logger`); what distinguishes them is
three choices:

| Product | Protocol spoken | Core | Storage |
| --- | --- | --- | --- |
| `pelikan-segcache` | `protocol-memcache` | `server` | `entrystore::Seg` (Segcache engine) |
| `pelikan-rds` | `protocol-resp` | `server` | `entrystore::Seg` (Segcache engine) |
| `pelikan-pingserver` | `protocol-ping` | `server` | `entrystore::Noop` (no storage) |
| `pelikan-pingproxy` | `protocol-ping` (client + server) | `proxy` | none (`entrystore` linked only for trait bounds) |

Note that `pelikan-segcache` and `pelikan-rds` differ purely in wire
protocol — both run the Segcache engine. The no-op engine (dashed in the
chart) exists because ping needs no storage, and the proxy stores nothing at
all: requests pass through to upstream servers.

A new cache service is a new row in this table: pick a protocol, an engine,
and a core, and write the thin crate that wires them together.
`pelikan-pingserver` is the minimal worked example.

## How a Service Runs

[![Pelikan threading architecture](diagrams/threading.svg)](diagrams/threading.svg?raw=1)

The chart starts with the cache-server I/O scheduling fork, then shows the
backend-neutral shared-storage topology and the proxy runtime. Where the chart
names a runtime thread, it uses the literal registered name — what you see in
`top -H` is what the chart says. The storage-only panel instead names neutral
execution contexts so it does not imply Mio or Ringline owns storage
differently:

- **Cache-server I/O**: Mio accepts on `pelikan_listener` and drives
  `pelikan_work_i` callback state machines after a session-queue handoff.
  `ringline-acceptor` sends each accepted file descriptor through a bounded
  per-worker queue and wakes the selected `ringline-worker-*`; that worker's
  event loop then schedules the async connection task internally. This fork
  changes I/O scheduling, not storage ownership.
- **Server storage**: whichever backend is active, each neutral request
  execution context parses and responds while executing directly against one
  process-owned, internally synchronized engine shared through an `Arc`. The
  panel draws those direct calls into one engine node, with no backend listener,
  storage thread, or request/response storage queues. The `[worker] threads`
  config option is a scaling knob, not a mode switch: `1` (the default) is
  simply n = 1 of the same model. There is no maintenance thread either — the
  engine treats expired items as missing on access and reclaims expired
  segments under write pressure.
- **Proxy**: frontend threads (`pelikan_fe_i`) face clients, backend threads
  (`pelikan_be_i`) face upstream servers, connected by object queues.

Cache servers select the data-plane I/O backend once during startup. `mio` is
the portable default; on Linux, `server.io_backend = "ringline"` attempts a
Ringline acceptor and `ringline-worker-*` task runtime before traffic is
accepted. Unsupported kernel setup falls back to Mio, reusing the same shared
storage `Arc`, and records both the requested and active backend. TLS, admin,
and proxy sockets remain on Mio, and a live Ringline process never migrates
established connections. The admin `FlushHandle` also clones the shared `Arc`;
`flush_all` clears that one engine synchronously before acknowledging success.
Two conventions carry the meaning: heavier edges are bytes crossing the
process boundary (the wire); queue glyphs mark real internal queues, while thin
edges without a queue are direct calls or control signals. `pelikan_signal`
relays SIGINT/SIGTERM/SIGQUIT to `pelikan_admin` (port 9999). Under Ringline,
the admin thread sends shutdown through a signal queue/wake to the real
`pelikan_ringline_control` thread, which shuts down and monitors the runtime
and reports unexpected termination back to admin. A per-panel margin table
expands which binaries and protocols each panel covers.

## Life of a Request

[![Life of a request](diagrams/dataflow.svg)](diagrams/dataflow.svg?raw=1)

One request, traced as numbered stages on execution-context swimlanes, named
by the code's own verbs: `receive` (read + parse), `execute`, `send` (compose),
`flush`. The stage pitch is uniform across panels, so the panels compare column
by column and the differences that remain are the real ones:

- On a **server**, all four stages run in whichever Mio callback or Ringline
  connection task owns the session; stage ② executes directly against the
  `Arc`-shared engine, so the request crosses no storage queue.
- In the **proxy**, the request leaves through a backend thread to the
  upstream *servers* and the response retraces the path — six stages, with
  one queue crossing outbound (frontend → backend) and one on the return.

The control plane is intentionally out of scope here; the threading chart
carries it.

## Layer by Layer

**Runtime foundation** (`src/`)
- `common/` — shared types and traits across servers
- `config/` — TOML-based configuration parsing
- `logger/` — centralized logging with tracing
- `net/` — networking abstractions, event loops, TLS support
- `queues/` — inter-thread communication via queues and wakers
- `session/` — session management, buffered socket I/O

**Protocols** (`src/protocol/`)
- `admin/` — admin ASCII protocol for stats and management
- `memcache/` — Memcache ASCII protocol
- `resp/` — Redis RESP protocol with sorted set support
- `ping/` — minimal ping/pong protocol
- `http/` — HTTP protocol parser (not yet wired into any product; the admin
  HTTP endpoint on port 9998 is served by `core/admin` via `tiny_http`, not
  this crate)
- `common/` — shared protocol traits

**Storage** (`src/storage/`, `src/entrystore/`)
- `entrystore/` — the storage facade products program against; wraps the
  external `segcache` crate (segment-based engine from the `cache-rs`
  repository, [NSDI'21 paper](https://www.usenix.org/conference/nsdi21/presentation/yang-juncheng))
- `storage/types/` — shared storage type definitions
- `storage/bloom/` — bloom filter implementation (currently unused by any
  product)

**Server cores** (`src/core/`)
- `admin/` — the admin thread every binary runs
- `server/` — listener/worker event loops, thread management, signal
  handling
- `proxy/` — frontend/backend event loops for proxies

**Products** (`src/server/`, `src/proxy/`)
- `server/segcache/`, `server/rds/`, `server/pingserver/` — the three servers
- `proxy/ping/` — the ping proxy

## Design Principles

- **Workers never block.** Threads communicate over lockless queues, and the
  workers share a lock-free storage engine; the data plane holds no locks
  that a slow peer can convert into tail latency.
- **Control and data plane separation.** Management traffic (stats, version,
  shutdown) rides its own thread and port (9999 by default), so an operator
  inspecting a saturated server is not competing with cache traffic.
- **Per-module config and metrics.** Every module owns its configuration
  block and its metrics, and a product composes exactly the ones it uses.
- **Pluggable composition.** Protocols and storage engines are crates behind
  traits; adding one extends every product that wants it (see
  [The Big Picture](#the-big-picture)).

## Keeping This Document Honest

The three charts are generated — never hand-edited — by `cargo xtask
diagrams`:

- the build chart derives from `cargo metadata`, plus source greps for the
  wiring a manifest cannot see (which storage engine each product
  instantiates), and aborts on any unclassified crate;
- the runtime charts are anchored by source assertions (thread spawn sites,
  queue wiring, signal sets, ports, event-loop verbs — and absence
  assertions when a chart relies on something not existing) that abort
  generation when the code drifts.

CI regenerates all charts on every PR and fails on any diff against the
committed SVGs, so a refactor that changes the dependency structure or the
thread model fails the build instead of quietly invalidating a picture.
Regenerate after changing crate dependencies or thread/request-path code.
