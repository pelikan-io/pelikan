# ringline

**io_uring-native async I/O runtime for Linux.**

ringline is a thread-per-core I/O framework built directly on io_uring.
It provides an async/await API (`AsyncEventHandler`) on a single-threaded
executor with no work-stealing.

## What ringline is

- An io_uring-native runtime that exploits advanced kernel features: multishot
  recv, ring-provided buffers, SendMsgZc (zero-copy send), fixed file table
- Thread-per-core with CPU pinning — no work-stealing, no task migration
- Linux 6.0+ only — no epoll/kqueue/IOCP fallback, no portability tax

## What ringline is NOT

- A cross-platform runtime (Linux only, io_uring required)
- A Tokio replacement (different abstractions, not API-compatible)
- A general-purpose task scheduler (all tasks are `!Send`, pinned to cores)

## Quick Start

```rust
use ringline::{AsyncEventHandler, Config, ConnCtx, ParseResult, RinglineBuilder};

struct Echo;

impl AsyncEventHandler for Echo {
    fn on_accept(&self, conn: ConnCtx) -> impl std::future::Future<Output = ()> + 'static {
        async move {
            loop {
                let n = conn.with_data(|data| {
                    conn.send_nowait(data).ok();
                    ParseResult::Consumed(data.len())
                }).await;
                if n == 0 { break; }
            }
        }
    }
    fn create_for_worker(_id: usize) -> Self { Echo }
}

fn main() -> Result<(), ringline::Error> {
    let config = Config::default();
    let (_shutdown, handles) = RinglineBuilder::new(config)
        .bind("127.0.0.1:7878".parse().unwrap())
        .launch::<Echo>()?;
    for h in handles { h.join().unwrap()?; }
    Ok(())
}
```

## Architecture

```
                      ┌─────────────────────────────┐
                      │        Acceptor Thread       │
                      │   blocking accept4() loop    │
                      └──────────┬──────────────────┘
                                 │ round-robin
          ┌──────────────────────┼──────────────────────┐
          ▼                      ▼                      ▼
   ┌─────────────┐       ┌─────────────┐       ┌─────────────┐
   │  Worker 0   │       │  Worker 1   │       │  Worker N   │
   │ (CPU pinned)│       │ (CPU pinned)│       │ (CPU pinned)│
   │             │       │             │       │             │
   │  io_uring   │       │  io_uring   │       │  io_uring   │
   │  event loop │       │  event loop │       │  event loop │
   │             │       │             │       │             │
   │  Executor   │       │  Executor   │       │  Executor   │
   │  (futures)  │       │  (futures)  │       │  (futures)  │
   └─────────────┘       └─────────────┘       └─────────────┘
```

Each worker thread owns:
- A dedicated **io_uring** instance (SQ + CQ)
- A **ring-provided buffer pool** for recv (kernel selects buffers at completion time)
- A **send copy pool** for small sends and a **send slab** for scatter-gather zero-copy sends
- A **fixed file table** for O(1) fd lookups (no per-syscall fd table traversal)
- A **connection table** with generation-based stale detection

## io_uring Features Used

| Feature | Purpose |
|---------|---------|
| Multishot recv | Single SQE submission, multiple completions — no resubmission overhead |
| Ring-provided buffers | Kernel-managed recv buffer pool — kernel picks buffer at completion time |
| SendMsgZc | Zero-copy scatter-gather send — kernel DMAs directly from app buffers |
| Fixed file table | Direct descriptors — no per-syscall fd table lookup |
| IO_LINK chains | Atomic multi-step operations (connect + timeout) |
| COOP_TASKRUN | Reduced context switches |
| SINGLE_ISSUER | Lock-free kernel-side optimizations |

## Key Types

| Type | Description |
|------|-------------|
| `AsyncEventHandler` | Trait: `on_accept(ConnCtx) -> Future` — one task per connection |
| `ConnCtx` | Async connection context: `send()`, `send_nowait()`, `with_data()` |
| `WithDataFuture` | Future that resolves when recv data is available |
| `SendFuture` | Future that resolves when a send completes |
| `ConnectFuture` | Future that resolves when an outbound connection completes |
| `RinglineBuilder` | Builder: `RinglineBuilder::new(config).bind(addr).launch::<H>()` |
| `Config` | Runtime configuration (SQ size, buffer sizes, worker count, TLS, etc.) |
| `ShutdownHandle` | Triggers graceful shutdown of all workers |
| `GuardBox` | Type-erased container for `SendGuard` (64-byte inline storage, no heap) |
| `DriverCtx` | I/O context available in `on_tick()` and `on_notify()` callbacks |

## Platform Requirements

- **Linux 6.0+** (io_uring with required features)
- **x86_64** or **ARM64**

## MSRV

Rust 1.85+ (edition 2024)

## Examples

```bash
# Echo server (async API)
cargo run --example echo_async_server

# Echo client (connects to echo server)
cargo run --example echo_client

# Benchmark
cargo run --release --example echo_bench

# Outbound connect example
cargo run --example connect_echo

# TLS echo server
cargo run --example echo_tls_server
```

## License

MIT OR Apache-2.0
