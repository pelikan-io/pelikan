#![allow(clippy::manual_async_fn)]
//! Read-only NVMe passthrough smoke test against a real device.
//!
//! Exercises the full `IORING_OP_URING_CMD` path end-to-end: device open,
//! two `nvme_read`s of the first LBAs, and an `nvme_flush`. Never writes.
//!
//! Usage (needs root for /dev/ng*):
//!
//! ```text
//! sudo target/release/examples/nvme_smoke /dev/ng0n1 [nsid] [block_size]
//! ```
//!
//! Prints the first bytes of LBA 0 so the result can be cross-checked
//! against the block device (`dd if=/dev/nvmeXnY bs=<bs> count=1 | xxd`).

use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};

use ringline::{AsyncEventHandler, ConfigBuilder, ConnCtx, ParseResult, RinglineBuilder};

static DONE: AtomicBool = AtomicBool::new(false);

struct SmokeHandler;

impl AsyncEventHandler for SmokeHandler {
    fn on_accept(&self, conn: ConnCtx) -> impl Future<Output = ()> + 'static {
        async move {
            // Server side unused — client-only mode.
            let _ = conn.with_data(|d| ParseResult::Consumed(d.len())).await;
        }
    }

    fn on_tick(&mut self, ctx: &mut ringline::DriverCtx<'_>) {
        // Spawn the smoke task once, from inside the runtime.
        static SPAWNED: AtomicBool = AtomicBool::new(false);
        if SPAWNED.swap(true, Ordering::AcqRel) {
            if DONE.load(Ordering::Acquire) {
                ctx.request_shutdown();
            }
            return;
        }

        let path = std::env::args()
            .nth(1)
            .expect("usage: nvme_smoke /dev/ngXnY [nsid] [bs]");
        let nsid: u32 = std::env::args()
            .nth(2)
            .and_then(|s| s.parse().ok())
            .unwrap_or(1);
        let bs: u32 = std::env::args()
            .nth(3)
            .and_then(|s| s.parse().ok())
            .unwrap_or(512);

        let _ = ringline::spawn(async move {
            let run = async {
                let device = ringline::open_nvme_device(&path, nsid)?;
                println!("opened {path} nsid={nsid} block_size={bs}");

                // 4 KiB reads from LBA 0, page-aligned buffers.
                let blocks: u16 = (4096 / bs) as u16;
                let mut buf_a = vec![0u8; 8192];
                let mut buf_b = vec![0u8; 8192];
                let a_ptr = {
                    let p = buf_a.as_mut_ptr() as usize;
                    let aligned = (p + 4095) & !4095;
                    aligned as u64
                };
                let b_ptr = {
                    let p = buf_b.as_mut_ptr() as usize;
                    let aligned = (p + 4095) & !4095;
                    aligned as u64
                };

                // SAFETY: aligned regions of 4096 bytes inside live Vecs that
                // outlive the awaits below.
                let res_a = unsafe { ringline::nvme_read(device, 0, blocks, a_ptr, 4096)? }.await?;
                println!("nvme_read #1 (LBA 0, {blocks} blocks): {res_a:?}");
                let res_b = unsafe { ringline::nvme_read(device, 0, blocks, b_ptr, 4096)? }.await?;
                println!("nvme_read #2 (LBA 0, {blocks} blocks): {res_b:?}");

                let a = unsafe { std::slice::from_raw_parts(a_ptr as *const u8, 4096) };
                let b = unsafe { std::slice::from_raw_parts(b_ptr as *const u8, 4096) };
                if a != b {
                    return Err(std::io::Error::other("re-read of LBA 0 differs"));
                }
                let nonzero = a.iter().filter(|&&x| x != 0).count();
                println!(
                    "LBA0[0..32] = {:02x?}  (nonzero bytes in 4KiB: {nonzero})",
                    &a[..32]
                );

                let res_f = ringline::nvme_flush(device)?.await?;
                println!("nvme_flush: {res_f:?}");
                Ok::<(), std::io::Error>(())
            };
            match run.await {
                Ok(()) => println!("NVME SMOKE: PASS"),
                Err(e) => {
                    println!("NVME SMOKE: FAIL: {e}");
                    std::process::exit(1);
                }
            }
            DONE.store(true, Ordering::Release);
        });
    }

    fn create_for_worker(_id: usize) -> Self {
        SmokeHandler
    }
}

fn main() {
    let config = ConfigBuilder::new()
        .workers(1)
        .pin_to_core(false)
        .nvme(ringline::NvmeConfig::default())
        .build()
        .expect("valid config");

    let (_shutdown, handles) = RinglineBuilder::new(config)
        .launch::<SmokeHandler>()
        .expect("launch failed");
    for h in handles {
        h.join().unwrap().unwrap();
    }
}
