//! Observability paths and their current gaps, checked against source.
use crate::claims::{verify, Claim};
use crate::svg::*;
use std::fs;

const OUT: &str = "docs/diagrams/observability.svg";
const W: f64 = 1760.0;
const H: f64 = 1250.0;
const BOX_W: f64 = 440.0;
const BOX_H: f64 = 116.0;
const XS: [f64; 3] = [40.0, 660.0, 1280.0];

const CLAIMS: &[Claim] = &[
    Claim {
        path: "src/core/server/src/ringline/single.rs",
        pattern: r"SESSION_RECV\.increment\(\)",
        what: "Ringline handlers update shared session counters",
    },
    Claim {
        path: "src/session/src/server.rs",
        pattern: r"SESSION_RECV\.increment\(\)",
        what: "Mio sessions update the same counters",
    },
    Claim {
        path: "src/session/src/lib.rs",
        pattern: r"pub static REQUEST_LATENCY: AtomicHistogram",
        what: "request latency histogram is process-global",
    },
    Claim {
        path: "src/core/server/src/ringline/session.rs",
        pattern: r"REQUEST_LATENCY\.increment",
        what: "Ringline records latency",
    },
    Claim {
        path: "src/protocol/admin/src/snapshots.rs",
        pattern: r"Lazy::new\(\|\| Arc::new\(RwLock::new\(Snapshots::new\(\)\)\)\)",
        what: "histogram snapshots initialize lazily",
    },
    Claim {
        path: "src/core/admin/src/lib.rs",
        pattern: r"let snapshots = SNAPSHOTS\.read\(\)",
        what: "admin reads the shared snapshots",
    },
    Claim {
        path: "src/core/admin/src/lib.rs",
        pattern: r"counter\.value\(\)",
        what: "admin reads counters live",
    },
    Claim {
        path: "src/core/admin/src/lib.rs",
        pattern: r#""/metrics" =>"#,
        what: "Prometheus endpoint",
    },
    Claim {
        path: "src/protocol/admin/src/admin.rs",
        pattern: r"for metric in &metriken::metrics\(\)",
        what: "ASCII stats reads the same registry",
    },
    Claim {
        path: "ringline:src/metrics.rs",
        pattern: r"pub static CONNECTIONS: ShardedCounterGroup",
        what: "upstream runtime uses sharded counter groups",
    },
    Claim {
        path: "src/logger/src/lib.rs",
        pattern: r"tracing_appender::non_blocking\(",
        what: "logger queues output to appenders",
    },
    Claim {
        path: "src/logger/src/lib.rs",
        pattern: r"COUNTER\.fetch_add\(1, ::std::sync::atomic::Ordering::Relaxed\) % sample == 0",
        what: "command logging samples at the callsite",
    },
    Claim {
        path: "src/logger/src/lib.rs",
        pattern: r"_guards: Vec<WorkerGuard>",
        what: "LogDrain retains appender workers",
    },
    Claim {
        path: "src/core/admin/src/lib.rs",
        pattern: r"_log_drain: LogDrain",
        what: "admin owns the logging lifetime guard",
    },
    Claim {
        path: "tracing-appender:src/non_blocking.rs",
        pattern: r#"thread_name: "tracing-appender"\.to_string\(\)"#,
        what: "default appender thread name",
    },
    Claim {
        path: "tracing-appender:src/non_blocking.rs",
        pattern: r"is_lossy: true",
        what: "default appender is lossy",
    },
    Claim {
        path: "tracing-appender:src/non_blocking.rs",
        pattern: r"bounded\(buffered_lines_limit\)",
        what: "appender queues are bounded",
    },
];

const NEG_CLAIMS: &[Claim] = &[
    Claim {
        path: "src/core/admin/src/lib.rs",
        pattern: r"CounterGroup|load_counters",
        what: "HTTP exporter does not export counter groups",
    },
    Claim {
        path: "src/protocol/admin/src/admin.rs",
        pattern: r"CounterGroup|load_counters",
        what: "ASCII exporter does not export counter groups",
    },
];

// Check the whole application source, not just one presumed sampling thread.
fn check_snapshot_refresh(path: &std::path::Path, refresh: &regex::Regex) {
    for entry in fs::read_dir(path).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            check_snapshot_refresh(&path, refresh);
        } else if path.extension().is_some_and(|ext| ext == "rs") {
            let source = fs::read_to_string(&path).unwrap();
            assert!(
                !refresh.is_match(&source),
                "snapshot refresh added in {}; update observability chart",
                path.display()
            );
        }
    }
}

fn label(parts: &mut Vec<String>, x: f64, y: f64, s: &str) {
    parts.push(text(x, y, s).size(18).build());
}

fn node(parts: &mut Vec<String>, column: usize, y: f64, title: &str, lines: &[&str], fill: &str) {
    let x = XS[column];
    assert!(x + BOX_W <= W - 40.0 && y + BOX_H < H - 20.0);
    assert!(label_w_at(title, 21.0) < BOX_W - 32.0);
    let resolved_fill = if fill == FILL_CORE { "white" } else { fill };
    let shape = rect(x, y, BOX_W, BOX_H, resolved_fill).rx(6.0);
    let shape = if fill == FILL_EXTERNAL {
        shape.dashed()
    } else {
        shape
    };
    parts.push(shape.build());
    let heading = text(x + BOX_W / 2.0, y + 24.0, title).size(21).bold();
    let heading = if title == "pelikan_admin" || title == "tracing-appender" {
        heading.mono()
    } else if fill == FILL_EXTERNAL {
        heading.italic()
    } else {
        heading
    };
    parts.push(heading.build());
    let center = y + 75.0;
    let first = center - (lines.len().saturating_sub(1) as f64 * 24.0) / 2.0;
    assert!(
        first - 9.0 > y + 40.0
            && first + (lines.len().saturating_sub(1) as f64 * 24.0) + 9.0 < y + BOX_H - 10.0
    );
    for (i, line) in lines.iter().enumerate() {
        assert!(
            label_w_at(line, 18.0) < BOX_W - 32.0,
            "label too wide: {line}"
        );
        label(parts, x + BOX_W / 2.0, first + i as f64 * 24.0, line);
    }
}

fn across(parts: &mut Vec<String>, column: usize, y: f64, caption: &str) {
    let start = XS[column] + BOX_W;
    let end = XS[column + 1];
    assert!(label_w_at(caption, 18.0) < end - start - 24.0);
    parts.push(ortho(&[(start, y), (end, y)]).build());
    label(parts, (start + end) / 2.0, y - 24.0, caption);
}

pub fn generate() {
    verify(CLAIMS, NEG_CLAIMS);
    check_snapshot_refresh(
        std::path::Path::new("src"),
        &regex::Regex::new(r"SNAPSHOTS\s*\.\s*write\s*\(|Snapshots::update").unwrap(),
    );
    let mut p = vec![ARROW_DEFS.to_string()];
    p.push(
        text(40.0, 32.0, "Observability across execution threads")
            .start()
            .size(24)
            .bold()
            .build(),
    );
    p.push(
        text(
            40.0,
            69.0,
            "Metrics: writers update shared state; the admin thread reads it on demand",
        )
        .start()
        .size(21)
        .build(),
    );

    node(
        &mut p,
        0,
        108.0,
        "Mio / Ringline execution threads",
        &[
            "Session, protocol, storage, admin",
            "record counters and gauges",
        ],
        FILL_CORE,
    );
    node(
        &mut p,
        1,
        108.0,
        "Shared metriken registry",
        &[
            "Process-global counters and gauges",
            "Values combine updates across threads",
        ],
        FILL_FOUNDATION,
    );
    node(
        &mut p,
        2,
        108.0,
        "pelikan_admin",
        &[
            "Reads current values at export time",
            "No per-request metrics queue",
        ],
        FILL_CORE,
    );
    across(&mut p, 0, 166.0, "update");
    across(&mut p, 1, 166.0, "read values");

    node(
        &mut p,
        0,
        286.0,
        "Shared latency histogram",
        &[
            "Workers record REQUEST_LATENCY",
            "AtomicHistogram holds the samples",
        ],
        FILL_FOUNDATION,
    );
    node(
        &mut p,
        1,
        286.0,
        "Shared SNAPSHOTS (RwLock)",
        &[
            "Lazily loaded histogram snapshots",
            "No refresh caller in current source",
        ],
        FILL_FOUNDATION,
    );
    node(
        &mut p,
        2,
        286.0,
        "pelikan_admin",
        &["Reads snapshot percentiles", "Values can become stale"],
        FILL_CORE,
    );
    across(&mut p, 0, 344.0, "initial load");
    across(&mut p, 1, 344.0, "read lock");

    node(
        &mut p,
        0,
        464.0,
        "Ringline runtime workers",
        &[
            "Record runtime counter groups",
            "Each worker updates its own shard",
        ],
        FILL_CORE,
    );
    node(
        &mut p,
        1,
        464.0,
        "ShardedCounterGroup",
        &[
            "In the same metriken registry",
            "Not handled by current exporters",
        ],
        FILL_FOUNDATION,
    );
    across(&mut p, 0, 522.0, "update shard");
    node(
        &mut p,
        2,
        464.0,
        "Operator / metrics collector",
        &[
            "ASCII stats; /metrics; JSON; /vars",
            "Admin formats and returns responses",
        ],
        FILL_EXTERNAL,
    );
    // Export flows stay inside the admin column; labels are outside the gap.
    p.push(ortho(&[(1500.0, 402.0), (1500.0, 464.0)]).network().build());
    label(&mut p, 1630.0, 433.0, "export");

    p.push(
        text(
            40.0,
            638.0,
            "Logging: format on the calling thread, enqueue bytes, write on appender workers",
        )
        .start()
        .size(21)
        .build(),
    );
    node(
        &mut p,
        0,
        686.0,
        "Calling runtime thread",
        &[
            "Debug events and sampled klog calls",
            "tracing formats the record here",
        ],
        FILL_CORE,
    );
    node(
        &mut p,
        1,
        686.0,
        "Non-blocking appender queues",
        &[
            "Debug output; optional klog output",
            "Bounded queues; default mode is lossy",
        ],
        FILL_FOUNDATION,
    );
    node(
        &mut p,
        2,
        686.0,
        "tracing-appender",
        &[
            "Drain each configured writer queue",
            "Perform output I/O off the caller",
        ],
        FILL_CORE,
    );
    across(&mut p, 0, 744.0, "log bytes");
    across(&mut p, 1, 744.0, "dequeue");
    node(
        &mut p,
        0,
        884.0,
        "pelikan_admin",
        &[
            "Owns LogDrain / WorkerGuard values",
            "Guards keep appender workers alive",
        ],
        FILL_CORE,
    );
    node(
        &mut p,
        1,
        884.0,
        "Logging configuration",
        &[
            "Level filter and callsite sample rate",
            "Optional file rotation and compression",
        ],
        FILL_FOUNDATION,
    );
    node(
        &mut p,
        2,
        884.0,
        "stdout / log files",
        &[
            "Debug output and optional klog file",
            "klog also reaches the debug layer",
        ],
        FILL_EXTERNAL,
    );
    p.push(ortho(&[(1500.0, 802.0), (1500.0, 884.0)]).network().build());
    label(&mut p, 1630.0, 843.0, "write");

    p.push(
        text(40.0, 1055.0, "Reading this chart")
            .start()
            .size(21)
            .bold()
            .build(),
    );
    for (i, line) in [
        "Plain boxes are execution contexts; gray boxes are shared state or queues; dashed boxes are external consumers and outputs.",
        "Mio, Ringline, and proxy threads use the same process registry and logging subscriber. Metric families choose their own synchronization.",
        "Counters are read individually: an export is not an atomic snapshot of all threads. Histogram refresh and counter-group export are current gaps.",
        "The tracing-appender label is the dependency's default thread name; optional klog output creates another worker with that name.",
    ].iter().enumerate() {
        assert!(label_w_at(line, 18.0) < W - 80.0);
        p.push(text(40.0, 1094.0 + i as f64 * 34.0, line).start().size(18).build());
    }
    fs::write(OUT, svg_document(W, H, "cargo xtask diagrams", &p)).unwrap();
    eprintln!("generated: {OUT}");
}
