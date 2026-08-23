//! The threading architecture chart: the runtime thread model per binary in
//! a launch-time backend fork, a backend-neutral shared-storage topology, and
//! a proxy panel. Named runtime threads are literal names matching `top -H`;
//! the storage panel deliberately uses neutral execution contexts because Mio
//! callbacks and Ringline tasks share one ownership model. Heavier edges carry
//! bytes across the process boundary (wire) vs internal queues (object). The
//! thread, queue, control, and shared-storage inventory is asserted against
//! the sources at generation time.

use crate::claims::{verify, Claim};
use crate::svg::*;
use std::fs;

const OUT: &str = "docs/diagrams/threading.svg";

const CLAIMS: &[Claim] = &[
    Claim {
        path: "vendor/ringline-0.5.5/src/worker.rs",
        pattern: r#"name\(format!\("ringline-worker-\{worker_id\}"\)\)"#,
        what: "Ringline worker thread spawn",
    },
    Claim {
        path: "vendor/ringline-0.5.5/src/worker.rs",
        pattern: r#"name\("ringline-acceptor"\.to_string\(\)\)"#,
        what: "Ringline acceptor thread spawn",
    },
    Claim {
        path: "vendor/ringline-0.5.5/src/worker.rs",
        pattern: r"crossbeam_channel::bounded::<\(RawFd, SocketAddr\)>",
        what: "Ringline accepted-fd queues are bounded per worker",
    },
    Claim {
        path: "vendor/ringline-0.5.5/src/acceptor.rs",
        pattern: r"worker_channels\[worker_idx\]\.try_send\(\(fd, peer_addr\)\)",
        what: "Ringline acceptor queues accepted fds to workers",
    },
    Claim {
        path: "vendor/ringline-0.5.5/src/acceptor.rs",
        pattern: r"worker_wake_handles\[worker_idx\]\.wake\(\)",
        what: "Ringline acceptor wakes the selected worker",
    },
    Claim {
        path: "vendor/ringline-0.5.5/src/backend/uring/event_loop.rs",
        pattern: r"self\.executor\.task_slab\.spawn\(conn_index, future\);",
        what: "Ringline io_uring worker schedules the accepted connection task",
    },
    Claim {
        path: "vendor/ringline-0.5.5/src/backend/mio/event_loop.rs",
        pattern: r"self\.executor\.task_slab\.spawn\(conn_index, future\);",
        what: "Ringline Mio worker schedules the accepted connection task",
    },
    Claim {
        path: "src/core/server/src/process.rs",
        pattern: r#"name\(format!\("\{THREAD_PREFIX\}_admin"\)\)"#,
        what: "admin thread spawn",
    },
    Claim {
        path: "src/core/server/src/process.rs",
        pattern: r#"name\(format!\("\{THREAD_PREFIX\}_listener"\)\)"#,
        what: "listener thread spawn",
    },
    Claim {
        path: "src/core/server/src/workers/mod.rs",
        pattern: r#"name\(format!\("\{THREAD_PREFIX\}_work_\{id\}"\)\)"#,
        what: "worker thread spawn",
    },
    Claim {
        path: "src/core/server/src/process.rs",
        pattern: r"let storage = Arc::new\(storage\);",
        what: "workers share one Arc'd storage",
    },
    Claim {
        path: "src/core/admin/src/lib.rs",
        pattern: r"pub type FlushHandle = Arc<dyn Fn\(\) \+ Send \+ Sync>;",
        what: "admin holds the clear handle for flush_all",
    },
    Claim {
        path: "src/core/server/src/process.rs",
        pattern: r#"name\(format!\("\{THREAD_PREFIX\}_signal"\)\)"#,
        what: "signal handler thread spawn",
    },
    Claim {
        path: "src/core/server/src/process.rs",
        pattern: r#"name\(format!\("\{THREAD_PREFIX\}_ringline_control"\)\)"#,
        what: "Pelikan Ringline control thread spawn",
    },
    Claim {
        path: "src/core/server/src/process.rs",
        pattern: r"let \(mut admin_signal_queues, mut bridge_signal_queues\)",
        what: "admin-to-Ringline-control signal queue",
    },
    Claim {
        path: "src/core/server/src/process.rs",
        pattern: r"bridge_signal_tx\.try_send\(Signal::Shutdown\)",
        what: "Ringline control reports runtime termination to admin",
    },
    Claim {
        path: "src/core/server/src/process.rs",
        pattern: r"let \(mut signal_queue_tx, mut signal_queue_rx\)",
        what: "admin signal broadcast queues",
    },
    Claim {
        path: "src/core/server/src/process.rs",
        pattern: r"let mut thread_wakers = vec!\[listener\.waker\(\)\]",
        what: "Mio signal broadcast includes the listener waker",
    },
    Claim {
        path: "src/core/server/src/process.rs",
        pattern: r"let \(mut listener_session_queues, worker_session_queues\)",
        what: "listener->worker session queues",
    },
    Claim {
        path: "src/core/server/src/process.rs",
        pattern: r"thread_wakers\.extend_from_slice\(&workers\.wakers\(\)\)",
        what: "signal queues include every worker's waker",
    },
    Claim {
        path: "src/core/server/src/process.rs",
        pattern: r"spawn_signal_handler\(signal_tx\.clone\(\)\);",
        what: "Mio signal thread feeds the admin signal channel",
    },
    Claim {
        path: "src/core/admin/src/lib.rs",
        pattern: r"self\.signal_queue_tx\.try_send_all\(Signal::Shutdown\)",
        what: "admin broadcasts Mio shutdown to sibling threads",
    },
    Claim {
        path: "src/core/admin/src/lib.rs",
        pattern: r"self\.signal_queue_tx\.wake\(\)",
        what: "admin wakes threads after a signal broadcast",
    },
    Claim {
        path: "src/core/server/src/ringline/single.rs",
        pattern: r"storage: Arc::clone\(&storage\),",
        what: "Ringline workers clone the process's shared storage Arc",
    },
    Claim {
        path: "src/core/server/src/process.rs",
        pattern: r"SIGHUP, SIGINT, SIGTERM, SIGQUIT",
        what: "signal handler signal set",
    },
    Claim {
        path: "src/core/proxy/src/process.rs",
        pattern: r#"name\(format!\("\{THREAD_PREFIX\}_signal"\)\)"#,
        what: "proxy signal handler thread spawn",
    },
    Claim {
        path: "src/core/proxy/src/process.rs",
        pattern: r"SIGHUP, SIGINT, SIGTERM, SIGQUIT",
        what: "proxy signal handler signal set",
    },
    Claim {
        path: "src/core/proxy/src/process.rs",
        pattern: r#"name\(format!\("\{THREAD_PREFIX\}_fe_\{i\}"\)\)"#,
        what: "proxy frontend worker spawn",
    },
    Claim {
        path: "src/core/proxy/src/process.rs",
        pattern: r#"name\(format!\("\{THREAD_PREFIX\}_be_\{i\}"\)\)"#,
        what: "proxy backend worker spawn",
    },
    Claim {
        path: "src/core/proxy/src/process.rs",
        pattern: r"fe_data_queues, be_data_queues",
        what: "proxy frontend<->backend data queues",
    },
    Claim {
        path: "src/core/proxy/src/backend.rs",
        pattern: r"TcpStream::connect\(endpoint\)",
        what: "proxy backend connects to upstream endpoints",
    },
    Claim {
        path: "src/config/src/server.rs",
        pattern: r#"const SERVER_PORT: &str = "12321""#,
        what: "default data port",
    },
    Claim {
        path: "src/config/src/admin.rs",
        pattern: r#"const ADMIN_PORT: &str = "9999""#,
        what: "default admin port",
    },
];

/// Claims of absence: the diagram relies on these NOT existing.
const NEG_CLAIMS: &[Claim] = &[
    Claim {
        path: "src/core/server/src/workers/mod.rs",
        pattern: r"\}_storage",
        what: "no storage thread spawn",
    },
    Claim {
        path: "src/core/server/src/workers/mod.rs",
        pattern: r"storage_wakers",
        what: "no worker<->storage data queues",
    },
    Claim {
        path: "src/core/server/src/workers/worker.rs",
        pattern: r"\.expire\(",
        what: "no periodic expiration in the worker event loop",
    },
    Claim {
        path: "src/core/server/src/workers/worker.rs",
        pattern: r"storage\.clear\(",
        what: "workers do not clear; the admin thread owns flush_all",
    },
];

type Chip = (&'static str, &'static str);
const CHIP_PROTOCOL: Chip = ("protocol-*", FILL_PROTOCOL);
const CHIP_PROTOCOL_ADMIN: Chip = ("protocol-admin", FILL_PROTOCOL);
const CHIP_ENTRYSTORE: Chip = ("entrystore", FILL_STORAGE);
const CHIP_SEGCACHE: Chip = ("segcache", FILL_STORAGE);

// uniform geometry: every thread container is the same size; externals
// share their own smaller dashed size
const TB_W: f64 = 290.0;
const TB_H: f64 = 192.0;
const EXT_W: f64 = 124.0;
const EXT_H: f64 = 68.0;
const GAP: f64 = 40.0; // minimum arrow length between columns
const ELBOW: f64 = 130.0; // elbow verticals route outside the queue labels
const PANEL_W: f64 = 2190.0;

const TS: TypeScale = TYPE_SCALE;

/// Chart-local default: body text at this chart's scale.
fn text(x: f64, y: f64, s: &str) -> crate::svg::Text {
    crate::svg::text(x, y, s).size(TS.body)
}
const X0: f64 = 24.0;

fn gap_for(label: &str) -> f64 {
    (label_w_at(label, TS.body as f64) + 10.0).max(GAP)
}

fn queue_gap(label: &str) -> f64 {
    // labels wrap at " (" so only the longest line drives the overhang
    let longest = label.split(" (").max_by_key(|s| s.len()).unwrap();
    (GAP + 8.0)
        .max((label_w_at(longest, TS.body as f64) - 50.0) / 2.0 + 8.0)
        .max(ELBOW + 16.0)
}

fn thread_box(
    parts: &mut Vec<String>,
    x: f64,
    y: f64,
    name: &str,
    sub: Option<&str>,
    chips: &[Chip],
    external: bool,
) {
    thread_box_w(parts, x, y, TB_W, name, sub, chips, external);
}

#[allow(clippy::too_many_arguments)]
fn thread_box_w(
    parts: &mut Vec<String>,
    x: f64,
    y: f64,
    width: f64,
    name: &str,
    sub: Option<&str>,
    chips: &[Chip],
    external: bool,
) {
    parts.push(rect(x, y, width, TB_H, "#FFFFFF").rx(10.0).build());
    // name row, optional sub row, and the one-column bar stack (the
    // architecture chart's composition-bar idiom) vertically centered as
    // one block
    let bh = 36.0;
    let name_h = 30.0;
    let sub_h = if sub.is_some() { 38.0 } else { 0.0 };
    let bars_h = if chips.is_empty() {
        0.0
    } else {
        chips.len() as f64 * (bh + 6.0) - 6.0 + 18.0
    };
    let top = y + (TB_H - name_h - sub_h - bars_h) / 2.0;
    parts.push(
        text(x + width / 2.0, top + name_h / 2.0, name)
            .size(TS.h2)
            .bold()
            .mono()
            .build(),
    );
    if let Some(sub) = sub {
        let mut t = text(x + width / 2.0, top + name_h + sub_h / 2.0, sub).fill("#333");
        if external {
            t = t.italic();
        }
        parts.push(t.build());
    }
    if !chips.is_empty() {
        let bw = width - 40.0;
        let mut cy = top + name_h + sub_h + 18.0;
        for (label, cfill) in chips {
            parts.push(rect(x + 20.0, cy, bw, bh, cfill).sw(1.0).build());
            parts.push(text(x + width / 2.0, cy + bh / 2.0, label).build());
            cy += bh + 6.0;
        }
    }
}

fn queue_glyph(parts: &mut Vec<String>, x: f64, y: f64, w: f64, h: f64, label: &str) {
    let ncells = 5;
    let cw = w / ncells as f64;
    for i in 0..ncells {
        parts.push(
            rect(x + i as f64 * cw, y, cw, h, QUEUE_FILL)
                .sw(1.0)
                .build(),
        );
    }
    if let Some((first, rest)) = label.split_once(" (") {
        parts.push(text(x + w / 2.0, y - 42.0, first).fill("#555").build());
        parts.push(
            text(x + w / 2.0, y - 16.0, &format!("({rest}"))
                .fill("#555")
                .build(),
        );
    } else {
        parts.push(text(x + w / 2.0, y - 16.0, label).fill("#555").build());
    }
}

fn ext_box(parts: &mut Vec<String>, x: f64, y_row: f64, name: &str) {
    let y = y_row + (TB_H - EXT_H) / 2.0;
    parts.push(rect(x, y, EXT_W, EXT_H, FILL_EXTERNAL).dashed().build());
    parts.push(
        text(x + EXT_W / 2.0, y + EXT_H / 2.0, name)
            .size(TS.h2)
            .italic()
            .build(),
    );
}

/// Two uniform thread boxes with an ellipsis between them; returns the
/// second box's y.
fn worker_column(
    parts: &mut Vec<String>,
    x: f64,
    y_top: f64,
    names: (&str, &str),
    chips: &[Chip],
) -> f64 {
    let y1 = y_top + TB_H + 40.0;
    thread_box(parts, x, y_top, names.0, None, chips, false);
    thread_box(parts, x, y1, names.1, None, chips, false);
    let dots_cy = (y_top + TB_H + y1) / 2.0;
    for dy in [-8.0, 0.0, 8.0] {
        parts.push(format!(
            "<circle cx=\"{:.0}\" cy=\"{:.0}\" r=\"2\" fill=\"#777\"/>",
            x + TB_W / 2.0,
            dots_cy + dy
        ));
    }
    y1
}

/// Right-margin block: panel title over a binary->protocol mini-table, the
/// whole block vertically centered.
fn margin_block(parts: &mut Vec<String>, cx: f64, cy: f64, title: &str, rows: &[(&str, &str)]) {
    let (row_h, title_h, gap) = (34.0, 38.0, 12.0);
    let block_h = title_h + gap + rows.len() as f64 * row_h;
    let ty = cy - block_h / 2.0 + title_h / 2.0;
    parts.push(text(cx, ty, title).size(TS.h1).bold().build());
    let mut ry = ty + title_h / 2.0 + gap + row_h / 2.0;
    for (binary, proto) in rows {
        parts.push(text(cx - 6.0, ry, binary).fill("#555").end().build());
        parts.push(text(cx, ry, ":").size(TS.h2).fill("#555").build());
        parts.push(text(cx + 8.0, ry, proto).fill("#555").start().build());
        ry += row_h;
    }
}

fn server_panel(y0: f64, title: &str, rows: &[(&str, &str)]) -> (Vec<String>, f64) {
    let mut parts = Vec::new();
    let h = 560.0;
    parts.push(
        rect(X0, y0, PANEL_W, h, PANEL_FILL)
            .stroke(PANEL_BORDER)
            .sw(2.0)
            .build(),
    );
    margin_block(&mut parts, X0 + PANEL_W + 130.0, y0 + h / 2.0, title, rows);

    let context_x = X0 + 120.0;
    let context_w = 390.0;
    let context_0_y = y0 + 45.0;
    let context_n_y = y0 + h - TB_H - 45.0;
    thread_box_w(
        &mut parts,
        context_x,
        context_0_y,
        context_w,
        "execution context 0",
        Some("Mio callback / Ringline task"),
        &[CHIP_PROTOCOL],
        false,
    );
    thread_box_w(
        &mut parts,
        context_x,
        context_n_y,
        context_w,
        "execution context n-1",
        Some("Mio callback / Ringline task"),
        &[CHIP_PROTOCOL],
        false,
    );
    let dots_cy = y0 + h / 2.0;
    for dy in [-8.0, 0.0, 8.0] {
        parts.push(format!(
            "<circle cx=\"{:.0}\" cy=\"{:.0}\" r=\"2\" fill=\"#777\"/>",
            context_x + context_w / 2.0,
            dots_cy + dy
        ));
    }

    let storage_x = context_x + 720.0;
    let storage_w = 390.0;
    let storage_y = y0 + (h - TB_H) / 2.0;
    thread_box_w(
        &mut parts,
        storage_x,
        storage_y,
        storage_w,
        "one Arc-shared engine",
        Some("process-owned; not a thread"),
        &[CHIP_ENTRYSTORE, CHIP_SEGCACHE],
        false,
    );

    let storage_mid = storage_y + TB_H / 2.0;
    let elbow_x = storage_x - 180.0;
    for (context_y, storage_offset) in [(context_0_y, -42.0), (context_n_y, 42.0)] {
        parts.push(
            ortho(&[
                (context_x + context_w, context_y + TB_H / 2.0),
                (elbow_x, context_y + TB_H / 2.0),
                (elbow_x, storage_mid + storage_offset),
                (storage_x, storage_mid + storage_offset),
            ])
            .both()
            .build(),
        );
    }
    parts.push(
        text(
            (context_x + context_w + storage_x) / 2.0,
            y0 + h / 2.0,
            "direct execute / response",
        )
        .fill("#555")
        .build(),
    );

    let admin_x = storage_x + 720.0;
    let admin_w = 390.0;
    thread_box_w(
        &mut parts,
        admin_x,
        storage_y,
        admin_w,
        "pelikan_admin",
        Some("FlushHandle clones same Arc"),
        &[CHIP_PROTOCOL_ADMIN],
        false,
    );
    parts.push(
        ortho(&[(admin_x, storage_mid), (storage_x + storage_w, storage_mid)])
            .signal()
            .build(),
    );
    parts.push(
        text(
            (storage_x + storage_w + admin_x) / 2.0,
            storage_mid - 18.0,
            "synchronous clear",
        )
        .fill("#777")
        .build(),
    );
    (parts, h)
}

fn backend_choice_panel(y0: f64) -> (Vec<String>, f64) {
    let mut parts = Vec::new();
    let h = 1000.0;
    parts.push(
        rect(X0, y0, PANEL_W, h, PANEL_FILL)
            .stroke(PANEL_BORDER)
            .sw(2.0)
            .build(),
    );
    margin_block(
        &mut parts,
        X0 + PANEL_W + 130.0,
        y0 + h / 2.0,
        "launch-time backend fork",
        &[("cache servers", "plain TCP")],
    );

    let cfg_x = X0 + 40.0;
    let mio_x = cfg_x + 430.0;
    let dispatch_x = mio_x + 430.0;
    let worker_x = dispatch_x + 430.0;
    let mio_y = y0 + 55.0;
    let ring_y = y0 + 380.0;
    thread_box(
        &mut parts,
        cfg_x,
        y0 + 164.0,
        "server.io_backend",
        Some("resolved once"),
        &[],
        true,
    );
    thread_box(
        &mut parts,
        mio_x,
        mio_y,
        "pelikan_listener",
        Some("Mio accept/readiness"),
        &[],
        false,
    );
    queue_glyph(
        &mut parts,
        dispatch_x,
        mio_y + TB_H / 2.0 - 11.0,
        50.0,
        22.0,
        "session queue",
    );
    thread_box(
        &mut parts,
        worker_x,
        mio_y,
        "pelikan_work_i",
        Some("callback state machine"),
        &[CHIP_PROTOCOL],
        false,
    );
    thread_box(
        &mut parts,
        mio_x,
        ring_y,
        "ringline-acceptor",
        Some("Ringline accept"),
        &[],
        false,
    );
    queue_glyph(
        &mut parts,
        dispatch_x,
        ring_y + TB_H / 2.0 - 11.0,
        50.0,
        22.0,
        "accepted fd queue / wake",
    );
    thread_box(
        &mut parts,
        worker_x,
        ring_y,
        "ringline-worker-i",
        Some("schedules async tasks"),
        &[CHIP_PROTOCOL],
        false,
    );

    let control_y = y0 + 675.0;
    let control_mid = control_y + TB_H / 2.0;
    thread_box(
        &mut parts,
        cfg_x,
        control_y,
        "pelikan_signal",
        Some("SIGINT/TERM/QUIT"),
        &[],
        false,
    );
    thread_box(
        &mut parts,
        mio_x,
        control_y,
        "pelikan_admin",
        Some(":9999 (Mio)"),
        &[CHIP_PROTOCOL_ADMIN],
        false,
    );
    queue_glyph(
        &mut parts,
        dispatch_x,
        control_mid - 11.0,
        50.0,
        22.0,
        "signal queue / wake",
    );
    let control_w = 430.0;
    thread_box_w(
        &mut parts,
        worker_x,
        control_y,
        control_w,
        "pelikan_ringline_control",
        Some("shutdown + monitor"),
        &[],
        false,
    );

    let broadcast_q_x = dispatch_x + 120.0;
    let broadcast_q_y = y0 + 300.0;
    let broadcast_q_mid = broadcast_q_y + 11.0;
    queue_glyph(
        &mut parts,
        broadcast_q_x,
        broadcast_q_y,
        50.0,
        22.0,
        "Mio broadcast queue / wake",
    );

    let cfg_mid = y0 + 164.0 + TB_H / 2.0;
    parts.push(
        ortho(&[
            (cfg_x + TB_W, cfg_mid),
            (mio_x - 40.0, cfg_mid),
            (mio_x - 40.0, mio_y + TB_H / 2.0),
            (mio_x, mio_y + TB_H / 2.0),
        ])
        .build(),
    );
    parts.push(
        text(
            (cfg_x + TB_W + mio_x) / 2.0,
            mio_y + TB_H / 2.0 - 18.0,
            "mio (default)",
        )
        .fill("#555")
        .build(),
    );
    parts.push(
        ortho(&[
            (cfg_x + TB_W, cfg_mid),
            (mio_x - 40.0, cfg_mid),
            (mio_x - 40.0, ring_y + TB_H / 2.0),
            (mio_x, ring_y + TB_H / 2.0),
        ])
        .build(),
    );
    parts.push(
        text(
            (cfg_x + TB_W + mio_x) / 2.0,
            ring_y + TB_H / 2.0 + 18.0,
            "ringline (Linux)",
        )
        .fill("#555")
        .build(),
    );
    parts.push(
        ortho(&[
            (mio_x + TB_W, mio_y + TB_H / 2.0),
            (dispatch_x, mio_y + TB_H / 2.0),
        ])
        .build(),
    );
    parts.push(
        ortho(&[
            (dispatch_x + 50.0, mio_y + TB_H / 2.0),
            (worker_x, mio_y + TB_H / 2.0),
        ])
        .build(),
    );
    parts.push(
        ortho(&[
            (mio_x + TB_W, ring_y + TB_H / 2.0),
            (dispatch_x, ring_y + TB_H / 2.0),
        ])
        .build(),
    );
    parts.push(
        ortho(&[
            (dispatch_x + 50.0, ring_y + TB_H / 2.0),
            (worker_x, ring_y + TB_H / 2.0),
        ])
        .build(),
    );
    parts.push(
        ortho(&[(cfg_x + TB_W, control_mid), (mio_x, control_mid)])
            .signal()
            .build(),
    );
    parts.push(
        text(
            (cfg_x + TB_W + mio_x) / 2.0,
            control_mid - 18.0,
            "OS signals",
        )
        .fill("#777")
        .build(),
    );
    parts.push(
        ortho(&[
            (mio_x + TB_W / 2.0, control_y),
            (mio_x + TB_W / 2.0, control_y - 40.0),
            (broadcast_q_x + 25.0, control_y - 40.0),
            (broadcast_q_x + 25.0, broadcast_q_y + 22.0),
        ])
        .signal()
        .build(),
    );
    parts.push(
        ortho(&[
            (broadcast_q_x, broadcast_q_mid),
            (mio_x + TB_W / 2.0, broadcast_q_mid),
            (mio_x + TB_W / 2.0, mio_y + TB_H),
        ])
        .signal()
        .build(),
    );
    parts.push(
        ortho(&[
            (broadcast_q_x + 50.0, broadcast_q_mid),
            (worker_x + TB_W / 2.0, broadcast_q_mid),
            (worker_x + TB_W / 2.0, mio_y + TB_H),
        ])
        .signal()
        .build(),
    );
    parts.push(
        ortho(&[(mio_x + TB_W, control_mid), (dispatch_x, control_mid)])
            .signal()
            .build(),
    );
    parts.push(
        ortho(&[(dispatch_x + 50.0, control_mid), (worker_x, control_mid)])
            .signal()
            .build(),
    );
    let report_y = control_y + TB_H + 28.0;
    parts.push(
        ortho(&[
            (worker_x + control_w / 2.0, control_y + TB_H),
            (worker_x + control_w / 2.0, report_y),
            (mio_x + TB_W / 2.0, report_y),
            (mio_x + TB_W / 2.0, control_y + TB_H),
        ])
        .signal()
        .build(),
    );
    parts.push(
        text(
            (mio_x + TB_W / 2.0 + worker_x + control_w / 2.0) / 2.0,
            report_y + 18.0,
            "runtime termination report",
        )
        .fill("#777")
        .build(),
    );
    (parts, h)
}

fn proxy_panel(y0: f64, title: &str, rows: &[(&str, &str)]) -> (Vec<String>, f64) {
    let mut parts = Vec::new();
    let h = 792.0;
    parts.push(
        rect(X0, y0, PANEL_W, h, PANEL_FILL)
            .stroke(PANEL_BORDER)
            .sw(2.0)
            .build(),
    );
    margin_block(&mut parts, X0 + PANEL_W + 130.0, y0 + h / 2.0, title, rows);

    let row_a = y0 + 80.0;
    let mid_a = row_a + TB_H / 2.0;

    let cl_x = X0 + 26.0;
    ext_box(&mut parts, cl_x, row_a, "clients");

    let li_x = cl_x + EXT_W + gap_for("accept").max(TB_W + GAP - EXT_W);
    thread_box(
        &mut parts,
        li_x,
        row_a,
        "pelikan_listener",
        Some(":12321"),
        &[],
        false,
    );
    parts.push(
        ortho(&[(cl_x + EXT_W, mid_a), (li_x, mid_a)])
            .network()
            .build(),
    );
    parts.push(
        text((cl_x + EXT_W + li_x) / 2.0, mid_a - 15.0, "accept")
            .fill("#555")
            .build(),
    );

    let q_w = 50.0;
    let qg = queue_gap("sessions");
    let q_x = li_x + TB_W + qg;
    queue_glyph(&mut parts, q_x, mid_a - 11.0, q_w, 22.0, "sessions");
    parts.push(ortho(&[(li_x + TB_W, mid_a), (q_x, mid_a)]).build());

    let fe_x = q_x + q_w + qg;
    let fe0_y = row_a;
    let fe1_y = worker_column(
        &mut parts,
        fe_x,
        fe0_y,
        ("pelikan_fe_0", "pelikan_fe_n-1"),
        &[CHIP_PROTOCOL],
    );
    parts.push(
        ortho(&[
            (q_x + q_w, mid_a),
            (fe_x - 20.0, mid_a),
            (fe_x - 20.0, fe0_y + TB_H / 2.0),
            (fe_x, fe0_y + TB_H / 2.0),
        ])
        .build(),
    );
    parts.push(
        ortho(&[
            (q_x + q_w, mid_a),
            (fe_x - 20.0, mid_a),
            (fe_x - 20.0, fe1_y + TB_H / 2.0),
            (fe_x, fe1_y + TB_H / 2.0),
        ])
        .build(),
    );

    let top_y = y0 + 40.0;
    parts.push(
        ortho(&[
            (cl_x + EXT_W / 2.0, row_a + (TB_H - EXT_H) / 2.0),
            (cl_x + EXT_W / 2.0, top_y),
            (fe_x + TB_W / 2.0, top_y),
            (fe_x + TB_W / 2.0, fe0_y),
        ])
        .both()
        .network()
        .build(),
    );
    parts.push(
        text(
            (cl_x + fe_x + TB_W) / 2.0,
            top_y - 15.0,
            "requests / responses (wire)",
        )
        .fill("#555")
        .build(),
    );

    let dq_w = 50.0;
    let dqg = queue_gap("requests / responses (object)");
    let dq_x = fe_x + TB_W + dqg;
    let grp_mid = (fe0_y + fe1_y + TB_H) / 2.0;
    queue_glyph(
        &mut parts,
        dq_x,
        grp_mid - 9.0,
        dq_w,
        18.0,
        "requests / responses (object)",
    );
    parts.push(
        ortho(&[
            (fe_x + TB_W, fe0_y + TB_H / 2.0),
            (dq_x - ELBOW, fe0_y + TB_H / 2.0),
            (dq_x - ELBOW, grp_mid),
            (dq_x, grp_mid),
        ])
        .both()
        .build(),
    );
    parts.push(
        ortho(&[
            (fe_x + TB_W, fe1_y + TB_H / 2.0),
            (dq_x - ELBOW, fe1_y + TB_H / 2.0),
            (dq_x - ELBOW, grp_mid),
            (dq_x, grp_mid),
        ])
        .both()
        .build(),
    );

    let be_x = dq_x + dq_w + dqg;
    let be1_y = worker_column(
        &mut parts,
        be_x,
        fe0_y,
        ("pelikan_be_0", "pelikan_be_n-1"),
        &[CHIP_PROTOCOL],
    );
    parts.push(
        ortho(&[
            (dq_x + dq_w, grp_mid),
            (dq_x + dq_w + ELBOW, grp_mid),
            (dq_x + dq_w + ELBOW, fe0_y + TB_H / 2.0),
            (be_x, fe0_y + TB_H / 2.0),
        ])
        .both()
        .build(),
    );
    parts.push(
        ortho(&[
            (dq_x + dq_w, grp_mid),
            (dq_x + dq_w + ELBOW, grp_mid),
            (dq_x + dq_w + ELBOW, fe1_y + TB_H / 2.0),
            (be_x, fe1_y + TB_H / 2.0),
        ])
        .both()
        .build(),
    );

    let sv_x = be_x + TB_W + gap_for("connect");
    ext_box(&mut parts, sv_x, row_a, "servers");
    parts.push(
        ortho(&[(be_x + TB_W, mid_a), (sv_x, mid_a)])
            .both()
            .network()
            .build(),
    );
    parts.push(
        text((be_x + TB_W + sv_x) / 2.0, mid_a - 15.0, "connect")
            .fill("#555")
            .build(),
    );

    // control plane: signal left of admin, admin aligned under listener
    let row_b = y0 + h - 212.0;
    let sg_x = X0 + 26.0;
    thread_box(
        &mut parts,
        sg_x,
        row_b,
        "pelikan_signal",
        Some("SIGINT/TERM/QUIT"),
        &[],
        true,
    );
    thread_box(
        &mut parts,
        li_x,
        row_b,
        "pelikan_admin",
        Some(":9999"),
        &[CHIP_PROTOCOL_ADMIN],
        false,
    );
    let mid_b = row_b + TB_H / 2.0;
    parts.push(
        ortho(&[(sg_x + TB_W, mid_b), (li_x, mid_b)])
            .signal()
            .build(),
    );
    parts.push(
        ortho(&[(li_x + 48.0, row_b), (li_x + 48.0, row_a + TB_H)])
            .signal()
            .build(),
    );
    parts.push(
        text(li_x + 16.0, (row_b + row_a + TB_H) / 2.0, "signals")
            .fill("#777")
            .build(),
    );
    parts.push(
        ortho(&[
            (li_x + TB_W, mid_b),
            (fe_x + TB_W / 2.0, mid_b),
            (fe_x + TB_W / 2.0, fe1_y + TB_H),
        ])
        .signal()
        .build(),
    );
    parts.push(
        ortho(&[
            (li_x + TB_W, mid_b),
            (be_x + TB_W / 2.0, mid_b),
            (be_x + TB_W / 2.0, be1_y + TB_H),
        ])
        .signal()
        .build(),
    );
    (parts, h)
}

pub fn generate() {
    verify(CLAIMS, NEG_CLAIMS);
    let server_rows = [
        ("segcache", "memcache"),
        ("rds", "resp"),
        ("pingserver", "ping"),
    ];
    let proxy_rows = [("pingproxy", "ping")];

    let mut parts = vec![ARROW_DEFS.to_string()];
    let mut y = 24.0;
    let (fork, fork_h) = backend_choice_panel(y);
    parts.extend(fork);
    y += fork_h + 20.0;
    let (p1, h1) = server_panel(y, "common Arc-shared storage", &server_rows);
    parts.extend(p1);
    y += h1 + 20.0;
    let (p2, h2) = proxy_panel(y, "proxy", &proxy_rows);
    parts.extend(p2);

    let (w, h) = (24.0 + PANEL_W + 260.0 + 24.0, y + h2 + 24.0);
    fs::write(OUT, svg_document(w, h, "cargo xtask diagrams", &parts)).unwrap();
    println!("generated: {OUT}");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backend_panel_shows_real_ringline_queue_workers_and_control_thread() {
        let (parts, _) = backend_choice_panel(0.0);
        let svg = parts.concat();

        assert!(svg.contains("accepted fd queue / wake"));
        assert!(svg.contains("pelikan_ringline_control"));
        assert!(svg.contains("schedules async tasks"));
        assert!(!svg.contains("runtime task dispatch"));
    }

    #[test]
    fn backend_panel_inventories_server_signal_thread_and_mio_broadcast() {
        let (parts, _) = backend_choice_panel(0.0);
        let svg = parts.concat();

        assert!(svg.contains("pelikan_signal"));
        assert!(svg.contains("OS signals"));
        assert!(svg.contains("Mio broadcast queue / wake"));
    }

    #[test]
    fn shared_storage_panel_is_backend_neutral() {
        let rows = [("segcache", "memcache")];
        let (parts, _) = server_panel(0.0, "common Arc-shared storage", &rows);
        let svg = parts.concat();

        assert!(svg.contains("execution context 0"));
        assert!(svg.contains("one Arc-shared engine"));
        for backend_specific in [
            "pelikan_listener",
            "pelikan_work",
            "ringline-acceptor",
            "ringline-worker",
        ] {
            assert!(!svg.contains(backend_specific));
        }
    }
}
