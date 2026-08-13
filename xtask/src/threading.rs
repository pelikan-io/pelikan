//! The threading architecture chart: the runtime thread model per binary in
//! three stacked panels (single worker / multiple workers / proxy). Literal
//! thread names in monospace matching `top -H`; build-module chips inside
//! each thread bridge this chart to the architecture chart; heavier edges
//! carry bytes across the process boundary (wire) vs internal queues
//! (object). The thread and queue inventory is asserted against the sources
//! at generation time.

use crate::claims::{verify, Claim};
use crate::svg::*;
use std::fs;

const OUT: &str = "docs/diagrams/threading.svg";

const CLAIMS: &[Claim] = &[
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
        pattern: r#"name\(format!\("\{THREAD_PREFIX\}_work"\)\)"#,
        what: "single worker thread spawn",
    },
    Claim {
        path: "src/core/server/src/workers/mod.rs",
        pattern: r#"name\(format!\("\{THREAD_PREFIX\}_work_\{id\}"\)\)"#,
        what: "multi worker thread spawn",
    },
    Claim {
        path: "src/core/server/src/workers/mod.rs",
        pattern: r#"name\(format!\("\{THREAD_PREFIX\}_storage"\)\)"#,
        what: "storage thread spawn",
    },
    Claim {
        path: "src/core/server/src/process.rs",
        pattern: r#"name\(format!\("\{THREAD_PREFIX\}_signal"\)\)"#,
        what: "signal handler thread spawn",
    },
    Claim {
        path: "src/core/server/src/process.rs",
        pattern: r"// queues for the `Admin` to send `Signal`s to all sibling threads",
        what: "admin signal broadcast queues",
    },
    Claim {
        path: "src/core/server/src/process.rs",
        pattern: r"// queues for the `Listener` to send `Session`s to the worker threads",
        what: "listener->worker session queues",
    },
    Claim {
        path: "src/core/server/src/workers/mod.rs",
        pattern: r"Queues::new\(worker_wakers, storage_wakers",
        what: "worker<->storage data queues",
    },
    Claim {
        path: "src/core/server/src/process.rs",
        pattern: r"thread_wakers\.extend_from_slice\(&self\.workers\.wakers\(\)\)",
        what: "signal queues include all worker-side wakers (incl. storage)",
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
const NEG_CLAIMS: &[Claim] = &[];

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
    parts.push(rect(x, y, TB_W, TB_H, "#FFFFFF").rx(10.0).build());
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
        text(x + TB_W / 2.0, top + name_h / 2.0, name)
            .size(TS.h2)
            .bold()
            .mono()
            .build(),
    );
    if let Some(sub) = sub {
        let mut t = text(x + TB_W / 2.0, top + name_h + sub_h / 2.0, sub).fill("#333");
        if external {
            t = t.italic();
        }
        parts.push(t.build());
    }
    if !chips.is_empty() {
        let bw = TB_W - 40.0;
        let mut cy = top + name_h + sub_h + 18.0;
        for (label, cfill) in chips {
            parts.push(rect(x + 20.0, cy, bw, bh, cfill).sw(1.0).build());
            parts.push(text(x + TB_W / 2.0, cy + bh / 2.0, label).build());
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

fn server_panel(y0: f64, title: &str, rows: &[(&str, &str)], multi: bool) -> (Vec<String>, f64) {
    let mut parts = Vec::new();
    let h = if multi { 792.0 } else { 560.0 };
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

    let wk_x = q_x + q_w + qg;
    let top_y = y0 + 40.0;
    let row_b = y0 + h - 212.0;

    let (wk_bottom, st): (f64, Option<(f64, f64)>) = if !multi {
        thread_box(
            &mut parts,
            wk_x,
            row_a,
            "pelikan_work",
            None,
            &[CHIP_PROTOCOL, CHIP_ENTRYSTORE, CHIP_SEGCACHE],
            false,
        );
        parts.push(ortho(&[(q_x + q_w, mid_a), (wk_x, mid_a)]).build());
        (row_a + TB_H, None)
    } else {
        let wk0_y = row_a;
        let wk1_y = worker_column(
            &mut parts,
            wk_x,
            wk0_y,
            ("pelikan_work_0", "pelikan_work_n-1"),
            &[CHIP_PROTOCOL],
        );
        parts.push(
            ortho(&[
                (q_x + q_w, mid_a),
                (wk_x - 22.0, mid_a),
                (wk_x - 22.0, wk0_y + TB_H / 2.0),
                (wk_x, wk0_y + TB_H / 2.0),
            ])
            .build(),
        );
        parts.push(
            ortho(&[
                (q_x + q_w, mid_a),
                (wk_x - 22.0, mid_a),
                (wk_x - 22.0, wk1_y + TB_H / 2.0),
                (wk_x, wk1_y + TB_H / 2.0),
            ])
            .build(),
        );
        let dq_w = 50.0;
        let dqg = queue_gap("requests / responses (object)");
        let dq_x = wk_x + TB_W + dqg;
        let dq_mid = (wk0_y + wk1_y + TB_H) / 2.0;
        queue_glyph(
            &mut parts,
            dq_x,
            dq_mid - 9.0,
            dq_w,
            18.0,
            "requests / responses (object)",
        );
        parts.push(
            ortho(&[
                (wk_x + TB_W, wk0_y + TB_H / 2.0),
                (dq_x - ELBOW, wk0_y + TB_H / 2.0),
                (dq_x - ELBOW, dq_mid),
                (dq_x, dq_mid),
            ])
            .both()
            .build(),
        );
        parts.push(
            ortho(&[
                (wk_x + TB_W, wk1_y + TB_H / 2.0),
                (dq_x - ELBOW, wk1_y + TB_H / 2.0),
                (dq_x - ELBOW, dq_mid),
                (dq_x, dq_mid),
            ])
            .both()
            .build(),
        );
        let st_x = dq_x + dq_w + dqg;
        let st_y = dq_mid - TB_H / 2.0;
        thread_box(
            &mut parts,
            st_x,
            st_y,
            "pelikan_storage",
            None,
            &[CHIP_ENTRYSTORE, CHIP_SEGCACHE],
            false,
        );
        parts.push(
            ortho(&[(dq_x + dq_w, dq_mid), (st_x, dq_mid)])
                .both()
                .build(),
        );
        (wk1_y + TB_H, Some((st_x, st_y)))
    };

    // requests/responses between clients and workers, over the top
    parts.push(
        ortho(&[
            (cl_x + EXT_W / 2.0, row_a + (TB_H - EXT_H) / 2.0),
            (cl_x + EXT_W / 2.0, top_y),
            (wk_x + TB_W / 2.0, top_y),
            (wk_x + TB_W / 2.0, row_a),
        ])
        .both()
        .network()
        .build(),
    );
    parts.push(
        text(
            (cl_x + wk_x + TB_W) / 2.0,
            top_y - 15.0,
            "requests / responses (wire)",
        )
        .fill("#555")
        .build(),
    );

    // control plane: signal left of admin, admin aligned under listener
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
            (wk_x + TB_W / 2.0, mid_b),
            (wk_x + TB_W / 2.0, wk_bottom),
        ])
        .signal()
        .build(),
    );
    if let Some((st_x, st_y)) = st {
        parts.push(
            ortho(&[
                (li_x + TB_W, mid_b),
                (st_x + TB_W / 2.0, mid_b),
                (st_x + TB_W / 2.0, st_y + TB_H),
            ])
            .signal()
            .build(),
        );
    }
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
    let (p1, h1) = server_panel(y, "single worker", &server_rows, false);
    parts.extend(p1);
    y += h1 + 20.0;
    let (p2, h2) = server_panel(y, "multiple workers", &server_rows, true);
    parts.extend(p2);
    y += h2 + 20.0;
    let (p3, h3) = proxy_panel(y, "proxy", &proxy_rows);
    parts.extend(p3);

    let (w, h) = (24.0 + PANEL_W + 260.0 + 24.0, y + h3 + 24.0);
    fs::write(OUT, svg_document(w, h, "cargo xtask diagrams", &parts)).unwrap();
    println!("generated: {OUT}");
}
