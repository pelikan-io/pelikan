//! The "life of a request" chart: one request traced through the threads of
//! each binary as numbered stages on thread swimlanes, using the code's own
//! verbs (receive/execute/send/flush). One uniform gap between stage columns
//! whether or not the path switches lanes (same-lane labels float above the
//! stage line; the gap is sized for the elbow-and-queue run of a crossing).
//! Stage claims are asserted against the event-loop sources, and panel
//! content is bounds-checked, at generation time. Control plane is
//! intentionally out of scope.

use crate::claims::{verify, Claim};
use crate::svg::*;
use regex::Regex;
use std::fs;

const OUT: &str = "docs/diagrams/dataflow.svg";

const CLAIMS: &[Claim] = &[
    Claim {
        path: "src/core/server/src/workers/single.rs",
        pattern: r"session\.receive\(\)",
        what: "single worker: session.receive parses a request",
    },
    Claim {
        path: "src/core/server/src/workers/single.rs",
        pattern: r"self\.storage\.execute\(&request\)",
        what: "single worker executes on thread-local storage",
    },
    Claim {
        path: "src/core/server/src/workers/single.rs",
        pattern: r"session\.send\(response\)",
        what: "single worker composes the response",
    },
    Claim {
        path: "src/core/server/src/workers/single.rs",
        pattern: r"session\.flush\(\)",
        what: "single worker flushes to the socket",
    },
    Claim {
        path: "src/core/server/src/workers/multi.rs",
        pattern: r"try_send_to\(0, \(request, token\)\)",
        what: "multi worker enqueues the parsed request to storage",
    },
    Claim {
        path: "src/core/server/src/workers/storage.rs",
        pattern: r"self\.storage\.execute\(&request\)",
        what: "storage thread executes requests",
    },
    Claim {
        path: "src/core/server/src/workers/storage.rs",
        pattern: r"try_send_to\(sender, message\)",
        what: "storage thread returns responses to the sending worker",
    },
    Claim {
        path: "src/core/server/src/workers/multi.rs",
        pattern: r"session\.send\(response\)",
        what: "multi worker composes the returned response",
    },
    Claim {
        path: "src/core/proxy/src/frontend.rs",
        pattern: r"BackendRequest::from\(request\)",
        what: "proxy frontend forwards the parsed request to a backend",
    },
    Claim {
        path: "src/core/proxy/src/backend.rs",
        pattern: r"try_send_to\(0, \(request, response, fe_token\)\)",
        what: "proxy backend returns the upstream response to the frontend",
    },
    Claim {
        path: "src/core/proxy/src/backend.rs",
        pattern: r"session\.receive\(\)",
        what: "proxy backend parses the upstream response",
    },
];

type Chip = (&'static str, &'static str);
const CHIP_SESSION: Chip = ("session", FILL_CORE);
const CHIP_PROTOCOL: Chip = ("protocol-*", FILL_PROTOCOL);
const CHIP_ENTRYSTORE: Chip = ("entrystore", FILL_STORAGE);
const CHIP_SEGCACHE: Chip = ("segcache", FILL_STORAGE);

const LANE_LINE: &str = "#DDDDDD";

// geometry: uniform chips size the stage; one uniform inter-column gap
const CHIP_W: f64 = 84.0;
const CHIP_H: f64 = 24.0;
const ST_W: f64 = 184.0;
const ST_H: f64 = 72.0;
const GAP: f64 = 80.0;
const LANE_H: f64 = 116.0;
const LANE_LABEL_W: f64 = 175.0;
const PANEL_W: f64 = 1740.0;
const X0: f64 = 24.0;

fn stage(parts: &mut Vec<String>, x: f64, y: f64, num: u32, name: &str, chips: &[Chip]) {
    parts.push(rect(x, y, ST_W, ST_H, "#FFFFFF").rx(10.0).build());
    parts.push(format!(
        "<circle cx=\"{:.0}\" cy=\"{:.0}\" r=\"11\" fill=\"none\" \
         stroke=\"#4D4D4D\" stroke-width=\"1.2\"/>",
        x + 18.0,
        y + 17.0
    ));
    parts.push(text(x + 18.0, y + 17.0, &num.to_string()).bold().build());
    parts.push(
        text(x + ST_W / 2.0 + 8.0, y + 17.0, name)
            .size(17)
            .bold()
            .build(),
    );
    let group_w = CHIP_W * chips.len() as f64 + 6.0 * (chips.len() - 1) as f64;
    let mut cx = x + (ST_W - group_w) / 2.0;
    let cy = y + ST_H - 32.0;
    for (label, cfill) in chips {
        parts.push(rect(cx, cy, CHIP_W, CHIP_H, cfill).sw(1.0).build());
        parts.push(text(cx + CHIP_W / 2.0, cy + CHIP_H / 2.0, label).build());
        cx += CHIP_W + 6.0;
    }
}

fn vqueue(parts: &mut Vec<String>, x: f64, y_mid: f64, label: &str) {
    let (h, w, n) = (60.0, 16.0, 5);
    let ch = h / n as f64;
    for i in 0..n {
        parts.push(
            rect(
                x - w / 2.0,
                y_mid - h / 2.0 + i as f64 * ch,
                w,
                ch,
                QUEUE_FILL,
            )
            .sw(1.0)
            .build(),
        );
    }
    parts.push(
        text(x + w / 2.0 + 8.0, y_mid, label)
            .fill("#555")
            .start()
            .build(),
    );
}

fn lane_header(parts: &mut Vec<String>, y: f64, name: &str, external: bool) {
    if !external {
        parts.push(
            text(X0 + 18.0, y + LANE_H / 2.0, name)
                .size(16)
                .bold()
                .mono()
                .start()
                .build(),
        );
    }
    parts.push(format!(
        "<line x1=\"{:.0}\" y1=\"{:.0}\" x2=\"{:.0}\" y2=\"{:.0}\" \
         stroke=\"{}\" stroke-width=\"1\"/>",
        X0 + 12.0,
        y + LANE_H,
        X0 + PANEL_W - 12.0,
        y + LANE_H,
        LANE_LINE
    ));
}

fn columns(n: usize) -> Vec<f64> {
    let x0 = X0 + LANE_LABEL_W + 30.0;
    (0..n).map(|i| x0 + i as f64 * (ST_W + GAP)).collect()
}

fn margin_block(parts: &mut Vec<String>, cx: f64, cy: f64, title: &str, rows: &[(&str, &str)]) {
    let (row_h, title_h, gap) = (20.0, 26.0, 8.0);
    let block_h = title_h + gap + rows.len() as f64 * row_h;
    let ty = cy - block_h / 2.0 + title_h / 2.0;
    parts.push(text(cx, ty, title).size(20).bold().build());
    let mut ry = ty + title_h / 2.0 + gap + row_h / 2.0;
    for (binary, proto) in rows {
        parts.push(
            text(cx - 6.0, ry, binary)
                .size(13)
                .fill("#555")
                .end()
                .build(),
        );
        parts.push(text(cx, ry, ":").size(13).fill("#555").build());
        parts.push(
            text(cx + 8.0, ry, proto)
                .size(13)
                .fill("#555")
                .start()
                .build(),
        );
        ry += row_h;
    }
}

enum Kind {
    Single,
    Multi,
    Proxy,
}

fn panel(y0: f64, title: &str, rows: &[(&str, &str)], kind: Kind) -> (Vec<String>, f64) {
    let mut parts: Vec<String> = Vec::new();
    let lanes: Vec<&str> = match kind {
        Kind::Single => vec!["clients", "pelikan_work"],
        Kind::Multi => vec!["clients", "pelikan_work_i", "pelikan_storage"],
        Kind::Proxy => vec!["clients", "pelikan_fe_i", "pelikan_be_i", "servers"],
    };
    let h = 40.0 + LANE_H * lanes.len() as f64 + 24.0;
    parts.push(
        rect(X0, y0, PANEL_W, h, PANEL_FILL)
            .stroke(PANEL_BORDER)
            .sw(2.0)
            .build(),
    );
    margin_block(&mut parts, X0 + PANEL_W + 100.0, y0 + h / 2.0, title, rows);
    let n_margin = parts.len();

    let mut lane_y: Vec<(&str, f64)> = Vec::new();
    let mut y = y0 + 28.0;
    for nm in &lanes {
        lane_header(&mut parts, y, nm, *nm == "clients" || *nm == "servers");
        lane_y.push((nm, y));
        y += LANE_H;
    }
    let ly = |ln: &str| lane_y.iter().find(|(n, _)| *n == ln).unwrap().1;
    let st_y = |ln: &str| ly(ln) + (LANE_H - ST_H) / 2.0;
    let st_mid = |ln: &str| ly(ln) + LANE_H / 2.0;

    let cl_x = X0 + LANE_LABEL_W + 30.0 - 30.0 - 90.0;
    let cl_y = st_mid("clients") - 28.0;
    parts.push(rect(cl_x, cl_y, 90.0, 56.0, FILL_EXTERNAL).dashed().build());
    parts.push(
        text(cl_x + 45.0, cl_y + 28.0, "clients")
            .size(15)
            .italic()
            .build(),
    );

    // shared fragments -----------------------------------------------------
    let wire_in = |parts: &mut Vec<String>, xs: &[f64], ln: &str| {
        parts.push(
            ortho(&[
                (cl_x + 90.0, st_mid("clients")),
                (xs[0] + ST_W / 2.0, st_mid("clients")),
                (xs[0] + ST_W / 2.0, st_y(ln)),
            ])
            .network()
            .build(),
        );
        parts.push(
            text(
                (cl_x + 90.0 + xs[0] + ST_W / 2.0) / 2.0,
                st_mid("clients") - 12.0,
                "request (wire)",
            )
            .fill("#555")
            .build(),
        );
    };
    let wire_out = |parts: &mut Vec<String>, xs: &[f64], ln: &str| {
        let x = xs[xs.len() - 1] + ST_W / 2.0;
        parts.push(
            ortho(&[
                (x, st_y(ln)),
                (x, st_mid("clients")),
                (cl_x + 90.0, st_mid("clients")),
            ])
            .network()
            .build(),
        );
        let half = label_w("response (wire)") / 2.0;
        let lx = (x + 90.0).min(X0 + PANEL_W - half);
        parts.push(
            text(lx, st_mid("clients") - 12.0, "response (wire)")
                .fill("#555")
                .build(),
        );
    };
    let gap_label = |parts: &mut Vec<String>, xs: &[f64], i: usize, label: &str, ln: &str| {
        let cx = xs[i] + ST_W + (xs[i + 1] - xs[i] - ST_W) / 2.0;
        parts.push(text(cx, st_y(ln) - 12.0, label).fill("#555").build());
    };
    let crossing =
        |parts: &mut Vec<String>, xs: &[f64], i: usize, from: &str, to: &str, label: &str| {
            let gx = xs[i] + ST_W + GAP / 2.0;
            parts.push(
                ortho(&[
                    (xs[i] + ST_W, st_mid(from)),
                    (gx, st_mid(from)),
                    (gx, st_mid(to)),
                    (xs[i + 1], st_mid(to)),
                ])
                .build(),
            );
            vqueue(parts, gx, (st_mid(from) + st_mid(to)) / 2.0, label);
        };
    let straight = |parts: &mut Vec<String>, xs: &[f64], i: usize, ln: &str| {
        parts.push(ortho(&[(xs[i] + ST_W, st_mid(ln)), (xs[i + 1], st_mid(ln))]).build());
    };

    match kind {
        Kind::Single => {
            let wl = "pelikan_work";
            let xs = columns(4);
            stage(
                &mut parts,
                xs[0],
                st_y(wl),
                1,
                "receive",
                &[CHIP_SESSION, CHIP_PROTOCOL],
            );
            stage(
                &mut parts,
                xs[1],
                st_y(wl),
                2,
                "execute",
                &[CHIP_ENTRYSTORE, CHIP_SEGCACHE],
            );
            stage(
                &mut parts,
                xs[2],
                st_y(wl),
                3,
                "send",
                &[CHIP_SESSION, CHIP_PROTOCOL],
            );
            stage(&mut parts, xs[3], st_y(wl), 4, "flush", &[CHIP_SESSION]);
            wire_in(&mut parts, &xs, wl);
            for i in 0..3 {
                straight(&mut parts, &xs, i, wl);
            }
            gap_label(&mut parts, &xs, 0, "request (object)", wl);
            gap_label(&mut parts, &xs, 1, "response (object)", wl);
            wire_out(&mut parts, &xs, wl);
        }
        Kind::Multi => {
            let (wl, sl) = ("pelikan_work_i", "pelikan_storage");
            let xs = columns(4);
            stage(
                &mut parts,
                xs[0],
                st_y(wl),
                1,
                "receive",
                &[CHIP_SESSION, CHIP_PROTOCOL],
            );
            stage(
                &mut parts,
                xs[1],
                st_y(sl),
                2,
                "execute",
                &[CHIP_ENTRYSTORE, CHIP_SEGCACHE],
            );
            stage(
                &mut parts,
                xs[2],
                st_y(wl),
                3,
                "send",
                &[CHIP_SESSION, CHIP_PROTOCOL],
            );
            stage(&mut parts, xs[3], st_y(wl), 4, "flush", &[CHIP_SESSION]);
            wire_in(&mut parts, &xs, wl);
            crossing(&mut parts, &xs, 0, wl, sl, "request (object)");
            crossing(&mut parts, &xs, 1, sl, wl, "response (object)");
            straight(&mut parts, &xs, 2, wl);
            wire_out(&mut parts, &xs, wl);
        }
        Kind::Proxy => {
            let (fl, bl) = ("pelikan_fe_i", "pelikan_be_i");
            let xs = columns(6);
            stage(
                &mut parts,
                xs[0],
                st_y(fl),
                1,
                "receive",
                &[CHIP_SESSION, CHIP_PROTOCOL],
            );
            stage(
                &mut parts,
                xs[1],
                st_y(bl),
                2,
                "send",
                &[CHIP_SESSION, CHIP_PROTOCOL],
            );
            stage(&mut parts, xs[2], st_y(bl), 3, "flush", &[CHIP_SESSION]);
            stage(
                &mut parts,
                xs[3],
                st_y(bl),
                4,
                "receive",
                &[CHIP_SESSION, CHIP_PROTOCOL],
            );
            stage(
                &mut parts,
                xs[4],
                st_y(fl),
                5,
                "send",
                &[CHIP_SESSION, CHIP_PROTOCOL],
            );
            stage(&mut parts, xs[5], st_y(fl), 6, "flush", &[CHIP_SESSION]);
            wire_in(&mut parts, &xs, fl);
            crossing(&mut parts, &xs, 0, fl, bl, "request (object)");
            straight(&mut parts, &xs, 1, bl);
            // upstream round trip through the servers lane
            let sv_mid = st_mid("servers");
            let sx = xs[2] + ST_W + GAP / 2.0;
            parts.push(
                rect(sx - 45.0, sv_mid - 28.0, 90.0, 56.0, FILL_EXTERNAL)
                    .dashed()
                    .build(),
            );
            parts.push(text(sx, sv_mid, "servers").size(15).italic().build());
            parts.push(
                ortho(&[
                    (xs[2] + ST_W / 2.0, st_y(bl) + ST_H),
                    (xs[2] + ST_W / 2.0, sv_mid),
                    (sx - 45.0, sv_mid),
                ])
                .network()
                .build(),
            );
            parts.push(
                text(xs[2] + ST_W / 2.0 - 12.0, sv_mid - 34.0, "request (wire)")
                    .fill("#555")
                    .end()
                    .build(),
            );
            parts.push(
                ortho(&[
                    (sx + 45.0, sv_mid),
                    (xs[3] + ST_W / 2.0, sv_mid),
                    (xs[3] + ST_W / 2.0, st_y(bl) + ST_H),
                ])
                .network()
                .build(),
            );
            parts.push(
                text(xs[3] + ST_W / 2.0 + 12.0, sv_mid - 34.0, "response (wire)")
                    .fill("#555")
                    .start()
                    .build(),
            );
            crossing(&mut parts, &xs, 3, bl, fl, "response (object)");
            straight(&mut parts, &xs, 4, fl);
            wire_out(&mut parts, &xs, fl);
        }
    }

    // bounds check: panel content (after the frame and the margin block)
    // stays inside the panel
    let re = Regex::new(r#"x2?="(-?\d+)""#).unwrap();
    for part in &parts[n_margin..] {
        for c in re.captures_iter(part) {
            let x: f64 = c[1].parse().unwrap();
            if x > X0 + PANEL_W {
                eprintln!(
                    "ERROR: element beyond panel bounds: {}",
                    &part[..90.min(part.len())]
                );
                std::process::exit(1);
            }
        }
    }
    (parts, h)
}

pub fn generate() {
    verify(CLAIMS, &[]);
    let server_rows = [
        ("segcache", "memcache"),
        ("rds", "resp"),
        ("pingserver", "ping"),
    ];
    let proxy_rows = [("pingproxy", "ping")];

    let mut parts = vec![ARROW_DEFS.to_string()];
    let mut y = 24.0;
    let (p1, h1) = panel(y, "single worker", &server_rows, Kind::Single);
    parts.extend(p1);
    y += h1 + 20.0;
    let (p2, h2) = panel(y, "multiple workers", &server_rows, Kind::Multi);
    parts.extend(p2);
    y += h2 + 20.0;
    let (p3, h3) = panel(y, "proxy", &proxy_rows, Kind::Proxy);
    parts.extend(p3);

    let (w, h) = (X0 + PANEL_W + 200.0 + 24.0, y + h3 + 24.0);
    fs::write(OUT, svg_document(w, h, "cargo xtask diagrams", &parts)).unwrap();
    println!("generated: {OUT}");
}
