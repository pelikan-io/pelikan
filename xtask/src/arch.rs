//! The workspace architecture chart: a layer-cake block diagram in the style
//! of the classic Pelikan architecture figure. Three full-width bands
//! (Services / Cache libraries / Runtime libraries) separated by rules, band
//! names in the right margin, composition shown by nesting (each service box
//! contains its protocol, storage-engine, and core bars), rigidly aligned
//! grids, no arrows.
//!
//! Derived from `cargo metadata` — never from a hand-maintained list. Every
//! workspace crate must be classified in LAYER (or listed as tooling) and
//! every external worth showing must appear in EXTERNALS; anything
//! unclassified aborts generation rather than being silently omitted.

use crate::svg::*;

const TS: TypeScale = TYPE_SCALE;

/// Chart-local default: body text at this chart's scale.
fn text(x: f64, y: f64, s: &str) -> crate::svg::Text {
    crate::svg::text(x, y, s).size(TS.body)
}
use cargo_metadata::MetadataCommand;
use regex::Regex;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

const OUT: &str = "docs/diagrams/architecture.svg";

#[derive(Clone, Copy, PartialEq, Eq)]
enum Layer {
    Product,
    Core,
    Protocol,
    Storage,
    Foundation,
}

/// Classification table: crate -> layer. A workspace member missing from
/// this table (and not tooling) is a hard error, so a new crate forces a
/// conscious classification.
const LAYER: &[(&str, Layer)] = &[
    ("pelikan-segcache", Layer::Product),
    ("pelikan-rds", Layer::Product),
    ("pelikan-pingserver", Layer::Product),
    ("pelikan-pingproxy", Layer::Product),
    ("server", Layer::Core),
    ("proxy", Layer::Core),
    ("admin", Layer::Core),
    ("session", Layer::Core),
    ("queues", Layer::Core),
    ("protocol-admin", Layer::Protocol),
    ("protocol-common", Layer::Protocol),
    ("protocol-http", Layer::Protocol),
    ("protocol-memcache", Layer::Protocol),
    ("protocol-ping", Layer::Protocol),
    ("protocol-resp", Layer::Protocol),
    ("entrystore", Layer::Storage),
    ("storage-types", Layer::Storage),
    ("bloom", Layer::Storage),
    ("common", Layer::Foundation),
    ("config", Layer::Foundation),
    ("logger", Layer::Foundation),
    ("pelikan-net", Layer::Foundation),
];

/// Repo tooling, deliberately outside the architecture chart.
const TOOLING: &[&str] = &["xtask"];

/// External (non-workspace) dependencies worth showing, and which crate
/// pulls them in. Curated: the full external graph is ~270 crates.
const EXTERNALS: &[(&str, &[&str])] = &[
    ("segcache", &["entrystore"]),
    ("rustls", &["pelikan-net"]),
    ("metriken", &["common"]),
];

/// External modules: underlined and hyperlinked to their crate location.
const EXTERNAL_LINK: &[(&str, &str)] = &[
    ("segcache", "https://crates.io/crates/segcache"),
    ("metriken", "https://crates.io/crates/metriken"),
    ("rustls", "https://crates.io/crates/rustls"),
];

/// Curated display order (flagship first); validated against LAYER at run
/// time so it cannot drift.
const PRODUCT_ORDER: &[&str] = &[
    "pelikan-segcache",
    "pelikan-rds",
    "pelikan-pingserver",
    "pelikan-pingproxy",
];
const PROTOCOL_ORDER: &[&str] = &["memcache", "resp", "ping", "http", "admin", "common"];

/// Band 3 utility row: foundation crates plus external utilities, judged by
/// their relationship to the stack rather than where they are hosted.
const FOUNDATION_ROW: &[&str] = &[
    "common",
    "config",
    "logger",
    "pelikan-net",
    "metriken",
    "rustls",
];

// layout constants (the SVG_STYLE table of the Python original)
const CELL_H: f64 = 52.0;
const PAD: f64 = 24.0;
const GAP: f64 = 12.0;
const BAND_GAP: f64 = 26.0;
const RULE_H: f64 = 7.0;
const CANVAS_W: f64 = 2280.0;
const MARGIN: f64 = 24.0;
const LABEL_W: f64 = 300.0;

fn layer_of(name: &str) -> Option<Layer> {
    LAYER.iter().find(|(n, _)| *n == name).map(|(_, l)| *l)
}

fn external_link(s: &str) -> Option<&'static str> {
    EXTERNAL_LINK.iter().find(|(n, _)| *n == s).map(|(_, u)| *u)
}

/// Cell/bar text: external modules are underlined (explicitly — some
/// renderers ignore text-decoration) and hyperlinked to their crate page.
fn cell_text(x: f64, y: f64, s: &str, size: u32, weight_bold: bool) -> String {
    let mut t = text(x, y, s).size(size);
    if weight_bold {
        t = t.bold();
    }
    let t = t.build();
    if let Some(url) = external_link(s) {
        let half = s.len() as f64 * size as f64 * 0.27;
        let uy = y + size as f64 * 0.62;
        let line = format!(
            "<line x1=\"{:.0}\" y1=\"{:.0}\" x2=\"{:.0}\" y2=\"{:.0}\" \
             stroke=\"#000\" stroke-width=\"1\"/>",
            x - half,
            uy,
            x + half,
            uy
        );
        format!("<a href=\"{url}\" target=\"_blank\">{t}{line}</a>")
    } else {
        t
    }
}

type Graph = BTreeMap<String, BTreeSet<String>>;

struct Workspace {
    graph: Graph,
    manifest_dirs: BTreeMap<String, PathBuf>,
}

fn workspace_graph() -> Workspace {
    let meta = MetadataCommand::new()
        .no_deps()
        .exec()
        .expect("cargo metadata");
    let mut graph = Graph::new();
    let mut manifest_dirs = BTreeMap::new();
    for pkg in &meta.packages {
        let name = pkg.name.to_string();
        if TOOLING.contains(&name.as_str()) {
            continue;
        }
        if layer_of(&name).is_none() {
            eprintln!("ERROR: workspace crate '{name}' is not classified in LAYER");
            std::process::exit(1);
        }
        let deps: BTreeSet<String> = pkg
            .dependencies
            .iter()
            .filter(|d| d.path.is_some())
            .map(|d| d.name.clone())
            .collect();
        manifest_dirs.insert(
            name.clone(),
            pkg.manifest_path
                .parent()
                .expect("manifest dir")
                .to_path_buf()
                .into_std_path_buf(),
        );
        graph.insert(name, deps);
    }
    for (name, _) in LAYER {
        if !graph.contains_key(*name) {
            eprintln!("ERROR: LAYER classifies crate '{name}' not in the workspace");
            std::process::exit(1);
        }
    }
    for (ext, users) in EXTERNALS {
        for u in *users {
            if !graph.contains_key(*u) {
                eprintln!("ERROR: EXTERNALS entry '{ext}' names unknown crate '{u}'");
                std::process::exit(1);
            }
        }
    }
    Workspace {
        graph,
        manifest_dirs,
    }
}

struct Composition {
    protocols: Vec<String>,
    engine: Option<String>,
    core: Vec<String>,
}

fn rust_files(dir: &Path, out: &mut Vec<PathBuf>) {
    if let Ok(entries) = fs::read_dir(dir) {
        for e in entries.flatten() {
            let p = e.path();
            if p.is_dir() {
                rust_files(&p, out);
            } else if p.extension().is_some_and(|x| x == "rs") {
                out.push(p);
            }
        }
    }
}

/// Derive each product's composition bars from the code: its protocol
/// crate(s), the entrystore engine its source actually wires (grepped from
/// `use entrystore::<Engine>`), and which core (server/proxy) it runs on.
fn product_composition(ws: &Workspace) -> BTreeMap<String, Composition> {
    let re = Regex::new(r"use entrystore::(\w+)").unwrap();
    let mut comp = BTreeMap::new();
    for (name, layer) in LAYER {
        if *layer != Layer::Product {
            continue;
        }
        let deps = &ws.graph[*name];
        let protocols: Vec<String> = deps
            .iter()
            .filter(|d| layer_of(d) == Some(Layer::Protocol))
            .map(|d| d.replace("protocol-", ""))
            .collect();
        let core: Vec<String> = deps
            .iter()
            .filter(|d| layer_of(d) == Some(Layer::Core))
            .cloned()
            .collect();
        let engine = if deps.contains("entrystore") {
            let mut files = Vec::new();
            rust_files(&ws.manifest_dirs[*name], &mut files);
            let mut found = BTreeSet::new();
            for f in files {
                if let Ok(src) = fs::read_to_string(&f) {
                    for c in re.captures_iter(&src) {
                        found.insert(c[1].to_string());
                    }
                }
            }
            if found.is_empty() {
                eprintln!(
                    "ERROR: {name} depends on entrystore but no \
                     `use entrystore::<Engine>` found in its sources"
                );
                std::process::exit(1);
            }
            Some(found.into_iter().collect::<Vec<_>>().join("/"))
        } else {
            None
        };
        comp.insert(
            name.to_string(),
            Composition {
                protocols,
                engine,
                core,
            },
        );
    }
    comp
}

fn engine_label(engine: &str) -> &str {
    match engine {
        "Seg" => "segcache",
        other => other,
    }
}

/// Row-major reading order must be a topological order: everything a cell
/// depends on (within the group) appears strictly later.
fn verify_topo_rows(rows: &[Vec<String>], deps_of: &Graph, context: &str) {
    let order: Vec<&String> = rows.iter().flatten().collect();
    let pos: BTreeMap<&str, usize> = order
        .iter()
        .enumerate()
        .map(|(i, c)| (c.as_str(), i))
        .collect();
    for c in &order {
        if let Some(deps) = deps_of.get(*c) {
            for d in deps {
                if let Some(dp) = pos.get(d.as_str()) {
                    if *dp <= pos[c.as_str()] {
                        eprintln!("ERROR: {context}: '{c}' reads after its dependency '{d}'");
                        std::process::exit(1);
                    }
                }
            }
        }
    }
}

/// Dependency map restricted to `members`, with external-crate edges from
/// EXTERNALS (user -> external).
fn extended_subgraph(graph: &Graph, members: &[&str]) -> Graph {
    let member_set: BTreeSet<&str> = members.iter().copied().collect();
    let mut deps = Graph::new();
    for m in members {
        let d: BTreeSet<String> = graph
            .get(*m)
            .map(|ds| {
                ds.iter()
                    .filter(|d| member_set.contains(d.as_str()))
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();
        deps.insert(m.to_string(), d);
    }
    for (ext, users) in EXTERNALS {
        if member_set.contains(ext) {
            for u in *users {
                if member_set.contains(u) {
                    deps.get_mut(*u).unwrap().insert(ext.to_string());
                }
            }
        }
    }
    deps
}

/// Topological levels of the dependency graph restricted to one block,
/// highest level (most dependent) first.
fn intra_block_levels(graph: &Graph, members: &[&str]) -> Vec<Vec<String>> {
    let member_set: BTreeSet<&str> = members.iter().copied().collect();
    fn level<'a>(
        n: &'a str,
        graph: &'a Graph,
        member_set: &BTreeSet<&str>,
        memo: &mut BTreeMap<&'a str, usize>,
    ) -> usize {
        if let Some(v) = memo.get(n) {
            return *v;
        }
        let v = 1 + graph
            .get(n)
            .map(|deps| {
                deps.iter()
                    .filter(|d| member_set.contains(d.as_str()))
                    .map(|d| level(d.as_str(), graph, member_set, memo))
                    .max()
                    .unwrap_or(0)
            })
            .unwrap_or(0);
        memo.insert(n, v);
        v
    }
    let mut memo = BTreeMap::new();
    for m in members {
        level(m, graph, &member_set, &mut memo);
    }
    let mut by_level: BTreeMap<usize, Vec<String>> = BTreeMap::new();
    for (n, l) in memo {
        by_level.entry(l).or_default().push(n.to_string());
    }
    by_level
        .into_iter()
        .rev()
        .map(|(_, mut v)| {
            v.sort();
            v
        })
        .collect()
}

pub fn generate() {
    let ws = workspace_graph();
    let graph = &ws.graph;
    let comp = product_composition(&ws);

    // validate curated orders against the derived facts
    let comp_names: BTreeSet<&str> = comp.keys().map(String::as_str).collect();
    let order_names: BTreeSet<&str> = PRODUCT_ORDER.iter().copied().collect();
    assert_eq!(comp_names, order_names, "PRODUCT_ORDER out of date");
    let protos_actual: BTreeSet<String> = LAYER
        .iter()
        .filter(|(_, l)| *l == Layer::Protocol)
        .map(|(n, _)| n.replace("protocol-", ""))
        .collect();
    let protos_order: BTreeSet<String> = PROTOCOL_ORDER.iter().map(|s| s.to_string()).collect();
    assert_eq!(protos_actual, protos_order, "PROTOCOL_ORDER out of date");

    let inner_w = CANVAS_W - 2.0 * MARGIN - LABEL_W;
    let mut parts: Vec<String> = Vec::new();
    let mut y = MARGIN;

    let band_label = |parts: &mut Vec<String>, name: &str, y0: f64, y1: f64| {
        parts.push(
            text(MARGIN + inner_w + LABEL_W / 2.0, (y0 + y1) / 2.0, name)
                .size(TS.h1)
                .bold()
                .build(),
        );
    };

    // ---- band 1: services (one nested box per product) -------------------
    let y0 = y;
    let n = PRODUCT_ORDER.len() as f64;
    let box_w = (inner_w - (n - 1.0) * GAP) / n;
    let bar_h = CELL_H - 6.0;
    let bar_gap = 8.0;
    let max_bars = comp
        .values()
        .map(|c| c.protocols.len() + usize::from(c.engine.is_some()) + c.core.len())
        .max()
        .unwrap() as f64;
    let box_h = PAD * 2.0 + CELL_H + max_bars * (bar_h + bar_gap);
    for (i, prod) in PRODUCT_ORDER.iter().enumerate() {
        let bx = MARGIN + i as f64 * (box_w + GAP);
        parts.push(
            rect(bx, y, box_w, box_h, "#F5F5F5")
                .stroke("#9E9E9E")
                .sw(2.0)
                .build(),
        );
        parts.push(
            text(bx + box_w / 2.0, y + PAD + CELL_H / 2.0 - 6.0, prod)
                .size(TS.h2)
                .bold()
                .build(),
        );
        let c = &comp[*prod];
        let mut bars: Vec<(String, &str, bool)> = Vec::new();
        for p in &c.protocols {
            bars.push((p.clone(), FILL_PROTOCOL, false));
        }
        if let Some(engine) = &c.engine {
            bars.push((
                engine_label(engine).to_string(),
                FILL_STORAGE,
                engine == "Noop",
            ));
        }
        for co in &c.core {
            bars.push((co.clone(), FILL_CORE, false));
        }
        let mut by = y + PAD + CELL_H;
        for (label, fill, stub) in bars {
            let mut r = rect(bx + PAD, by, box_w - 2.0 * PAD, bar_h, fill);
            if stub {
                r = r.dashed();
            }
            parts.push(r.build());
            parts.push(cell_text(
                bx + box_w / 2.0,
                by + bar_h / 2.0,
                &label,
                TS.body,
                false,
            ));
            by += bar_h + bar_gap;
        }
    }
    y += box_h;
    band_label(&mut parts, "Services", y0, y);

    // ---- rule -----------------------------------------------------------
    y += BAND_GAP;
    parts.push(
        rect(MARGIN, y, inner_w, RULE_H, "#BDBDBD")
            .stroke("none")
            .sw(0.0)
            .build(),
    );
    y += RULE_H + BAND_GAP;

    // ---- band 2: cache libraries (protocols group + storage group) ------
    let y0 = y;

    let group = |parts: &mut Vec<String>,
                 x: f64,
                 y: f64,
                 w: f64,
                 title: &str,
                 rows: &[Vec<String>],
                 fill: &str|
     -> f64 {
        let gh = PAD * 2.0 + CELL_H + rows.len() as f64 * (CELL_H + GAP) - GAP;
        parts.push(
            rect(x, y, w, gh, "#F5F5F5")
                .stroke("#9E9E9E")
                .sw(2.0)
                .build(),
        );
        parts.push(
            text(x + w / 2.0, y + PAD + CELL_H / 2.0 - 6.0, title)
                .size(TS.h2)
                .bold()
                .build(),
        );
        let mut cy = y + PAD + CELL_H;
        let nrows = rows.len();
        for (i, row) in rows.iter().enumerate() {
            // rows stack because of dependencies: dependents on top, most
            // opaque; each tier below fades
            let op = if nrows == 1 {
                1.0
            } else {
                1.0 - 0.72 * i as f64 / (nrows - 1) as f64
            };
            let cw = (w - 2.0 * PAD - (row.len() - 1) as f64 * GAP) / row.len() as f64;
            let mut cx = x + PAD;
            for cell in row {
                parts.push(rect(cx, cy, cw, CELL_H, fill).opacity(op).build());
                parts.push(cell_text(
                    cx + cw / 2.0,
                    cy + CELL_H / 2.0,
                    cell,
                    TS.body,
                    false,
                ));
                cx += cw + GAP;
            }
            cy += CELL_H + GAP;
        }
        gh
    };

    // protocols: wire protocols (and admin/http) over the shared substrate
    let proto_rows: Vec<Vec<String>> = vec![
        PROTOCOL_ORDER
            .iter()
            .filter(|p| **p != "common")
            .map(|s| s.to_string())
            .collect(),
        vec!["common".to_string()],
    ];
    let mut proto_deps = Graph::new();
    for (n, l) in LAYER {
        if *l == Layer::Protocol {
            let short = n.replace("protocol-", "");
            let d: BTreeSet<String> = graph[*n]
                .iter()
                .filter(|d| layer_of(d) == Some(Layer::Protocol))
                .map(|d| d.replace("protocol-", ""))
                .collect();
            proto_deps.insert(short, d);
        }
    }
    verify_topo_rows(&proto_rows, &proto_deps, "Protocols group");

    // storage: wrappers and helpers over the featured engine
    let stor_members = ["entrystore", "bloom", "storage-types", "segcache"];
    let stor_rows: Vec<Vec<String>> = vec![
        vec![
            "entrystore".to_string(),
            "bloom".to_string(),
            "storage-types".to_string(),
        ],
        vec!["segcache".to_string()],
    ];
    verify_topo_rows(
        &stor_rows,
        &extended_subgraph(graph, &stor_members),
        "Storage group",
    );

    let w_proto = inner_w * 0.55;
    let w_stor = inner_w - w_proto - GAP;
    let h1 = group(
        &mut parts,
        MARGIN,
        y,
        w_proto,
        "Protocols",
        &proto_rows,
        FILL_PROTOCOL,
    );
    let h2 = group(
        &mut parts,
        MARGIN + w_proto + GAP,
        y,
        w_stor,
        "Storage",
        &stor_rows,
        FILL_STORAGE,
    );
    y += h1.max(h2);
    band_label(&mut parts, "Cache libraries", y0, y);

    // ---- rule -----------------------------------------------------------
    y += BAND_GAP;
    parts.push(
        rect(MARGIN, y, inner_w, RULE_H, "#BDBDBD")
            .stroke("none")
            .sw(0.0)
            .build(),
    );
    y += RULE_H + BAND_GAP;

    // ---- band 3: runtime libraries (core group + foundation group) ------
    let y0 = y;
    let core_members: Vec<&str> = LAYER
        .iter()
        .filter(|(_, l)| *l == Layer::Core)
        .map(|(n, _)| *n)
        .collect();
    let core_rows = intra_block_levels(graph, &core_members);
    verify_topo_rows(
        &core_rows,
        &extended_subgraph(graph, &core_members),
        "Core group",
    );

    // foundation: strict utility chain plus external utilities, rewrapped
    // to two columns preserving topological reading order
    let fnd_deps = extended_subgraph(graph, FOUNDATION_ROW);
    let fnd_levels = intra_block_levels(&fnd_deps, FOUNDATION_ROW);
    let flat: Vec<String> = fnd_levels.into_iter().flatten().collect();
    let fnd_rows: Vec<Vec<String>> = flat.chunks(2).map(|c| c.to_vec()).collect();
    verify_topo_rows(&fnd_rows, &fnd_deps, "Foundation group");

    let w_core = inner_w * 0.5;
    let w_fnd = inner_w - w_core - GAP;
    let h1 = group(
        &mut parts,
        MARGIN,
        y,
        w_core,
        "Server / proxy core",
        &core_rows,
        FILL_CORE,
    );
    let h2 = group(
        &mut parts,
        MARGIN + w_core + GAP,
        y,
        w_fnd,
        "Foundation",
        &fnd_rows,
        FILL_FOUNDATION,
    );
    y += h1.max(h2);
    band_label(&mut parts, "Runtime libraries", y0, y);

    let total_h = y + MARGIN;
    let svg = svg_document(CANVAS_W, total_h, "cargo xtask diagrams", &parts);
    fs::create_dir_all("docs/diagrams").unwrap();
    fs::write(OUT, svg).unwrap();

    // signature and orphan report, matching the Python generator's output
    let products: Vec<&str> = PRODUCT_ORDER.to_vec();
    let mut shared: BTreeSet<String> = graph[products[0]].clone();
    for p in &products[1..] {
        shared = shared.intersection(&graph[*p]).cloned().collect();
    }
    println!("generated: {OUT}");
    println!(
        "shared direct deps (skeleton): {:?}",
        shared.iter().collect::<Vec<_>>()
    );
    let mut sorted_products = products.clone();
    sorted_products.sort();
    for p in sorted_products {
        let sig: Vec<&String> = graph[p].difference(&shared).collect();
        println!("  {p} signature: {sig:?}");
    }
    let orphans: Vec<&String> = graph
        .keys()
        .filter(|n| {
            layer_of(n) != Some(Layer::Product) && !graph.values().any(|deps| deps.contains(*n))
        })
        .collect();
    if !orphans.is_empty() {
        println!("note: crates with no workspace dependents: {orphans:?}");
    }
}
