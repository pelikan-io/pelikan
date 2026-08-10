#!/usr/bin/env python3
"""Generate architecture diagrams from the cargo workspace dependency graph.

Derives content from `cargo metadata` — never from a hand-maintained list —
and emits docs/diagrams/architecture.svg: a layer-cake block diagram of the
workspace (Services / Cache libraries / Runtime libraries).

Every workspace crate must appear in LAYER below and every external dependency
worth showing must appear in EXTERNALS; an unclassified crate aborts the run
rather than being silently omitted.

Run from the repo root:  python3 scripts/gen-arch-diagrams.py
Requires: cargo.
"""

import json
import re
import subprocess
import sys
from pathlib import Path

OUT_DIR = Path("docs/diagrams")

# Classification table: crate -> layer. A workspace member missing from this
# table is a hard error, so a new crate forces a conscious classification.
LAYER = {
    "pelikan-segcache": "product",
    "pelikan-rds": "product",
    "pelikan-pingserver": "product",
    "pingproxy": "product",
    "server": "core",
    "proxy": "core",
    "admin": "core",
    "session": "core",
    "queues": "core",
    "protocol-admin": "protocol",
    "protocol-common": "protocol",
    "protocol-http": "protocol",
    "protocol-memcache": "protocol",
    "protocol-ping": "protocol",
    "protocol-resp": "protocol",
    "entrystore": "storage",
    "storage-types": "storage",
    "bloom": "storage",
    "common": "foundation",
    "config": "foundation",
    "logger": "foundation",
    "pelikan-net": "foundation",
}

# External (non-workspace) dependencies worth showing, and which crate pulls
# them in. Curated: the full external graph is ~270 crates. Each entry is
# (external crate, [workspace crates that depend on it directly]).
EXTERNALS = {
    "segcache": ["entrystore"],
    "rustls": ["pelikan-net"],
    "metriken": ["common"],
}

LAYER_ORDER = ["product", "core", "protocol", "storage", "foundation"]

LAYER_LABEL = {
    "product": "Products (src/server/*, src/proxy/*)",
    "core": "Server/Proxy core (src/core/*, src/session, src/queues)",
    "protocol": "Protocols (src/protocol/*)",
    "storage": "Storage (src/entrystore, src/storage/*)",
    "foundation": "Foundation (src/common, src/config, src/logger, src/net)",
}

# d3 schemePastel1 (ColorBrewer Pastel1): fill encodes layer.
FILL = {
    "product": "#FFFFCC",
    "core": "#CCEBC5",
    "protocol": "#FBB4AE",
    "storage": "#B3CDE3",
    "foundation": "#F2F2F2",
    "external": "#FFFFFF",
}

EDGE = "#4D4D4D"        # depends-on
PANEL_FILL = "#F2F2F2"
PANEL_BORDER = "#9E9E9E"


def cargo_metadata():
    out = subprocess.run(
        ["cargo", "metadata", "--no-deps", "--format-version", "1"],
        check=True, capture_output=True, text=True,
    )
    return json.loads(out.stdout)


def workspace_graph(meta):
    """Return {crate: set(workspace path-deps)} and the set of binary crates."""
    graph, bins = {}, set()
    for pkg in meta["packages"]:
        name = pkg["name"]
        if name not in LAYER:
            sys.exit(f"ERROR: workspace crate '{name}' is not classified in LAYER")
        deps = {d["name"] for d in pkg["dependencies"] if d.get("path")}
        graph[name] = deps
        if any("bin" in t["kind"] for t in pkg["targets"]):
            bins.add(name)
    stale = set(LAYER) - set(graph)
    if stale:
        sys.exit(f"ERROR: LAYER classifies crates not in the workspace: {stale}")
    for ext, users in EXTERNALS.items():
        for u in users:
            if u not in graph:
                sys.exit(f"ERROR: EXTERNALS entry '{ext}' names unknown crate '{u}'")
    return graph, bins


def intra_block_levels(graph, members):
    """Topological levels of the dependency graph restricted to one block:
    level(n) = 1 + max(level of its in-block deps). Higher levels sit above
    what they depend on, so stacking encodes intra-block dependency."""
    members = set(members)
    memo = {}

    def level(n):
        if n not in memo:
            deps = graph.get(n, set()) & members
            memo[n] = 1 + max((level(d) for d in deps), default=0)
        return memo[n]

    for n in members:
        level(n)
    by_level = {}
    for n, l in memo.items():
        by_level.setdefault(l, []).append(n)
    # highest level first: it depends on everything below it
    return [sorted(by_level[l]) for l in sorted(by_level, reverse=True)]


def squarish_rows(ordered):
    """Wrap an ordered list into a near-square grid with balanced rows
    (row sizes differ by at most one, no orphan single-cell rows)."""
    n = len(ordered)
    r = max(1, round(n ** 0.5))
    base, extra = divmod(n, r)
    if base == 1 and r > 1:  # avoid rows of one
        r -= 1
        base, extra = divmod(n, r)
    rows, i = [], 0
    for k in range(r):
        size = base + (1 if k < extra else 0)
        rows.append(ordered[i:i + size])
        i += size
    return rows


def block_table(block, members, rows, dashed=False):
    """One block as a single node with an HTML table label: cells colored by
    the block's layer fill, reading order (row-major) is a topological order
    of the intra-block dependencies."""
    fill = FILL[LAYER.get(members[0], "external")]
    label = dict(LAYER_LABEL, external="External dependencies")[block]
    ncols = max(len(r) for r in rows)
    html = [
        '<<table border="0" cellborder="0" cellspacing="6" cellpadding="4">',
        f'<tr><td colspan="{ncols}"><b>{label}</b></td></tr>',
    ]
    for row in rows:
        cells = "".join(
            f'<td border="1" bgcolor="{fill}" cellpadding="6">{n}</td>' for n in row
        )
        pad = ncols - len(row)
        left = pad // 2
        lpad = f'<td colspan="{left}"></td>' if left else ""
        rpad = f'<td colspan="{pad - left}"></td>' if pad - left else ""
        html.append(f"<tr>{lpad}{cells}{rpad}</tr>")
    html.append("</table>>")
    style = "dashed" if dashed else "solid"
    return (
        f'  {block} [shape=box, style="{style}", color="{PANEL_BORDER}", '
        f'fontname="Helvetica", fontsize=11, label={"".join(html)}];'
    )


SVG_STYLE = {
    "cell_h": 40, "pad": 16, "gap": 12, "band_gap": 26, "rule_h": 7,
    "canvas_w": 1280, "margin": 24, "label_w": 200,
    "font": "Helvetica, Arial, sans-serif",
}


def _rect(x, y, w, h, fill, stroke="#4D4D4D", sw=1.5, rx=0, opacity=1.0,
          dashed=False):
    op = f' fill-opacity="{opacity:.2f}"' if opacity < 1.0 else ""
    dash = ' stroke-dasharray="6,4"' if dashed else ""
    return (f'<rect x="{x:.0f}" y="{y:.0f}" width="{w:.0f}" height="{h:.0f}" '
            f'fill="{fill}" stroke="{stroke}" stroke-width="{sw}" rx="{rx}"{op}{dash}/>')


def _text(x, y, s, size=15, weight="normal", fill="#000", anchor="middle"):
    t = (f'<text x="{x:.0f}" y="{y:.0f}" dy="0.35em" '
         f'font-family="{SVG_STYLE["font"]}" '
         f'font-size="{size}" font-weight="{weight}" fill="{fill}" '
         f'text-anchor="{anchor}">{s}</text>')
    if s in EXTERNAL_LINK:
        # explicit underline: text-decoration is ignored by some renderers
        half = len(s) * size * 0.27
        uy = y + size * 0.62
        t += (f'<line x1="{x - half:.0f}" y1="{uy:.0f}" '
              f'x2="{x + half:.0f}" y2="{uy:.0f}" '
              f'stroke="{fill}" stroke-width="1"/>')
        t = f'<a href="{EXTERNAL_LINK[s]}" target="_blank">{t}</a>'
    return t


def product_composition(graph, meta):
    """Derive each product's composition bars from the code: its protocol
    crate(s), the entrystore engine its source actually wires (grepped from
    `use entrystore::<Engine>`), and which core (server/proxy) it runs on.
    Fails loudly if a product deps entrystore but no engine import is found."""
    src_dir = {p["name"]: Path(p["manifest_path"]).parent for p in meta["packages"]}
    comp = {}
    for prod in sorted(n for n, l in LAYER.items() if l == "product"):
        deps = graph[prod]
        protocols = sorted(d.replace("protocol-", "") for d in deps
                           if LAYER.get(d) == "protocol")
        core = sorted(d for d in deps if LAYER.get(d) == "core")
        engine = None
        if "entrystore" in deps:
            found = set()
            for rs in src_dir[prod].rglob("*.rs"):
                found.update(re.findall(r"use entrystore::(\w+)", rs.read_text()))
            if not found:
                sys.exit(f"ERROR: {prod} depends on entrystore but no "
                         f"`use entrystore::<Engine>` found in its sources")
            engine = "/".join(sorted(found))
        comp[prod] = {"protocols": protocols, "engine": engine, "core": core}
    return comp


def verify_topo_rows(rows, deps_of, context):
    """Row-major reading order must be a topological order: everything a cell
    depends on (within the group) appears strictly later. Fails loudly."""
    order = [c for row in rows for c in row]
    pos = {c: i for i, c in enumerate(order)}
    for c in order:
        for d in deps_of.get(c, ()):
            if d in pos and pos[d] <= pos[c]:
                sys.exit(f"ERROR: {context}: '{c}' reads after its dependency '{d}'")


def extended_subgraph(graph, members):
    """Dependency map restricted to `members`, with external-crate edges
    derived from EXTERNALS (user -> external)."""
    ext_label = {"segcache": "segcache",
                 "rustls": "rustls", "metriken": "metriken"}
    deps = {m: set(graph.get(m, ())) & set(members) for m in members}
    for ext, users in EXTERNALS.items():
        lbl = ext_label[ext]
        if lbl in members:
            for u in users:
                if u in members:
                    deps[u].add(lbl)
    return deps


ENGINE_LABEL = {"Seg": "segcache", "Noop": "Noop"}

# external modules: underlined and hyperlinked to their crate location
EXTERNAL_LINK = {
    "segcache": "https://crates.io/crates/segcache",
    "metriken": "https://crates.io/crates/metriken",
    "rustls": "https://crates.io/crates/rustls",
}

# curated display order (flagship first); validated against LAYER at run time
PRODUCT_ORDER = ["pelikan-segcache", "pelikan-rds", "pelikan-pingserver", "pingproxy"]
PROTOCOL_ORDER = ["memcache", "resp", "ping", "http", "admin", "common"]

# band 3 utility row: foundation crates plus external utilities, judged by
# their relationship to the stack rather than where they are hosted
FOUNDATION_ROW = ["common", "config", "logger", "pelikan-net", "metriken", "rustls"]


def full_chart(graph, bins, meta):
    """Layer-cake block diagram in the style of the classic Pelikan
    architecture figure: three full-width bands (Services / Cache libraries /
    Runtime libraries) separated by rules, band names in the right margin,
    composition shown by nesting (each service box contains its protocol,
    storage-engine, and core bars), rigidly aligned grids, no arrows."""
    S = SVG_STYLE
    comp = product_composition(graph, meta)
    inner_w = S["canvas_w"] - 2 * S["margin"] - S["label_w"]
    parts, y = [], S["margin"]

    def band_label(name, y0, y1):
        parts.append(_text(S["margin"] + inner_w + S["label_w"] / 2,
                           (y0 + y1) / 2, name, size=20, weight="bold"))

    # ---- band 1: services (one nested box per product) -------------------
    y0 = y
    assert set(PRODUCT_ORDER) == set(comp), "PRODUCT_ORDER out of date"
    prods = PRODUCT_ORDER
    n = len(prods)
    box_w = (inner_w - (n - 1) * S["gap"]) / n
    bar_h, bar_gap = S["cell_h"] - 6, 8
    max_bars = max(len(c["protocols"]) + (1 if c["engine"] else 0) + len(c["core"])
                   for c in comp.values())
    box_h = S["pad"] * 2 + S["cell_h"] + max_bars * (bar_h + bar_gap)
    for i, prod in enumerate(prods):
        bx = S["margin"] + i * (box_w + S["gap"])
        parts.append(_rect(bx, y, box_w, box_h, "#F5F5F5", "#9E9E9E", 2))
        parts.append(_text(bx + box_w / 2, y + S["pad"] + S["cell_h"] / 2 - 6,
                           prod, size=16, weight="bold"))
        by = y + S["pad"] + S["cell_h"]
        bars = ([(p, FILL["protocol"], False) for p in comp[prod]["protocols"]]
                + ([(ENGINE_LABEL.get(comp[prod]["engine"], comp[prod]["engine"]),
                     FILL["storage"], comp[prod]["engine"] == "Noop")]
                   if comp[prod]["engine"] else [])
                + [(c, FILL["core"], False) for c in comp[prod]["core"]])
        for label, fill, stub in bars:
            parts.append(_rect(bx + S["pad"], by, box_w - 2 * S["pad"], bar_h, fill,
                               dashed=stub))
            parts.append(_text(bx + box_w / 2, by + bar_h / 2, label, size=14))
            by += bar_h + bar_gap
    y += box_h
    band_label("Services", y0, y)

    # ---- rule -----------------------------------------------------------
    y += S["band_gap"]
    parts.append(_rect(S["margin"], y, inner_w, S["rule_h"], "#BDBDBD", "none", 0))
    y += S["rule_h"] + S["band_gap"]

    # ---- band 2: cache libraries (protocols group + storage group) ------
    y0 = y

    def group(x, w, title, rows, fill):
        """A group box whose rows of cells stretch to fill the width; row
        stacking encodes intra-group dependency (verified by caller)."""
        gh = S["pad"] * 2 + S["cell_h"] + len(rows) * (S["cell_h"] + S["gap"]) - S["gap"]
        parts.append(_rect(x, y, w, gh, "#F5F5F5", "#9E9E9E", 2))
        parts.append(_text(x + w / 2, y + S["pad"] + S["cell_h"] / 2 - 6,
                           title, size=16, weight="bold"))
        cy = y + S["pad"] + S["cell_h"]
        R = len(rows)
        for i, row in enumerate(rows):
            # rows stack because of dependencies: dependents on top, most
            # opaque; each tier below fades
            op = 1.0 if R == 1 else 1.0 - 0.72 * i / (R - 1)
            cw = (w - 2 * S["pad"] - (len(row) - 1) * S["gap"]) / len(row)
            cx = x + S["pad"]
            for cell in row:
                f = fill(cell) if callable(fill) else fill
                parts.append(_rect(cx, cy, cw, S["cell_h"], f, opacity=op))
                parts.append(_text(cx + cw / 2, cy + S["cell_h"] / 2, cell, size=14))
                cx += cw + S["gap"]
            cy += S["cell_h"] + S["gap"]
        return gh

    # protocols: wire protocols (and admin/http) over the shared substrate
    protos_actual = {n.replace("protocol-", "")
                     for n, l in LAYER.items() if l == "protocol"}
    assert set(PROTOCOL_ORDER) == protos_actual, "PROTOCOL_ORDER out of date"
    proto_rows = [[p for p in PROTOCOL_ORDER if p != "common"], ["common"]]
    proto_deps = {n.replace("protocol-", ""):
                  {d.replace("protocol-", "") for d in graph[n]
                   if LAYER.get(d) == "protocol"}
                  for n, l in LAYER.items() if l == "protocol"}
    verify_topo_rows(proto_rows, proto_deps, "Protocols group")

    # storage: wrappers and helpers over the featured engine
    stor_members = ["entrystore", "bloom", "storage-types", "segcache"]
    stor_rows = [["entrystore", "bloom", "storage-types"], ["segcache"]]
    verify_topo_rows(stor_rows, extended_subgraph(graph, stor_members),
                     "Storage group")

    w_proto = inner_w * 0.55
    w_stor = inner_w - w_proto - S["gap"]
    h1 = group(S["margin"], w_proto, "Protocols", proto_rows, FILL["protocol"])
    h2 = group(S["margin"] + w_proto + S["gap"], w_stor, "Storage", stor_rows,
               FILL["storage"])
    y += max(h1, h2)
    band_label("Cache libraries", y0, y)

    # ---- rule -----------------------------------------------------------
    y += S["band_gap"]
    parts.append(_rect(S["margin"], y, inner_w, S["rule_h"], "#BDBDBD", "none", 0))
    y += S["rule_h"] + S["band_gap"]

    # ---- band 3: runtime libraries (core group + foundation group) ------
    y0 = y
    # core: event-loop runtimes over shared admin over connection/IPC prims,
    # rows derived from the crate graph restricted to the core layer
    core_members = sorted(n for n, l in LAYER.items() if l == "core")
    core_rows = intra_block_levels(graph, core_members)
    verify_topo_rows(core_rows, extended_subgraph(graph, core_members),
                     "Core group")

    # foundation: strict utility chain plus external utilities, rewrapped to
    # two columns preserving topological reading order
    fnd_members = ["common", "config", "logger", "pelikan-net",
                   "metriken", "rustls"]
    fnd_deps = extended_subgraph(graph, fnd_members)
    fnd_levels = []
    memo = {}
    def flevel(n):
        if n not in memo:
            memo[n] = 1 + max((flevel(d) for d in fnd_deps[n]), default=0)
        return memo[n]
    for m in fnd_members:
        flevel(m)
    flat = [n for lv in sorted({v for v in memo.values()}, reverse=True)
            for n in sorted(m for m in fnd_members if memo[m] == lv)]
    fnd_rows = [flat[i:i + 2] for i in range(0, len(flat), 2)]
    verify_topo_rows(fnd_rows, fnd_deps, "Foundation group")

    w_core = inner_w * 0.5
    w_fnd = inner_w - w_core - S["gap"]
    h1 = group(S["margin"], w_core, "Server / proxy core", core_rows, FILL["core"])
    h2 = group(S["margin"] + w_core + S["gap"], w_fnd, "Foundation", fnd_rows,
               FILL["foundation"])
    y += max(h1, h2)
    band_label("Runtime libraries", y0, y)

    total_h = y + S["margin"]
    return (f'<svg xmlns="http://www.w3.org/2000/svg" '
            f'width="{S["canvas_w"]}" height="{total_h:.0f}" '
            f'viewBox="0 0 {S["canvas_w"]} {total_h:.0f}">\n'
            f'<!-- generated by scripts/gen-arch-diagrams.py, do not edit -->\n'
            f'<rect width="100%" height="100%" fill="white"/>\n'
            + "\n".join(parts) + "\n</svg>")


def main():
    meta = cargo_metadata()
    graph, bins = workspace_graph(meta)

    products = sorted(n for n, l in LAYER.items() if l == "product")
    # the skeleton is what every product *directly* depends on; a product's
    # remaining direct deps are its architectural signature
    shared_direct = set.intersection(*(graph[p] for p in products))

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    outputs = []

    full_svg = OUT_DIR / "architecture.svg"
    full_svg.write_text(full_chart(graph, bins, meta) + "\n")
    (OUT_DIR / "architecture.dot").unlink(missing_ok=True)
    outputs.append(full_svg)

    orphans = sorted(
        n for n in graph
        if n not in bins and not any(n in deps for deps in graph.values())
    )
    print(f"generated: {', '.join(str(o) for o in outputs)}")
    print(f"shared direct deps (skeleton): {sorted(shared_direct)}")
    for p in products:
        print(f"  {p} signature: {sorted(graph[p] - shared_direct)}")
    if orphans:
        print(f"note: crates with no workspace dependents: {orphans}")


if __name__ == "__main__":
    main()
