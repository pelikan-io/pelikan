#!/usr/bin/env python3
"""Generate the threading architecture diagram for pelikan cache servers.

Emits docs/diagrams/threading.svg: two panels showing the runtime thread
model of `src/core/server` — the single-worker specialization (thread-local
storage) and the multi-worker model (dedicated storage thread) — with the
queues that connect the threads and the control/data plane separation.

The thread and queue inventory is asserted against the sources at generation
time: if a thread name, queue wiring site, or default port disappears from
the code, generation aborts rather than drawing a stale claim.

Run from the repo root:  python3 scripts/gen-threading-diagram.py
Requires: nothing beyond Python.
"""

import re
import sys
from pathlib import Path

OUT = Path("docs/diagrams/threading.svg")

# ---- source-of-truth assertions -------------------------------------------
# each claim the diagram makes is anchored to a grep against the code; a
# failed assertion means the thread model changed and the diagram must too
CLAIMS = [
    ("src/core/server/src/process.rs", r'name\(format!\("\{THREAD_PREFIX\}_admin"\)\)',
     "admin thread spawn"),
    ("src/core/server/src/process.rs", r'name\(format!\("\{THREAD_PREFIX\}_listener"\)\)',
     "listener thread spawn"),
    ("src/core/server/src/workers/mod.rs", r'name\(format!\("\{THREAD_PREFIX\}_work"\)\)',
     "single worker thread spawn"),
    ("src/core/server/src/workers/mod.rs", r'name\(format!\("\{THREAD_PREFIX\}_work_\{id\}"\)\)',
     "multi worker thread spawn"),
    ("src/core/server/src/workers/mod.rs", r'name\(format!\("\{THREAD_PREFIX\}_storage"\)\)',
     "storage thread spawn"),
    ("src/core/server/src/process.rs", r'name\(format!\("\{THREAD_PREFIX\}_signal"\)\)',
     "signal handler thread spawn"),
    ("src/core/server/src/process.rs", r"// queues for the `Admin` to send `Signal`s to all sibling threads",
     "admin signal broadcast queues"),
    ("src/core/server/src/process.rs", r"// queues for the `Listener` to send `Session`s to the worker threads",
     "listener->worker session queues"),
    ("src/core/server/src/workers/mod.rs", r"Queues::new\(worker_wakers, storage_wakers",
     "worker<->storage data queues"),
    ("src/core/server/src/process.rs", r"thread_wakers\.extend_from_slice\(&self\.workers\.wakers\(\)\)",
     "signal queues include all worker-side wakers (incl. storage)"),
    ("src/core/server/src/process.rs", r"SIGHUP, SIGINT, SIGTERM, SIGQUIT",
     "signal handler signal set"),
    ("src/core/proxy/src/process.rs", r'name\(format!\("\{THREAD_PREFIX\}_fe_\{i\}"\)\)',
     "proxy frontend worker spawn"),
    ("src/core/proxy/src/process.rs", r'name\(format!\("\{THREAD_PREFIX\}_be_\{i\}"\)\)',
     "proxy backend worker spawn"),
    ("src/core/proxy/src/process.rs", r"fe_data_queues, be_data_queues",
     "proxy frontend<->backend data queues"),
    ("src/core/proxy/src/backend.rs", r"TcpStream::connect\(endpoint\)",
     "proxy backend connects to upstream endpoints"),
    ("src/config/src/server.rs", r'const SERVER_PORT: &str = "12321"',
     "default data port"),
    ("src/config/src/admin.rs", r'const ADMIN_PORT: &str = "9999"',
     "default admin port"),
]


# claims of absence: the diagram relies on these NOT existing
NEG_CLAIMS = [
    ("src/core/proxy/src/process.rs", r'\{THREAD_PREFIX\}_signal',
     "proxy core has no OS signal-handler thread"),
]


def verify_claims():
    for path, pattern, what in CLAIMS:
        text = Path(path).read_text()
        if not re.search(pattern, text):
            sys.exit(f"ERROR: source claim not found ({what}): "
                     f"{pattern!r} in {path} — thread model changed?")
    for path, pattern, what in NEG_CLAIMS:
        if re.search(pattern, Path(path).read_text()):
            sys.exit(f"ERROR: absence claim violated ({what}): "
                     f"{pattern!r} now present in {path}")


# ---- style (shared visual language with gen-arch-diagrams.py) -------------
FONT = "Helvetica, Arial, sans-serif"
MONO = "SFMono-Regular, Menlo, Consolas, monospace"
FILL_THREAD = "#FFFFFF"     # plain container: nothing unusual about these threads

FILL_EXTERNAL = "#FFFFFF"
QUEUE_FILL = "#F2F2F2"
PANEL_FILL = "#FAFAFA"
PANEL_BORDER = "#9E9E9E"
EDGE = "#4D4D4D"
EDGE_SIGNAL = "#999999"


def rect(x, y, w, h, fill, stroke="#4D4D4D", sw=1.5, rx=0, dashed=False):
    dash = ' stroke-dasharray="6,4"' if dashed else ""
    return (f'<rect x="{x:.0f}" y="{y:.0f}" width="{w:.0f}" height="{h:.0f}" '
            f'fill="{fill}" stroke="{stroke}" stroke-width="{sw}" rx="{rx}"{dash}/>')


def text(x, y, s, size=13, weight="normal", fill="#000", italic=False,
         mono=False, anchor="middle"):
    style = ' font-style="italic"' if italic else ""
    fam = MONO if mono else FONT
    return (f'<text x="{x:.0f}" y="{y:.0f}" dy="0.35em" font-family="{fam}" '
            f'font-size="{size}" font-weight="{weight}" fill="{fill}"'
            f'{style} text-anchor="{anchor}">{s}</text>')


def thread_box(x, y, w, h, name, sub=None, chips=None, fill=FILL_THREAD,
               external=False):
    """A thread: rounded box (it runs). `chips` are the build-time modules
    that execute on this thread, drawn as small cells in the build chart's
    layer colors — the bridge between the build and runtime views."""
    out = [rect(x, y, w, h, fill, rx=10)]
    ty = y + 20
    out.append(text(x + w / 2, ty, name, size=17, weight="bold", mono=True))
    if sub:
        ty += 26
        out.append(text(x + w / 2, ty, sub, size=16, fill="#333",
                        italic=external))
    if chips:
        cw = (w - 16 - 6 * (len(chips) - 1)) / len(chips)
        cx = x + 8
        cy = y + h - 32
        for label, cfill in chips:
            out.append(rect(cx, cy, cw, 24, cfill, sw=1.0))
            out.append(text(cx + cw / 2, cy + 12, label, size=14))
            cx += cw + 6
    return out


def queue_glyph(x, y, w, h, label, ncells=5):
    out = []
    cw = w / ncells
    for i in range(ncells):
        out.append(rect(x + i * cw, y, cw, h, QUEUE_FILL, sw=1.0))
    if " (" in label:
        first, rest = label.split(" (", 1)
        out.append(text(x + w / 2, y - 24, first, size=14, fill="#555"))
        out.append(text(x + w / 2, y - 11, f"({rest}", size=14, fill="#555"))
    else:
        out.append(text(x + w / 2, y - 10, label, size=14, fill="#555"))
    return out


def ortho(points, signal=False, both=False, network=False):
    """Orthogonal polyline arrow through (x, y) waypoints — straight
    corners only. Network edges (bytes crossing the process boundary)
    draw heavier than internal queue edges (parsed structs)."""
    stroke = EDGE_SIGNAL if signal else EDGE
    dash = ' stroke-dasharray="5,4"' if signal else ""
    ms = ' marker-start="url(#a)"' if both else ""
    sw = 2.4 if network else 1.4
    d = f'M {points[0][0]:.0f} {points[0][1]:.0f} ' + " ".join(
        f"L {x:.0f} {y:.0f}" for x, y in points[1:])
    return [(f'<path d="{d}" fill="none" stroke="{stroke}" '
             f'stroke-width="{sw}"{dash} marker-end="url(#a)"{ms}/>')]


CHIP_PROTOCOL = ("protocol-*", "#FBB4AE")
CHIP_PROTOCOL_ADMIN = ("protocol-admin", "#FBB4AE")
CHIP_ENTRYSTORE = ("entrystore", "#B3CDE3")
CHIP_SEGCACHE = ("segcache", "#B3CDE3")


# uniform geometry: every thread container is the same size; externals
# share their own smaller dashed size
TB_W, TB_H = 270, 92
EXT_W, EXT_H = 90, 56
GAP = 34                      # minimum arrow length between columns


def label_w(s, size=14):
    """Estimated rendered width of a label."""
    return len(s) * size * 0.55 + 16


def gap_for(label):
    """Column gap sized to fit its arrow label."""
    return max(GAP, label_w(label) + 10)


ELBOW = 45                    # elbow verticals sit this far from a queue


def queue_gap(label, qw=50):
    """Gap on each side of a queue column: must fit the label overhang
    ((label - queue)/2) and keep the elbow lane clear of it. Labels wrap
    at " (" so only the longest line drives the overhang."""
    longest = max(label.split(" ("), key=len)
    return max(GAP + 6, (label_w(longest) - qw) / 2 + 8, ELBOW + 14)


def ext_box(x, y_row, name):
    """External element, vertically centered against a thread row."""
    y = y_row + (TB_H - EXT_H) / 2
    return [rect(x, y, EXT_W, EXT_H, FILL_EXTERNAL, dashed=True),
            text(x + EXT_W / 2, y + EXT_H / 2, name, size=15, italic=True)]


def worker_column(parts, x, y_top, names, chips):
    """Two uniform thread boxes with an ellipsis between them."""
    y1 = y_top + TB_H + 34
    parts += thread_box(x, y_top, TB_W, TB_H, names[0], None, chips=chips)
    parts += thread_box(x, y1, TB_W, TB_H, names[1], None, chips=chips)
    dots_cy = (y_top + TB_H + y1) / 2
    for dy in (-8, 0, 8):
        parts.append(f'<circle cx="{x + TB_W / 2:.0f}" '
                     f'cy="{dots_cy + dy:.0f}" r="2" fill="#777"/>')
    return y1


def margin_block(parts, cx, cy, title, rows):
    """Right-margin block: panel title over a binary->protocol mini-table,
    the whole block vertically centered at cy."""
    row_h, title_h, gap = 20, 26, 8
    block_h = title_h + gap + len(rows) * row_h
    ty = cy - block_h / 2 + title_h / 2
    parts.append(text(cx, ty, title, size=20, weight="bold"))
    ry = ty + title_h / 2 + gap + row_h / 2
    for binary, proto in rows:
        parts.append(text(cx - 6, ry, binary, size=13, fill="#555",
                          anchor="end"))
        parts.append(text(cx, ry, ":", size=13, fill="#555"))
        parts.append(text(cx + 8, ry, proto, size=13, fill="#555",
                          anchor="start"))
        ry += row_h


def panel(y0, title, rows, multi):
    parts = []
    W = 1700
    H = 414 if multi else 316
    x0 = 24
    parts.append(rect(x0, y0, W, H, PANEL_FILL, PANEL_BORDER, 2))
    margin_block(parts, x0 + W + 100, y0 + H / 2, title, rows)

    row_a = y0 + 56
    mid_a = row_a + TB_H / 2

    cl_x = x0 + 26
    parts += ext_box(cl_x, row_a, "clients")

    li_x = cl_x + EXT_W + max(gap_for("accept"), TB_W + GAP - EXT_W)
    parts += thread_box(li_x, row_a, TB_W, TB_H, "pelikan_listener", ":12321")
    parts += ortho([(cl_x + EXT_W, mid_a), (li_x, mid_a)], network=True)
    parts.append(text((cl_x + EXT_W + li_x) / 2, mid_a - 10, "accept",
                      size=14, fill="#555"))

    q_w = 50
    qg = queue_gap("sessions")
    q_x = li_x + TB_W + qg
    parts += queue_glyph(q_x, mid_a - 9, q_w, 18, "sessions")
    parts += ortho([(li_x + TB_W, mid_a), (q_x, mid_a)])

    wk_x = q_x + q_w + qg
    top_y = y0 + 26

    if not multi:
        parts += thread_box(wk_x, row_a, TB_W, TB_H, "pelikan_work", None,
                            chips=[CHIP_PROTOCOL, CHIP_ENTRYSTORE,
                                   CHIP_SEGCACHE])
        parts += ortho([(q_x + q_w, mid_a), (wk_x, mid_a)])
        wk_bottom = row_a + TB_H
        row_b = y0 + H - 100
    else:
        wk0_y = row_a
        wk1_y = worker_column(parts, wk_x, wk0_y,
                              ("pelikan_work_0", "pelikan_work_n-1"), [CHIP_PROTOCOL])
        parts += ortho([(q_x + q_w, mid_a), (wk_x - 18, mid_a),
                        (wk_x - 18, wk0_y + TB_H / 2), (wk_x, wk0_y + TB_H / 2)])
        parts += ortho([(q_x + q_w, mid_a), (wk_x - 18, mid_a),
                        (wk_x - 18, wk1_y + TB_H / 2), (wk_x, wk1_y + TB_H / 2)])
        dq_w = 50
        dqg = queue_gap("requests / responses (object)")
        dq_x = wk_x + TB_W + dqg
        dq_mid = (wk0_y + wk1_y + TB_H) / 2
        parts += queue_glyph(dq_x, dq_mid - 9, dq_w, 18,
                             "requests / responses (object)")
        parts += ortho([(wk_x + TB_W, wk0_y + TB_H / 2),
                        (dq_x - ELBOW, wk0_y + TB_H / 2),
                        (dq_x - ELBOW, dq_mid), (dq_x, dq_mid)], both=True)
        parts += ortho([(wk_x + TB_W, wk1_y + TB_H / 2),
                        (dq_x - ELBOW, wk1_y + TB_H / 2),
                        (dq_x - ELBOW, dq_mid), (dq_x, dq_mid)], both=True)
        st_x = dq_x + dq_w + dqg
        st_y = dq_mid - TB_H / 2
        parts += thread_box(st_x, st_y, TB_W, TB_H, "pelikan_storage", None,
                            chips=[CHIP_ENTRYSTORE, CHIP_SEGCACHE])
        parts += ortho([(dq_x + dq_w, dq_mid), (st_x, dq_mid)], both=True)
        wk_bottom = wk1_y + TB_H
        row_b = y0 + H - 100

    # requests/responses between clients and workers, over the top
    parts += ortho([(cl_x + EXT_W / 2, row_a + (TB_H - EXT_H) / 2),
                    (cl_x + EXT_W / 2, top_y), (wk_x + TB_W / 2, top_y),
                    (wk_x + TB_W / 2, row_a)], both=True, network=True)
    parts.append(text((cl_x + wk_x + TB_W) / 2, top_y - 10,
                      "requests / responses (wire)", size=14, fill="#555"))

    # control plane: signal left of admin, admin aligned under listener
    sg_x = x0 + 26
    parts += thread_box(sg_x, row_b, TB_W, TB_H, "pelikan_signal", "SIGINT/TERM/QUIT",
                        external=True)
    parts += thread_box(li_x, row_b, TB_W, TB_H, "pelikan_admin", ":9999",
                        chips=[CHIP_PROTOCOL_ADMIN])
    mid_b = row_b + TB_H / 2
    parts += ortho([(sg_x + TB_W, mid_b), (li_x, mid_b)], signal=True)
    parts += ortho([(li_x + 40, row_b), (li_x + 40, row_a + TB_H)],
                   signal=True)
    parts.append(text(li_x + 14, (row_b + row_a + TB_H) / 2, "signals",
                      size=14, fill="#777"))
    parts += ortho([(li_x + TB_W, mid_b), (wk_x + TB_W / 2, mid_b),
                    (wk_x + TB_W / 2, wk_bottom)], signal=True)
    if multi:
        parts += ortho([(li_x + TB_W, mid_b), (st_x + TB_W / 2, mid_b),
                        (st_x + TB_W / 2, st_y + TB_H)], signal=True)
    return parts, H


def proxy_panel(y0, title, rows):
    parts = []
    W, H = 1700, 414
    x0 = 24
    parts.append(rect(x0, y0, W, H, PANEL_FILL, PANEL_BORDER, 2))
    margin_block(parts, x0 + W + 100, y0 + H / 2, title, rows)

    row_a = y0 + 56
    mid_a = row_a + TB_H / 2

    cl_x = x0 + 26
    parts += ext_box(cl_x, row_a, "clients")

    li_x = cl_x + EXT_W + max(gap_for("accept"), TB_W + GAP - EXT_W)
    parts += thread_box(li_x, row_a, TB_W, TB_H, "pelikan_listener", ":12321")
    parts += ortho([(cl_x + EXT_W, mid_a), (li_x, mid_a)], network=True)
    parts.append(text((cl_x + EXT_W + li_x) / 2, mid_a - 10, "accept",
                      size=14, fill="#555"))

    q_w = 50
    qg = queue_gap("sessions")
    q_x = li_x + TB_W + qg
    parts += queue_glyph(q_x, mid_a - 9, q_w, 18, "sessions")
    parts += ortho([(li_x + TB_W, mid_a), (q_x, mid_a)])

    fe_x = q_x + q_w + qg
    fe0_y = row_a
    fe1_y = worker_column(parts, fe_x, fe0_y,
                          ("pelikan_fe_0", "pelikan_fe_n-1"), [CHIP_PROTOCOL])
    parts += ortho([(q_x + q_w, mid_a), (fe_x - 16, mid_a),
                    (fe_x - 16, fe0_y + TB_H / 2), (fe_x, fe0_y + TB_H / 2)])
    parts += ortho([(q_x + q_w, mid_a), (fe_x - 16, mid_a),
                    (fe_x - 16, fe1_y + TB_H / 2), (fe_x, fe1_y + TB_H / 2)])

    top_y = y0 + 26
    parts += ortho([(cl_x + EXT_W / 2, row_a + (TB_H - EXT_H) / 2),
                    (cl_x + EXT_W / 2, top_y), (fe_x + TB_W / 2, top_y),
                    (fe_x + TB_W / 2, fe0_y)], both=True, network=True)
    parts.append(text((cl_x + fe_x + TB_W) / 2, top_y - 10,
                      "requests / responses (wire)", size=14, fill="#555"))

    dq_w = 50
    dqg = queue_gap("requests / responses (object)")
    dq_x = fe_x + TB_W + dqg
    grp_mid = (fe0_y + fe1_y + TB_H) / 2
    parts += queue_glyph(dq_x, grp_mid - 9, dq_w, 18,
                         "requests / responses (object)")
    parts += ortho([(fe_x + TB_W, fe0_y + TB_H / 2),
                    (dq_x - ELBOW, fe0_y + TB_H / 2),
                    (dq_x - ELBOW, grp_mid), (dq_x, grp_mid)], both=True)
    parts += ortho([(fe_x + TB_W, fe1_y + TB_H / 2),
                    (dq_x - ELBOW, fe1_y + TB_H / 2),
                    (dq_x - ELBOW, grp_mid), (dq_x, grp_mid)], both=True)

    be_x = dq_x + dq_w + dqg
    be1_y = worker_column(parts, be_x, fe0_y,
                          ("pelikan_be_0", "pelikan_be_n-1"), [CHIP_PROTOCOL])
    parts += ortho([(dq_x + dq_w, grp_mid), (dq_x + dq_w + ELBOW, grp_mid),
                    (dq_x + dq_w + ELBOW, fe0_y + TB_H / 2),
                    (be_x, fe0_y + TB_H / 2)], both=True)
    parts += ortho([(dq_x + dq_w, grp_mid), (dq_x + dq_w + ELBOW, grp_mid),
                    (dq_x + dq_w + ELBOW, fe1_y + TB_H / 2),
                    (be_x, fe1_y + TB_H / 2)], both=True)

    sv_x = be_x + TB_W + gap_for("connect")
    parts += ext_box(sv_x, row_a, "servers")
    parts += ortho([(be_x + TB_W, mid_a), (sv_x, mid_a)], both=True,
                   network=True)
    parts.append(text((be_x + TB_W + sv_x) / 2, mid_a - 10,
                      "connect", size=14, fill="#555"))

    # control plane: admin only — the proxy core installs no OS signal handler
    row_b = y0 + H - 100
    parts += thread_box(li_x, row_b, TB_W, TB_H, "pelikan_admin", ":9999",
                        chips=[CHIP_PROTOCOL_ADMIN])
    mid_b = row_b + TB_H / 2
    parts += ortho([(li_x + 40, row_b), (li_x + 40, row_a + TB_H)],
                   signal=True)
    parts.append(text(li_x + 14, (row_b + row_a + TB_H) / 2, "signals",
                      size=14, fill="#777"))
    parts += ortho([(li_x + TB_W, mid_b), (fe_x + TB_W / 2, mid_b),
                    (fe_x + TB_W / 2, fe1_y + TB_H)], signal=True)
    parts += ortho([(li_x + TB_W, mid_b), (be_x + TB_W / 2, mid_b),
                    (be_x + TB_W / 2, be1_y + TB_H)], signal=True)
    return parts, H


def main():
    verify_claims()
    parts = ['<defs><marker id="a" viewBox="0 0 10 10" refX="9" refY="5" '
             'markerWidth="6" markerHeight="6" orient="auto-start-reverse">'
             '<path d="M 0 0 L 10 5 L 0 10 z" fill="#4D4D4D"/></marker></defs>']
    y = 24
    p1, h1 = panel(y, "single worker", [("segcache", "memcache"), ("rds", "resp"), ("pingserver", "ping")], multi=False)
    parts += p1
    y += h1 + 20
    p2, h2 = panel(y, "multiple workers", [("segcache", "memcache"), ("rds", "resp"), ("pingserver", "ping")], multi=True)
    parts += p2
    y += h2 + 20
    p3, h3 = proxy_panel(y, "proxy", [("pingproxy", "ping")])
    parts += p3
    W, H = 24 + 1700 + 200 + 24, y + h3 + 24
    svg = (f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" '
           f'viewBox="0 0 {W} {H}">\n'
           f'<!-- generated by scripts/gen-threading-diagram.py, do not edit -->\n'
           f'<rect width="100%" height="100%" fill="white"/>\n'
           + "\n".join(parts) + "\n</svg>")
    OUT.write_text(svg + "\n")
    print(f"generated: {OUT}")


if __name__ == "__main__":
    main()
