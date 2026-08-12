#!/usr/bin/env python3
"""Generate the "life of a request" dataflow diagram for pelikan binaries.

Emits docs/diagrams/dataflow.svg: three stacked panels (single-worker server,
multi-worker server, proxy) tracing one request through the threads of each
binary — swimlanes per thread, numbered stages in execution order, the build
modules each stage runs as uniform chips, queue glyphs where the path crosses
threads, and heavier strokes where bytes cross the process boundary.
Control plane (admin, signals) is intentionally out of scope.

Layout rules: one uniform gap between stage columns whether or not the path
switches lanes (same-lane labels float above the stage line; the gap is
sized for the elbow-and-queue run of a lane crossing). Stage claims are
asserted against the event-loop sources, and panel content is bounds-checked,
at generation time.

Run from the repo root:  python3 scripts/gen-dataflow-diagram.py
Requires: nothing beyond Python.
"""

import re
import sys
from pathlib import Path

OUT = Path("docs/diagrams/dataflow.svg")

CLAIMS = [
    ("src/core/server/src/workers/single.rs", r"session\.receive\(\)",
     "single worker: session.receive parses a request"),
    ("src/core/server/src/workers/single.rs", r"self\.storage\.execute\(&request\)",
     "single worker executes on thread-local storage"),
    ("src/core/server/src/workers/single.rs", r"session\.send\(response\)",
     "single worker composes the response"),
    ("src/core/server/src/workers/single.rs", r"session\.flush\(\)",
     "single worker flushes to the socket"),
    ("src/core/server/src/workers/multi.rs", r"try_send_to\(0, \(request, token\)\)",
     "multi worker enqueues the parsed request to storage"),
    ("src/core/server/src/workers/storage.rs", r"self\.storage\.execute\(&request\)",
     "storage thread executes requests"),
    ("src/core/server/src/workers/storage.rs", r"try_send_to\(sender, message\)",
     "storage thread returns responses to the sending worker"),
    ("src/core/server/src/workers/multi.rs", r"session\.send\(response\)",
     "multi worker composes the returned response"),
    ("src/core/proxy/src/frontend.rs", r"BackendRequest::from\(request\)",
     "proxy frontend forwards the parsed request to a backend"),
    ("src/core/proxy/src/backend.rs", r"try_send_to\(0, \(request, response, fe_token\)\)",
     "proxy backend returns the upstream response to the frontend"),
    ("src/core/proxy/src/backend.rs", r"session\.receive\(\)",
     "proxy backend parses the upstream response"),
]


def verify_claims():
    for path, pattern, what in CLAIMS:
        if not re.search(pattern, Path(path).read_text()):
            sys.exit(f"ERROR: source claim not found ({what}): "
                     f"{pattern!r} in {path} — request path changed?")


# ---- shared visual language (matches the threading diagram) ---------------
FONT = "Helvetica, Arial, sans-serif"
MONO = "SFMono-Regular, Menlo, Consolas, monospace"
FILL_STAGE = "#FFFFFF"
FILL_EXTERNAL = "#FFFFFF"
QUEUE_FILL = "#F2F2F2"
PANEL_FILL = "#FAFAFA"
PANEL_BORDER = "#9E9E9E"
EDGE = "#4D4D4D"
LANE_LINE = "#DDDDDD"

CHIP_SESSION = ("session", "#CCEBC5")
CHIP_PROTOCOL = ("protocol-*", "#FBB4AE")
CHIP_ENTRYSTORE = ("entrystore", "#B3CDE3")
CHIP_SEGCACHE = ("segcache", "#B3CDE3")

# geometry: uniform chips size the stage; one uniform inter-column gap
CHIP_W, CHIP_H = 84, 24
ST_W, ST_H = 184, 72
GAP = 80            # between stage columns, lane switch or not
LANE_H = 116
LANE_LABEL_W = 175
PANEL_W = 1740
X0 = 24
LBL = 14            # one size for every edge/queue/gap label


def rect(x, y, w, h, fill, stroke="#4D4D4D", sw=1.5, rx=0, dashed=False):
    dash = ' stroke-dasharray="6,4"' if dashed else ""
    return (f'<rect x="{x:.0f}" y="{y:.0f}" width="{w:.0f}" height="{h:.0f}" '
            f'fill="{fill}" stroke="{stroke}" stroke-width="{sw}" rx="{rx}"{dash}/>')


def text(x, y, s, size=LBL, weight="normal", fill="#000", italic=False,
         mono=False, anchor="middle"):
    style = ' font-style="italic"' if italic else ""
    fam = MONO if mono else FONT
    return (f'<text x="{x:.0f}" y="{y:.0f}" dy="0.35em" font-family="{fam}" '
            f'font-size="{size}" font-weight="{weight}" fill="{fill}"'
            f'{style} text-anchor="{anchor}">{s}</text>')


def ortho(points, both=False, network=False):
    ms = ' marker-start="url(#a)"' if both else ""
    sw = 2.4 if network else 1.4
    d = f'M {points[0][0]:.0f} {points[0][1]:.0f} ' + " ".join(
        f"L {x:.0f} {y:.0f}" for x, y in points[1:])
    return [(f'<path d="{d}" fill="none" stroke="{EDGE}" '
             f'stroke-width="{sw}" marker-end="url(#a)"{ms}/>')]


def vqueue(x, y_mid, label=None):
    """Small vertical queue glyph centered on a lane-crossing edge."""
    h, w, n = 60, 16, 5
    out = []
    ch = h / n
    for i in range(n):
        out.append(rect(x - w / 2, y_mid - h / 2 + i * ch, w, ch, QUEUE_FILL,
                        sw=1.0))
    if label:
        out.append(text(x + w / 2 + 8, y_mid, label, fill="#555",
                        anchor="start"))
    return out


def stage(x, y, num, name, chips):
    """One pipeline stage: numbered badge, verb, uniform-width module chips."""
    out = [rect(x, y, ST_W, ST_H, FILL_STAGE, rx=10)]
    out.append(f'<circle cx="{x + 18:.0f}" cy="{y + 17:.0f}" r="11" '
               f'fill="none" stroke="#4D4D4D" stroke-width="1.2"/>')
    out.append(text(x + 18, y + 17, str(num), weight="bold"))
    out.append(text(x + ST_W / 2 + 8, y + 17, name, size=17, weight="bold"))
    group_w = CHIP_W * len(chips) + 6 * (len(chips) - 1)
    cx = x + (ST_W - group_w) / 2
    cy = y + ST_H - 32
    for label, cfill in chips:
        out.append(rect(cx, cy, CHIP_W, CHIP_H, cfill, sw=1.0))
        out.append(text(cx + CHIP_W / 2, cy + CHIP_H / 2, label))
        cx += CHIP_W + 6
    return out


def lane(parts, y, name, external=False):
    if not external:
        parts.append(text(X0 + 18, y + LANE_H / 2, name, size=16,
                          weight="bold", mono=True, anchor="start"))
    parts.append(f'<line x1="{X0 + 12}" y1="{y + LANE_H}" '
                 f'x2="{X0 + PANEL_W - 12}" y2="{y + LANE_H}" '
                 f'stroke="{LANE_LINE}" stroke-width="1"/>')


def columns(n):
    """Uniformly pitched stage x positions."""
    x0 = X0 + LANE_LABEL_W + 30
    return [x0 + i * (ST_W + GAP) for i in range(n)]


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


def panel(y0, title, rows, kind):
    parts = []
    lanes = {"single": ["clients", "pelikan_work"],
             "multi": ["clients", "pelikan_work_i", "pelikan_storage"],
             "proxy": ["clients", "pelikan_fe_i", "pelikan_be_i", "servers"]}[kind]
    H = 40 + LANE_H * len(lanes) + 24
    parts.append(rect(X0, y0, PANEL_W, H, PANEL_FILL, PANEL_BORDER, 2))
    n_margin_parts = len(parts)
    margin_block(parts, X0 + PANEL_W + 100, y0 + H / 2, title, rows)

    lane_y = {}
    y = y0 + 28
    for nm in lanes:
        lane(parts, y, nm, external=nm in ("clients", "servers"))
        lane_y[nm] = y
        y += LANE_H

    def st_y(ln):
        return lane_y[ln] + (LANE_H - ST_H) / 2

    def st_mid(ln):
        return lane_y[ln] + LANE_H / 2

    def gap_label(xs, i, label, ln):
        """Same-lane transition label, floated above the stage line."""
        cx = xs[i] + ST_W + (xs[i + 1] - xs[i] - ST_W) / 2
        parts.append(text(cx, st_y(ln) - 12, label, fill="#555"))

    def crossing(xs, i, ln_from, ln_to, label):
        """Lane-crossing edge with a queue glyph at the midpoint."""
        gx = xs[i] + ST_W + GAP / 2
        parts.extend(ortho([(xs[i] + ST_W, st_mid(ln_from)),
                            (gx, st_mid(ln_from)),
                            (gx, st_mid(ln_to)), (xs[i + 1], st_mid(ln_to))]))
        parts.extend(vqueue(gx, (st_mid(ln_from) + st_mid(ln_to)) / 2, label))

    cl_x = X0 + LANE_LABEL_W + 30 - 30 - 90
    cl_y = st_mid("clients") - 28
    parts.append(rect(cl_x, cl_y, 90, 56, FILL_EXTERNAL, dashed=True))
    parts.append(text(cl_x + 45, cl_y + 28, "clients", size=15, italic=True))

    def wire_in(xs, ln):
        parts.extend(ortho([(cl_x + 90, st_mid("clients")),
                            (xs[0] + ST_W / 2, st_mid("clients")),
                            (xs[0] + ST_W / 2, st_y(ln))], network=True))
        parts.append(text((cl_x + 90 + xs[0] + ST_W / 2) / 2,
                          st_mid("clients") - 12, "request (wire)",
                          fill="#555"))

    def wire_out(xs, ln):
        x = xs[-1] + ST_W / 2
        parts.extend(ortho([(x, st_y(ln)), (x, st_mid("clients")),
                            (cl_x + 90, st_mid("clients"))], network=True))
        half = len("response (wire)") * LBL * 0.55 / 2 + 8
        lx = min(x + 90, X0 + PANEL_W - half)
        parts.append(text(lx, st_mid("clients") - 12, "response (wire)",
                          fill="#555"))

    if kind == "single":
        wl = "pelikan_work"
        xs = columns(4)
        specs = [("receive", [CHIP_SESSION, CHIP_PROTOCOL]),
                 ("execute", [CHIP_ENTRYSTORE, CHIP_SEGCACHE]),
                 ("send", [CHIP_SESSION, CHIP_PROTOCOL]),
                 ("flush", [CHIP_SESSION])]
        for i, (nm, chips) in enumerate(specs):
            parts.extend(stage(xs[i], st_y(wl), i + 1, nm, chips))
        wire_in(xs, wl)
        for i in range(3):
            parts.extend(ortho([(xs[i] + ST_W, st_mid(wl)),
                                (xs[i + 1], st_mid(wl))]))
        gap_label(xs, 0, "request (object)", wl)
        gap_label(xs, 1, "response (object)", wl)
        wire_out(xs, wl)

    elif kind == "multi":
        wl, sl = "pelikan_work_i", "pelikan_storage"
        xs = columns(4)
        parts.extend(stage(xs[0], st_y(wl), 1, "receive",
                           [CHIP_SESSION, CHIP_PROTOCOL]))
        parts.extend(stage(xs[1], st_y(sl), 2, "execute",
                           [CHIP_ENTRYSTORE, CHIP_SEGCACHE]))
        parts.extend(stage(xs[2], st_y(wl), 3, "send",
                           [CHIP_SESSION, CHIP_PROTOCOL]))
        parts.extend(stage(xs[3], st_y(wl), 4, "flush", [CHIP_SESSION]))
        wire_in(xs, wl)
        crossing(xs, 0, wl, sl, "request (object)")
        crossing(xs, 1, sl, wl, "response (object)")
        parts.extend(ortho([(xs[2] + ST_W, st_mid(wl)), (xs[3], st_mid(wl))]))
        wire_out(xs, wl)

    else:  # proxy
        fl, bl = "pelikan_fe_i", "pelikan_be_i"
        xs = columns(6)
        parts.extend(stage(xs[0], st_y(fl), 1, "receive",
                           [CHIP_SESSION, CHIP_PROTOCOL]))
        parts.extend(stage(xs[1], st_y(bl), 2, "send",
                           [CHIP_SESSION, CHIP_PROTOCOL]))
        parts.extend(stage(xs[2], st_y(bl), 3, "flush", [CHIP_SESSION]))
        parts.extend(stage(xs[3], st_y(bl), 4, "receive",
                           [CHIP_SESSION, CHIP_PROTOCOL]))
        parts.extend(stage(xs[4], st_y(fl), 5, "send",
                           [CHIP_SESSION, CHIP_PROTOCOL]))
        parts.extend(stage(xs[5], st_y(fl), 6, "flush", [CHIP_SESSION]))
        wire_in(xs, fl)
        crossing(xs, 0, fl, bl, "request (object)")
        parts.extend(ortho([(xs[1] + ST_W, st_mid(bl)), (xs[2], st_mid(bl))]))
        # upstream round trip through the servers lane
        sv_mid = st_mid("servers")
        sx = xs[2] + ST_W + GAP / 2
        parts.append(rect(sx - 45, sv_mid - 28, 90, 56, FILL_EXTERNAL,
                          dashed=True))
        parts.append(text(sx, sv_mid, "servers", size=15, italic=True))
        parts.extend(ortho([(xs[2] + ST_W / 2, st_y(bl) + ST_H),
                            (xs[2] + ST_W / 2, sv_mid), (sx - 45, sv_mid)],
                           network=True))
        parts.append(text(xs[2] + ST_W / 2 - 12, sv_mid - 34,
                          "request (wire)", fill="#555", anchor="end"))
        parts.extend(ortho([(sx + 45, sv_mid), (xs[3] + ST_W / 2, sv_mid),
                            (xs[3] + ST_W / 2, st_y(bl) + ST_H)],
                           network=True))
        parts.append(text(xs[3] + ST_W / 2 + 12, sv_mid - 34,
                          "response (wire)", fill="#555", anchor="start"))
        crossing(xs, 3, bl, fl, "response (object)")
        parts.extend(ortho([(xs[4] + ST_W, st_mid(fl)), (xs[5], st_mid(fl))]))
        wire_out(xs, fl)

    # bounds check: panel content (after the frame and the margin block)
    # stays inside
    n_margin = 1 + 1 + 3 * len(rows)  # frame + title + table cells
    for part in parts[n_margin:]:
        for mx in re.finditer(r'x2?="(-?\d+)"', part):
            if int(mx.group(1)) > X0 + PANEL_W:
                sys.exit(f"ERROR: element beyond panel bounds: {part[:90]}")
    return parts, H


def main():
    verify_claims()
    parts = ['<defs><marker id="a" viewBox="0 0 10 10" refX="9" refY="5" '
             'markerWidth="6" markerHeight="6" orient="auto-start-reverse">'
             '<path d="M 0 0 L 10 5 L 0 10 z" fill="#4D4D4D"/></marker></defs>']
    y = 24
    p1, h1 = panel(y, "single worker", [("segcache", "memcache"), ("rds", "resp"), ("pingserver", "ping")], "single")
    parts += p1
    y += h1 + 20
    p2, h2 = panel(y, "multiple workers", [("segcache", "memcache"), ("rds", "resp"), ("pingserver", "ping")], "multi")
    parts += p2
    y += h2 + 20
    p3, h3 = panel(y, "proxy", [("pingproxy", "ping")], "proxy")
    parts += p3
    W, H = X0 + PANEL_W + 200 + 24, y + h3 + 24
    svg = (f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" '
           f'viewBox="0 0 {W} {H}">\n'
           f'<!-- generated by scripts/gen-dataflow-diagram.py, do not edit -->\n'
           f'<rect width="100%" height="100%" fill="white"/>\n'
           + "\n".join(parts) + "\n</svg>")
    OUT.write_text(svg + "\n")
    print(f"generated: {OUT}")


if __name__ == "__main__":
    main()
