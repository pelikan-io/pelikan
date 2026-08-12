# Architecture diagrams

## What

Generated architecture diagrams for the workspace: one full crate-dependency
chart clustered by layer, plus one minichart per product highlighting what
architecturally distinguishes it. Applied the `dataflow-diagram` skill's
principles to a domain it wasn't written for (dependency graphs, not runtime
dataflow), recording every override — raw material for a future
architecture-diagram skill.

## Decided

- **Derive, never draw**: nodes/edges come from `cargo metadata` at generation
  time (`scripts/gen-arch-diagrams.py`); an unclassified workspace crate or a
  stale classification aborts the run.
- **Distinctiveness = direct dependencies.** First attempt used
  intersection-of-transitive-closures as the shared skeleton; it failed
  because `entrystore` depends on all three data protocols, dragging every
  protocol crate into every product's closure. A product's architectural
  signature is its *direct* deps minus the deps all products share.
- Product signatures that fell out: the three servers are
  `<protocol> + server + entrystore`; the proxy is `protocol-ping + proxy`
  with storage reached only transitively.
- Orphan finding made visible: `bloom` and `protocol-http` have no workspace
  dependents.

## Skill overrides (findings for the future architecture-diagram skill)

- **Top-level chart: blocks, not crates.** Crate-level arrows at workspace
  scale are spaghetti regardless of layout tuning. The design that worked:
  each layer is ONE graphviz node whose HTML table label grids its member
  crates (rows = intra-block dependency levels, so stacking encodes intra-
  block dependency positionally), and the only arrows are aggregated
  block-level edges ("some crate in A depends on some crate in B").
  Failed attempts worth remembering: clusters + per-crate edges +
  concentrate (still spaghetti); clusters + cluster-clipped aggregate edges
  with newrank (global ranks interleave block interiors, staggering them);
  old-mode local cluster ranking (blocks scatter horizontally); osage
  packing (grid geometry right, but ignores cluster clipping so aggregate
  edges render as false crate-to-crate claims).
- **Block-level cycles are real and must survive.** protocols <-> storage
  genuinely depend on each other (protocol-admin/ping -> storage-types;
  entrystore -> protocol-*). A strict layer stack cannot claim acyclicity;
  draw both arrows and let the back edge bend (constraint=false).
- **Square-ish grids beat truthful-but-tall stacks.** Strict
  row-per-dependency-level made blocks very wide or very tall (foundation
  was a 4x1 column). Requirement that emerged: near-square balanced grids
  (6 -> 3x2, never 5+1 orphans), with the positional claim softened from
  "sits above its dependencies" to "row-major reading order is a
  topological order" — weaker but still checkable, and it survives any
  grid aspect.
- **Blocks band by hierarchy; same-band blocks align.** Block-level
  topological banding (rank=same for the mutually-dependent pair), bottoms
  loosely aligned, heights roughly multiples of each other.
- **Arrows should not drive an architecture diagram's layout at all.** The
  final design (after maintainer review of three graphviz iterations) has
  zero arrows in the top chart: three full-width bands separated by rules,
  position encodes layering, and product composition is shown by NESTING
  (each service box contains its protocol / storage-engine / core bars) —
  the style of the classic Pelikan architecture figure
  (pelikan.io/blog/benchmark-adq, Figure 1). Rigid alignment (uniform cell
  sizes, padded boxes) is itself a message: it signals the design is in
  order. Graphviz cannot deliver this; the generator emits SVG directly
  with computed geometry. Interdependency precision lives in the
  minicharts, which keep graphviz.
- **Provenance is not a category.** In-repo vs external crate is the wrong
  distinction for a Rust project's architecture chart — crates place by
  their role (Segcache from cache-rs sits prominently in Storage; rustls
  and metriken sit in the foundation row). The dashed-border external
  channel was dropped entirely from the top chart.
- **Recursive grouping goes all the way down, with a verifier.** Flattening
  a band into undifferentiated rows skims real structure (server/proxy sit
  on admin, which sits on session/queues; foundation is a strict chain).
  Each group's rows encode its internal dependency tiers, with the
  reference figure's full-width-substrate-bar idiom for shared bases
  (protocol-common under the protocols; Segcache under its wrappers;
  admin under server/proxy). Whether rows are derived or curated, a
  `verify_topo_rows` check asserts row-major reading order is topological
  against the real (external-extended) subgraph — curation can't drift.
- **Composition bars must still be derived.** The service boxes' bars come
  from direct deps plus a source grep for `use entrystore::<Engine>` (Seg
  vs Noop), failing loudly when a product deps entrystore without wiring an
  engine. Curated display orders (flagship product first, wire protocols
  before infrastructure ones) are validated against the classification
  tables at run time so they cannot drift silently.
- **Shape channel is droppable at block level.** "Products" as a block IS
  the set of binaries, so rounded-vs-square adds nothing there; keep the
  shape channel for minicharts where kinds mix.

- **Compute/data rounded-vs-square → binary/library.** In a dependency graph
  everything is code; the kind a reader resolves first is "runs" (product
  binary, rounded) vs "is linked" (library, square).
- **Fill = layer** (Accent palette re-roled: products yellow, core purple,
  protocol green, storage orange, foundation gray). In the full chart fill is
  redundant with cluster membership — kept anyway because the minicharts have
  no clusters and cross-chart color consistency is what makes the set read as
  one system.
- **External crates = dashed border + explicit whitelist.** The full external
  graph is ~270 crates; a curated EXTERNALS table (with a validity check) is a
  legitimate level-of-detail decision the dataflow skill has no analog for.
- **Dimming as a channel**: minicharts dim the shared skeleton (including
  externals) instead of omitting it — claims stay, salience changes.
- **Unused dataflow conventions**: segmented history glyphs (nothing holds a
  history), evaluation-order numbering (no order), dashed-unconstrained cycle
  edges (dependency graphs are acyclic by construction).
- **Key placement**: rendered as a cluster inside the same graph — layout
  engine guarantees no overlap, satisfying "placement is computed" without an
  inset-collision checker.
- `concentrate=true` merges parallel edge runs; emits cosmetic "degenerate
  concentrated rank" warnings on the foundation cluster but renders fine.

- **Minicharts scrapped.** Per-product dependency-closure charts (graphviz)
  added little beyond the top chart: the Services band's composition bars
  already carry each product's signature, and the textual table covers the
  rest. Techniques proven during the minichart experiments, banked for
  future charts (threading architecture, data flow): ranked layout by
  longest dependency path; block width proportional to dependency
  neighborhood (transitive deps + dependents); eliding ubiquitous
  foundation crates halves edge count and is statable as a verifiable
  claim; active-vs-inert distinction (entrystore links every protocol but
  a product exercises one — derive by restricting entrystore's protocol
  deps to the product's own and diffing closures); barycenter ordering
  recovers a columnar reading from a shared DAG without false ownership
  claims; arrowheads must enter target tops (side-entry reads as false
  same-rank edges); rank-skipping edges route through side channels.

## Open

- Distill the findings above into an `architecture-diagram` skill.
- Threading architecture diagram and data flow chart as separate derived
  diagrams (need ground truth from src/core/server thread spawning and
  queue wiring, not the crate graph).
- Decide whether `bloom`/`protocol-http` orphan status warrants action
  (removal, promotion, or documentation of intended future use).
- Port the generator from Python to a Rust `cargo xtask` once the chart
  conventions stabilize (after the threading/dataflow charts land):
  `cargo_metadata` gives the ground-truth extraction a typed API, and a
  pure-Rust repo shouldn't carry a Python tooling dependency. If deep D2
  integration is ever wanted (programmatic layout rather than emitting
  text), Go's `oss.terrastruct.com/d2` library is the alternative;
  Python stays only while the diagram language is still in flux.

## Threading diagram (2026-08-11 addendum)

Runtime companion to the build chart (`docs/diagrams/threading.svg`,
`scripts/gen-threading-diagram.py`), covering all four binaries in three
panels (single worker / multiple workers / proxy), stacked vertically for
mobile-friendly reading. Conventions that emerged, for the future skill's
runtime half:

- **Literal thread names, monospace** — boxes carry the exact
  `std::thread::Builder` names (`pelikan_work_0`, `pelikan_fe_n-1`), so an
  operator can match a hot thread in `top -H` to the chart directly; the
  names are grep-asserted against the spawn sites.
- **Chips bridge build and runtime** — each thread box contains the build
  modules that execute on it, in the build chart's layer colors. The
  single-vs-multi difference reads purely as chip migration (storage chips
  move from the worker into `pelikan_storage`).
- **Thread fill is reserved** for unusual scheduling (non-default
  scheduler, pinning); plain threads stay white so chips carry the color.
- **External elements are italic + dashed** (*clients*, *servers*, the
  *SIGINT/TERM/QUIT* stimulus); literal thread names stay upright mono.
- **Network vs internal**: boundary-crossing edges draw heavier; internal
  queues are labeled by payload truth ("parsed requests / responses").
  A process-boundary frame was rejected: clients and `pelikan_signal`
  share a column, so a rectangle would falsely enclose the external.
- **Queues are small connective glyphs** (5-6 narrow cells), not visual
  foci; queue labels size their columns' gaps, and elbow verticals run in
  lanes computed to clear the label spans.
- **Orthogonal arrows only; labels above arrows; columns on a computed
  grid** sized from max(min arrow length, label width).
- **Per-panel binary annotations** in the right margin (same convention as
  the build chart's band labels).
- **Negative claims matter**: the proxy core spawns no signal-handler
  thread — asserted as an absence check, and visible as the missing box.
  Possible real gap: pingproxy ignores SIGTERM (no graceful shutdown path
  from OS signals). *Update 2026-08-12: confirmed real and fixed — the
  proxy core now spawns `pelikan_signal` mirroring the server core; the
  fix tripped the absence claim exactly as designed, and the claim
  flipped to a positive one.*
- **Source anchoring for runtime charts**: 17 grep assertions (thread
  spawns, queue wiring, signal set, ports, upstream connect) abort
  generation on drift — the runtime analog of deriving from cargo
  metadata, for facts that live in code rather than manifests.

## Dataflow diagram (2026-08-11 addendum)

"Life of a request" chart (`docs/diagrams/dataflow.svg`,
`scripts/gen-dataflow-diagram.py`) — the third of the trio, and the one
squarely in the dataflow-diagram skill's home domain. Conventions:

- **Swimlanes are threads; stages sit in the lane that runs them.** The
  multi-worker panel's "dip" into `pelikan_storage` for execute, and the
  proxy's zig-zag (fe -> be -> upstream -> be -> fe), make the thread-hop
  cost of each design visible as geometry.
- **Numbered stage badges** — execution order is real here, so the
  dataflow skill's ordering convention finally applies (drawn as explicit
  circle+digit; unicode circled digits die in font fallback).
- **Stage verbs are the code's verbs** (`receive`, `execute`, `send`,
  `flush` — session.receive() bundles read+parse, session.send() bundles
  compose), each stage carrying chips for the modules it runs, completing
  the build -> threading -> dataflow chip thread.
- **Payload-truth edge labels**: "request/response bytes" on heavy
  network edges, `Request`/`Response` struct names on thin queue-crossing
  edges with small vertical queue glyphs at the crossings.
- **Per-panel stage pitch** computed so any stage count fits the shared
  panel width.
- Control plane omitted by scope decision; the threading chart carries it.

## Rust port (2026-08-12 addendum)

The three Python generators are now a single Rust `cargo xtask diagrams`
(xtask/ workspace member, alias in .cargo/config.toml), closing the
porting plan recorded above. Port notes:

- **Faithfulness proven, not assumed**: the Rust output is byte-identical
  to the Python output for all three charts (modulo Python printing
  `stroke-width="1.0"` where Rust prints `"1"`), verified by diffing
  before deleting the scripts.
- `cargo_metadata` replaces subprocess-plus-JSON for the build chart's
  ground truth; the regex claims and the entrystore-engine source grep
  carry over unchanged.
- Shared visual language (palette, type scale, builders for
  rect/text/ortho) lives once in xtask/src/svg.rs — the duplication the
  Python trio accumulated is gone.
- Adding xtask to the workspace immediately tripped the fail-loudly
  classification ("xtask not classified"), forcing the explicit TOOLING
  exclusion set — the guard works on tooling crates too.
- Contributors need no Python; the only toolchain is cargo, and the
  generator is one `cargo xtask diagrams` away.
