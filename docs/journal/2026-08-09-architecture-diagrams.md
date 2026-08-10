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
