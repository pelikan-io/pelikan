---
name: architecture-diagram
description: Use when creating or materially revising system architecture diagrams — a build-time structure chart (units, layers, product composition) or runtime charts (thread model, life-of-a-request flow). Triggers include "architecture diagram", "threading diagram", "dataflow chart", "life of a request", "diagram the system". Symptoms include a hand-maintained diagram drifting from the code, a chart that cannot say whether a thread or queue still exists, or structure and runtime claims tangled in one unreadable picture.
---

# Architecture Diagrams: a Build/Runtime Duo

## Purpose

A system's architecture needs at least two charts, because it makes two kinds
of claims with different ground truths:

- The **build-time chart** claims what the code *is*: its units (crates,
  packages, modules), how they layer, and what each shipped artifact is
  composed of. Ground truth is the build manifest, queried through the build
  system's own interface — never a hand-kept list.
- The **runtime charts** claim what the program *does*: the threads it spawns,
  the queues that connect them, and the path one request travels. Ground truth
  is the source itself, held to the chart by explicit assertions.

One chart cannot carry both. A dependency graph with runtime arrows overlaid
answers neither "what depends on what" nor "where does a request go", and a
reader cannot tell which kind of claim any given edge is making. Split the
claims, then bind the halves together visually (see "Chips bridge the halves").

## Relationship to the dataflow-diagram skill

This skill is the `dataflow-diagram` skill's principles carried into a
domain that skill was not written for, with every override recorded. The
shared principles below restate its core (derive-never-draw, fail-loudly,
channels as claims) compactly so an installed instance is self-contained;
where the two disagree, the disagreement is deliberate (arrows dropped
entirely from the build chart, provenance demoted from a visual channel to
nothing, geometry emitted directly rather than delegated to a layout
engine — each with its reason in the sections below). For a pure pipeline
or DAG chart of data moving through one program, use `dataflow-diagram`
directly; this skill covers the system-architecture duo — what the code
is, and what the process does at runtime.

## Project Contract

Read and fill the [diagram charter](references/diagram-charter.md) before
touching any diagram. It binds this skill's conventions to the project: the
chart inventory, the generator and its invocation, the ground-truth sources
and curated tables, the palette and type scale, the freshness check, and the
review gate. Recheck it at the start of every material diagram effort.

## Shared principles (both halves)

**Derive, never draw.** Nodes, edges, labels, and composition come from the
program's own structures at generation time. A hand-maintained diagram is a
second source of truth: correct the day it is drawn, wrong on the first
refactor, and silent about the divergence. A generated chart is regenerated;
a drawn one is renegotiated.

**Fail loudly on the unclassified.** Generators map program elements to
visual properties through tables. An element no table covers must abort the
run — silent omission still renders a complete-looking chart. The reverse
also fails loudly: a table entry naming an element the program no longer has
is a stale classification. Curated display orders (flagship product first,
wire protocols before infrastructure ones) are legitimate, but validate them
against the derived facts at run time so curation cannot drift.

**Assert absences too.** Some of a chart's strongest claims are negative:
this variant spawns *no* signal-handler thread, this band contains *no*
external units. Encode them as absence checks that abort on drift, so the
missing box stays honestly missing. A negative claim that fails is often a
real finding about the system, not the chart.

**Freshness is enforced, not hoped.** One command regenerates every chart;
CI runs it and fails on any diff against the committed output. This turns
every source assertion into a standing CI check: a refactor that changes the
thread model or the dependency structure fails the build instead of quietly
invalidating a picture.

**One visual language across the set.** A single shared module owns the
palette, type scale, and drawing primitives for all charts. Cross-chart
consistency is what makes separate charts read as one system — and rigid
alignment (uniform cell sizes, computed grids, consistent pitch) is itself a
message: it signals the design is in order.

**Chips bridge the halves.** Runtime containers (threads, stages) carry
small chips naming the build-time modules that execute there, filled in the
build chart's layer colors. A reader can then follow one module from the
layer diagram into the thread that runs it and the request stage that
exercises it. Differences between variants should read as chip migration —
in the reference implementation, single- versus multi-worker differs purely
by the storage chips moving from the worker thread into a dedicated storage
thread.

**Verify the rendering, not the source.** Bounds-check that every element
lands inside its panel (intentional margin elements excluded explicitly, not
by loosening the check). After a refactor of the generator, byte-compare the
output against the previous version — "looks the same" is not a check.
Renderers silently ignore attributes they do not support; confirm geometry
actually moved when you meant it to.

**Human review gates every chart.** Layout quality, label collisions, and
whether the chart actually communicates are perceptual judgments no
assertion covers. Every new chart and every visual-language change goes
through the human reviewer named in the charter; approval of an earlier
revision does not cover a later one.

## The build-time chart

- **Blocks, not units.** At whole-system scale, unit-level arrows are
  spaghetti regardless of layout tuning. Group units into a handful of
  blocks (layers, bands) and make the block the visual element.
- **Arrows should not drive the layout — and may not be needed at all.**
  Position can encode the dependency hierarchy: stacked full-width bands
  with layering read top-down, and composition shown by *nesting* (each
  product box contains bars for its protocol / storage / core choices)
  can eliminate arrows entirely. If block-level arrows remain, aggregate
  them ("some unit in A depends on some unit in B") — never per-unit.
- **Position claims are verified claims.** Within a block, arrange units so
  row-major reading order is a topological order of the real dependency
  subgraph, and assert that with a verifier at generation time. This is
  deliberately weaker than "sits above its dependencies" — it survives any
  grid aspect while staying checkable.
- **Square-ish grids beat truthful-but-tall stacks.** Strict
  row-per-dependency-level produces very wide or very tall blocks. Prefer
  near-square balanced grids (6 units as 3×2, never 5+1) under the
  row-major-topological constraint.
- **Block-level cycles are real.** Mutually dependent layers exist; a strict
  stack that hides the back edge claims an acyclicity the code does not
  have. Draw both directions and let the back edge bend, or band the
  mutually-dependent blocks at the same level.
- **Provenance is not a category.** In-repo versus external is the wrong
  primary distinction: units place by *role* (an external storage engine
  sits prominently in the storage band; external TLS and metrics libraries
  sit in the foundation). The full external graph is enormous, so externals
  enter through a curated whitelist — with a validity check so the whitelist
  cannot name things no longer depended on. Mark externals quietly (e.g.
  italic, or a link to their registry entry), not with a loud channel.
- **Composition is derived, including the parts manifests don't record.**
  A product's bars come from its direct dependencies plus targeted source
  greps for wiring choices a manifest cannot see (which storage engine a
  product instantiates). Fail loudly when a product links a facade without
  wiring a concrete choice.
- **Distinctiveness = direct dependencies.** What architecturally
  distinguishes one product from another is its direct dependencies minus
  those all products share. Transitive closures drag shared facades'
  dependencies into everything and erase the signature.

## The runtime charts

The runtime half is typically two charts: a **thread model** (who runs, how
connected) and a **request flow** (what happens to one request, in order).

Ground truth for both is source assertions: positive and negative pattern
claims against the spawn sites, queue wiring, signal registration, ports,
and event-loop verbs, checked at generation time. This is the runtime analog
of querying the build manifest — for facts that live in code rather than
manifests.

Thread model:

- **Literal runtime names, monospace.** Thread boxes carry the exact names
  the code registers, so an operator can match a hot thread in `top -H` (or
  the platform equivalent) to the chart directly. Grep-assert the names
  against the spawn sites. Monospace marks "this string is literal";
  reserve it for that.
- **Thread fill is reserved.** Plain threads stay unfilled so the module
  chips carry the color; fill a thread only for something genuinely unusual
  (non-default scheduler, pinning).
- **External elements are italic and dashed** (*clients*, *upstreams*, the
  signal stimulus) — one consistent style for everything outside the
  process. Do not draw a process-boundary frame if externals and internals
  must share a column; a rectangle that falsely encloses an external is a
  false claim.
- **Edge weight = boundary crossing.** Bytes crossing the process boundary
  draw heavier than internal queue traffic. Label edges by payload truth:
  what actually travels (wire bytes vs. parsed objects, named by the code's
  own types).
- **Queues are connective glyphs, not foci.** A small segmented glyph
  (few narrow cells) marks "a queue is here"; it should not compete with
  the threads it connects.
- **One panel per variant, stacked vertically** (mobile-friendly), each
  annotated in the margin with which binaries/configurations it covers.
  Same-role elements align across panels so variants compare by scanning.

Request flow:

- **Swimlanes are threads; a stage sits in the lane that runs it.** Thread
  hops then read as geometry — the dip into a storage thread, the zig-zag
  through a proxy — which is exactly the cost the chart exists to show.
- **Number the stages** with drawn badges (a circle plus a digit — Unicode
  circled digits die in font fallback). Execution order is a real fact
  here; numbering is honest.
- **Stage verbs are the code's verbs.** Name stages after the actual
  functions (`receive`, `execute`, `send`, `flush`), and note what each
  bundles. Invented vocabulary drifts; the code's vocabulary is asserted.
- **Uniform stage pitch across panels.** Keep horizontal spacing identical
  whether or not a lane switch occurs, so parallel panels column-align and
  the only visual difference between variants is the real one.
- **Scope each chart deliberately.** Data plane and control plane rarely
  belong on one request-flow chart; pick one and let the thread-model chart
  carry the other.

## Workflow

1. Read the charter; fill or update it if the project's bindings changed.
2. Decide which half (or both) the request touches, and which chart in the
   inventory it maps to — or whether it is genuinely a new chart.
3. Write the ground-truth extraction and claims *before* any geometry:
   manifest queries for the build half, source assertions (positive and
   negative) for the runtime half.
4. Build or extend the generator inside the project's native toolchain — no
   new language dependency for contributors — with all charts sharing the
   one visual-language module and one regeneration command.
5. Render-verify: bounds checks pass, claims pass, and (for refactors)
   output is byte-identical when no visual change was intended.
6. Regenerate via the single command; confirm the CI freshness check covers
   the new or changed chart.
7. Keep textual equivalents adjacent to every embedded chart, per the
   charter's placement conventions.
8. Obtain human review of the rendered result; re-review after any
   subsequent change, however small.
9. Record every convention this skill got wrong for your domain — with the
   reason — in the project's journal or equivalent. An override with a
   stated reason is worth more than an untested default, and it is how this
   skill improves.

## Known dead ends

Recorded so they are not re-attempted:

- Unit-level arrows at system scale, with any amount of layout-engine
  tuning (clustering, edge concentration, rank constraints): spaghetti.
- General-purpose graph layout engines for the band/nesting design: rigid
  alignment is the message, and layout engines will not deliver it — emit
  geometry directly.
- Transitive-closure intersection as the "shared skeleton": one shared
  facade drags everything into every product's closure.
- Unicode circled digits for stage numbering: font fallback.
- Text-decoration underlines for links: some SVG rasterizers ignore them —
  draw the line.
- Per-product dependency minicharts: superseded by composition nesting in
  the top chart plus a textual table. Prove a companion chart adds claims
  the main chart cannot carry before building it.

## Red flags

- A node or edge list maintained by hand beside the code it describes.
- A generator that skips an element it cannot classify.
- A curated order or whitelist with no validity check against derived facts.
- A runtime chart with no source assertions — or none that assert absence.
- Diagrams regenerable only on one contributor's machine (missing CI check,
  or a generator outside the project's native toolchain).
- Structure and runtime claims on one chart, or an edge whose kind of claim
  a reader cannot determine.
- A thread box whose name is not the literal runtime name.
- Chips or colors that disagree between the build and runtime charts.
- Variant panels whose spacing differs for reasons other than a real
  difference.
- A bounds check loosened to admit an overflowing label instead of the
  layout being fixed.
- A refactor of the generator merged without byte-comparing its output.
- A chart shipped without human review, or re-shipped under a stale
  approval.
- An override of these conventions applied without recording the reason.
