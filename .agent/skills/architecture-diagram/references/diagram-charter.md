# Architecture Diagram Charter

Fill this file from project evidence before using the skill. Replace all
placeholders, cite repository sources, and recheck the charter before every
material diagram effort. This copy is filled for Pelikan.

## Chart Inventory

| Chart | Half | Output | Generator module | Claims |
| --- | --- | --- | --- | --- |
| Layered architecture | build | `docs/diagrams/architecture.svg` | `xtask/src/arch.rs` | classification tables + `verify_topo_rows` + engine grep |
| Thread model | runtime | `docs/diagrams/threading.svg` | `xtask/src/threading.rs` | positive + negative source claims |
| Life of a request | runtime | `docs/diagrams/dataflow.svg` | `xtask/src/dataflow.rs` | 11 source claims + panel bounds check |

All three are embedded in `docs/ARCHITECTURE.md` with textual equivalents
(layer breakdown, thread table, binary:protocol table) adjacent to each.

## Generator

- Regeneration command: `cargo xtask diagrams` (alias in `.cargo/config.toml`;
  runs from any directory in the workspace)
- Toolchain: Rust only — `xtask/` is a workspace member; contributors need
  nothing beyond cargo
- Shared visual-language module: `xtask/src/svg.rs` (palette, type scale,
  rect/text/orthogonal-arrow builders, arrowhead defs)
- Source-claim helper: `xtask/src/claims.rs` (`Claim` struct, positive and
  negative regex assertions, exits nonzero on drift)

## Ground Truth Bindings

- Build half: `cargo_metadata` crate for the workspace dependency graph;
  product composition additionally greps `use entrystore::<Engine>` to
  resolve which storage engine each product wires (Seg vs. Noop), failing
  loudly if a product depends on entrystore without wiring an engine.
- Runtime half: regex assertions against `src/core/server` and
  `src/core/proxy` — thread spawn sites and literal thread names
  (`pelikan_*`), queue wiring, signal set (SIGINT/SIGTERM/SIGQUIT), ports,
  upstream connects, and event-loop verbs (`receive`/`execute`/`send`/
  `flush`). A negative claim asserting the proxy core spawned no
  signal-handler thread caught a real gap (no graceful shutdown on
  SIGTERM) and tripped as designed when the gap was fixed (#181), at
  which point it flipped to a positive claim.
- Curated tables that need maintenance when the workspace changes (each
  validated at generation time, so drift aborts the run): `LAYER`,
  `TOOLING` (excluded crates, e.g. `xtask` itself), `EXTERNALS` and
  `EXTERNAL_LINK`, `PRODUCT_ORDER`, `PROTOCOL_ORDER`, `FOUNDATION_ROW`, and
  the claims arrays in `threading.rs` / `dataflow.rs`.

## Visual Language Bindings

- Palette (d3 schemePastel1, re-roled): protocol `#FBB4AE`, storage
  `#B3CDE3`, core/runtime `#CCEBC5`, foundation `#F2F2F2`, externals white.
- Type scale: 14 (chips, legends, edge labels) / 16 (sub-labels) /
  17 (element labels) / 20 (panel titles).
- Monospace = literal runtime strings (thread names as they appear in
  `top -H`). Italic + dashed = external elements (*clients*, *servers*,
  signals). Underline + hyperlink = external crates (drawn as explicit
  lines — text-decoration is unreliable in rasterizers).
- Edge weight: 2.4 for wire/process-boundary edges, 1.4 for internal
  object/queue edges; labels sit above arrows; orthogonal arrows only.
- Panels stack vertically, one per variant (single worker / multiple
  workers / proxy), with right-margin vertically-centered annotations
  naming the binaries each panel covers, including the binary:protocol
  mini-table expanding `protocol-*` chips.

## Freshness

- CI: the `diagram-freshness` job in `.github/workflows/cargo.yml` runs
  `cargo xtask diagrams && git diff --exit-code docs/diagrams/` on every
  PR — this is what turns the source claims into standing CI checks.
- Locally: run the same two commands before pushing any change that touches
  dependencies, thread spawning, queue wiring, or the diagrams themselves.

## Review Gate

- Every new chart and every visual change requires maintainer review
  (Yao Yue), consistent with the document-feature charter's review gates:
  diagrams are always human-gated, and approval of an earlier revision does
  not cover a later one.

## Charter Evidence

- Filled by and date: Claude (agent), reviewed with maintainer, 2026-08-12
- Evidence: `docs/journal/2026-08-09-architecture-diagrams.md` (full design
  history and override findings), PRs #177 and #178, `xtask/src/`,
  `.github/workflows/cargo.yml`
- Unknowns or conflicts: none open
