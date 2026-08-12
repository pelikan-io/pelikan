# Architecture Diagram Charter

This charter is a **delta on the skill's defaults**. Fill the required
bindings from project evidence; record a convention only where the project
deviates, with the reason. Absence means the default applies — which is
what keeps the skill usable with no charter at all.

This copy is filled for Pelikan.

## Required Bindings (no default exists)

### Chart Inventory

| Chart | Half | Output | Generator module | Claims |
| --- | --- | --- | --- | --- |
| Layered architecture | build | `docs/diagrams/architecture.svg` | `xtask/src/arch.rs` | classification tables + `verify_topo_rows` + engine grep |
| Thread model | runtime | `docs/diagrams/threading.svg` | `xtask/src/threading.rs` | source claims (`CLAIMS`/`NEG_CLAIMS`) |
| Life of a request | runtime | `docs/diagrams/dataflow.svg` | `xtask/src/dataflow.rs` | source claims + panel bounds check |

All three are embedded in `docs/ARCHITECTURE.md` with textual equivalents
(layer breakdown, thread table, binary:protocol table) adjacent to each.

### Generator

- Regeneration command: `cargo xtask diagrams` (alias in `.cargo/config.toml`)
- Toolchain: Rust only — `xtask/` is a workspace member; contributors need
  nothing beyond cargo
- Shared visual-language module: `xtask/src/svg.rs`
- Source-claim helper: `xtask/src/claims.rs` (positive and negative regex
  assertions; exits nonzero on drift)

### Ground Truth

- Build half: `cargo_metadata` for the workspace graph; composition grep
  `use entrystore::<Engine>` resolves each product's storage engine (Seg
  vs. Noop), failing loudly if a product links entrystore without wiring
  one.
- Runtime half: regex assertions against `src/core/server` and
  `src/core/proxy` — thread spawn sites and literal `pelikan_*` names,
  queue wiring, signal set, ports, upstream connects, event-loop verbs.
  A negative claim asserting the proxy spawned no signal-handler thread
  caught a real bug (no graceful shutdown on SIGTERM), tripped as designed
  when the bug was fixed (#181), and flipped to a positive claim.
- Curated tables (each validated at generation time): `LAYER`, `TOOLING`,
  `EXTERNALS`/`EXTERNAL_LINK`, `PRODUCT_ORDER`, `PROTOCOL_ORDER`,
  `FOUNDATION_ROW`, and the claims arrays in `threading.rs`/`dataflow.rs`.

### Freshness

- CI: the `diagram-freshness` job in `.github/workflows/cargo.yml` runs
  `cargo xtask diagrams && git diff --exit-code docs/diagrams/` on every PR.
- Locally: run the same two commands before pushing anything that touches
  dependencies, thread spawning, queue wiring, or the diagrams.

### Review Gate

- Every new chart and every visual change requires maintainer review
  (Yao Yue); approval of an earlier revision does not cover a later one.
  Consistent with the document-feature charter's review gates.

## Overrides (defaults apply unless listed here)

- Two type ramps instead of one (#183): the runtime charts are half again
  wider than the build chart, and maintainer review at full size found one
  ramp too small for them — the roles stay (h1/h2/body, named in
  `xtask/src/svg.rs`), but the build chart uses 20/17/14 and the runtime
  charts 24/21/18. The skill's single-ramp default assumes charts of
  similar width.
- The skill's default 16px sub-label size is dropped; sub-labels are body
  text. Exactly three sizes per ramp.
- Otherwise defaults adopted wholesale — the palette, style channels, edge
  weights, and panel conventions were derived from this project's diagrams.

## Charter Evidence

- Filled by and date: Claude (agent), reviewed with maintainer, 2026-08-12;
  converted to delta form on template sync (skills-mcp#21)
- Evidence: `docs/journal/2026-08-09-architecture-diagrams.md`, PRs #177,
  #178, #181, `xtask/src/`, `.github/workflows/cargo.yml`
- Unknowns or conflicts: none open
