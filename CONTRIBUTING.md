# Contributing to Pelikan

Thanks for your interest in contributing! This guide covers the practical
steps; for questions and discussions, join our
[Discord server](https://discord.gg/yUBWHqxGUR).

## Prerequisites

- Rust [stable toolchain](https://www.rust-lang.org/learn/get-started)
- C toolchain and `cmake` (used to build AWS-LC, the cryptography library
  backing our TLS support)

## Building and testing

```sh
cargo build --workspace           # debug build
cargo build --workspace --release # release build
cargo test --workspace            # run all tests
```

Note: integration tests bind the default ports (`12321` data, `9999` admin);
stop any running pelikan server instance before running the test suite.

Before submitting, make sure the following pass — CI enforces them:

```sh
cargo fmt --all -- --check
cargo clippy --all-targets --all-features
cargo test --workspace
```

## Submitting a change

1. Create a new issue describing the problem or feature
2. Fork on GitHub and clone your fork
3. Create a feature branch on your fork
4. Make your change; keep commits focused and use
   [conventional commit](https://www.conventionalcommits.org/) style
   (`feat:`, `fix:`, `docs:`, `chore:`, ...) matching the existing history
5. Push your feature branch
6. Create a pull request linked to the issue

## Project layout

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for how the workspace is
organized. Non-trivial efforts are logged in [docs/journal/](docs/journal/).

## License

By contributing, you agree that your contributions will be licensed under the
[Apache 2.0 license](LICENSE).
