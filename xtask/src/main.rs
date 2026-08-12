//! Repo automation tasks. Invoke as `cargo xtask <command>` from anywhere in
//! the workspace (the alias lives in .cargo/config.toml).

mod arch;
mod claims;
mod dataflow;
mod svg;
mod threading;

use std::env;
use std::path::Path;

fn main() {
    // run from the workspace root so source claims and output paths resolve
    let manifest = env::var("CARGO_MANIFEST_DIR").expect("run via cargo");
    let root = Path::new(&manifest).parent().expect("workspace root");
    env::set_current_dir(root).expect("chdir to workspace root");

    let cmd = env::args().nth(1).unwrap_or_default();
    match cmd.as_str() {
        "diagrams" => {
            arch::generate();
            threading::generate();
            dataflow::generate();
        }
        _ => {
            eprintln!("usage: cargo xtask diagrams");
            eprintln!();
            eprintln!("  diagrams   regenerate docs/diagrams/*.svg from the workspace");
            std::process::exit(2);
        }
    }
}
