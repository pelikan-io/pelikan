//! Source-of-truth assertions: every claim a diagram makes is anchored to a
//! regex match against the sources; a failed assertion means the code
//! changed and the diagram must change with it. Negative claims assert that
//! something the diagram relies on being absent has not appeared.

use regex::Regex;
use std::fs;
use std::path::PathBuf;
use std::sync::OnceLock;

// Resolve external source claims through Cargo so diagrams check exactly the
// dependency pinned by Cargo.lock, whether it comes from git or a release.
fn source_path(path: &str) -> PathBuf {
    if let Some((dependency, relative)) = path.split_once(':') {
        static METADATA: OnceLock<cargo_metadata::Metadata> = OnceLock::new();
        let metadata = METADATA.get_or_init(|| {
            cargo_metadata::MetadataCommand::new()
                .features(cargo_metadata::CargoOpt::AllFeatures)
                .other_options(vec!["--locked".into()])
                .exec()
                .expect("resolve locked dependency sources for diagram claims")
        });
        let mut packages = metadata.packages.iter().filter(|p| p.name == dependency);
        let package = packages
            .next()
            .unwrap_or_else(|| panic!("missing dependency: {dependency}"));
        assert!(
            packages.next().is_none(),
            "ambiguous dependency: {dependency}"
        );
        package
            .manifest_path
            .parent()
            .unwrap()
            .as_std_path()
            .join(relative)
    } else {
        PathBuf::from(path)
    }
}

pub struct Claim {
    pub path: &'static str,
    pub pattern: &'static str,
    pub what: &'static str,
}

pub fn verify(claims: &[Claim], neg_claims: &[Claim]) {
    for c in claims {
        let text = fs::read_to_string(source_path(c.path))
            .unwrap_or_else(|e| panic!("cannot read {}: {e}", c.path));
        let re = Regex::new(c.pattern).expect("claim pattern must compile");
        if !re.is_match(&text) {
            eprintln!(
                "ERROR: source claim not found ({}): {:?} in {} — code changed?",
                c.what, c.pattern, c.path
            );
            std::process::exit(1);
        }
    }
    for c in neg_claims {
        let text = fs::read_to_string(source_path(c.path))
            .unwrap_or_else(|e| panic!("cannot read {}: {e}", c.path));
        let re = Regex::new(c.pattern).expect("claim pattern must compile");
        if re.is_match(&text) {
            eprintln!(
                "ERROR: absence claim violated ({}): {:?} now present in {}",
                c.what, c.pattern, c.path
            );
            std::process::exit(1);
        }
    }
}
