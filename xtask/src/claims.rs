//! Source-of-truth assertions: every claim a diagram makes is anchored to a
//! regex match against the sources; a failed assertion means the code
//! changed and the diagram must change with it. Negative claims assert that
//! something the diagram relies on being absent has not appeared.

use regex::Regex;
use std::fs;

pub struct Claim {
    pub path: &'static str,
    pub pattern: &'static str,
    pub what: &'static str,
}

pub fn verify(claims: &[Claim], neg_claims: &[Claim]) {
    for c in claims {
        let text =
            fs::read_to_string(c.path).unwrap_or_else(|e| panic!("cannot read {}: {e}", c.path));
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
        let text =
            fs::read_to_string(c.path).unwrap_or_else(|e| panic!("cannot read {}: {e}", c.path));
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
