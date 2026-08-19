// Copyright 2020 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use serde::{Deserialize, Serialize};

// definitions
#[derive(Clone, Serialize, Deserialize, Debug, Default)]
pub struct Tls {
    #[serde(default)]
    certificate_chain: Option<String>,
    #[serde(default)]
    private_key: Option<String>,
    #[serde(default)]
    certificate: Option<String>,
    #[serde(default)]
    ca_file: Option<String>,
}

// implementation
impl Tls {
    pub fn set_private_key(&mut self, path: impl Into<String>) {
        self.private_key = Some(path.into());
    }

    pub fn set_certificate(&mut self, path: impl Into<String>) {
        self.certificate = Some(path.into());
    }
}

impl common::ssl::TlsConfig for Tls {
    fn certificate_chain(&self) -> Option<String> {
        self.certificate_chain.clone()
    }

    fn private_key(&self) -> Option<String> {
        self.private_key.clone()
    }

    fn certificate(&self) -> Option<String> {
        self.certificate.clone()
    }

    fn ca_file(&self) -> Option<String> {
        self.ca_file.clone()
    }
}

// trait definitions
pub trait TlsConfig {
    fn tls(&self) -> &Tls;
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::ssl::TlsConfig as _;

    #[test]
    fn tls_files_can_be_selected_programmatically() {
        let mut tls = Tls::default();
        tls.set_private_key("key.pem");
        tls.set_certificate("cert.pem");
        assert_eq!(tls.private_key().as_deref(), Some("key.pem"));
        assert_eq!(tls.certificate().as_deref(), Some("cert.pem"));
    }
}
