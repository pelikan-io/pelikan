// Copyright 2023 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use momento::MomentoError;
use thiserror::Error;

pub type ProxyResult<T = ()> = Result<T, ProxyError>;

#[derive(Debug, Error)]
pub enum ProxyError {
    #[error("momento error: {0}")]
    Momento(#[source] MomentoError),
    #[error("io error: {0}")]
    Io(#[source] std::io::Error),
    #[error("timeout: {0}")]
    Timeout(#[source] tokio::time::error::Elapsed),
    #[error("{0}")]
    Custom(&'static str),
    #[error("unsupported resp command")]
    UnsupportedCommand(&'static str),
}

impl ProxyError {
    pub fn custom(message: &'static str) -> Self {
        assert!(!message.contains("\r\n"));

        Self::Custom(message)
    }
}

impl From<MomentoError> for ProxyError {
    fn from(value: MomentoError) -> Self {
        ProxyError::Momento(value)
    }
}

impl From<std::io::Error> for ProxyError {
    fn from(value: std::io::Error) -> Self {
        ProxyError::Io(value)
    }
}

impl From<tokio::time::error::Elapsed> for ProxyError {
    fn from(value: tokio::time::error::Elapsed) -> Self {
        ProxyError::Timeout(value)
    }
}
