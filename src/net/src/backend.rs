// Copyright 2026 Pelikan Foundation LLC.
// Licensed under the Apache License, Version 2.0

use std::fmt;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum IoBackend {
    #[default]
    Mio,
    Ringline,
}

impl IoBackend {
    pub fn parse(value: &str) -> Result<Self, InvalidIoBackend> {
        match value {
            "mio" => Ok(Self::Mio),
            "ringline" => Ok(Self::Ringline),
            _ => Err(InvalidIoBackend(value.to_string())),
        }
    }
}

impl fmt::Display for IoBackend {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Mio => write!(formatter, "mio"),
            Self::Ringline => write!(formatter, "ringline"),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InvalidIoBackend(pub String);

impl fmt::Display for InvalidIoBackend {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "invalid I/O backend: {}", self.0)
    }
}

impl std::error::Error for InvalidIoBackend {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FallbackReason {
    Unavailable,
    Initialization(String),
}

impl fmt::Display for FallbackReason {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unavailable => write!(formatter, "backend unavailable"),
            Self::Initialization(reason) => {
                write!(formatter, "backend initialization failed: {reason}")
            }
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BackendResolution {
    pub requested: IoBackend,
    pub active: IoBackend,
    pub fallback: Option<FallbackReason>,
}

pub fn resolve_backend(requested: IoBackend, ringline_available: bool) -> BackendResolution {
    let (active, fallback) = match requested {
        IoBackend::Mio => (IoBackend::Mio, None),
        IoBackend::Ringline if ringline_available => (IoBackend::Ringline, None),
        IoBackend::Ringline => (IoBackend::Mio, Some(FallbackReason::Unavailable)),
    };

    BackendResolution {
        requested,
        active,
        fallback,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_supported_names() {
        assert_eq!(IoBackend::parse("mio").unwrap(), IoBackend::Mio);
        assert_eq!(IoBackend::parse("ringline").unwrap(), IoBackend::Ringline);
        assert!(IoBackend::parse("other").is_err());
    }

    #[test]
    fn resolves_mio_by_default() {
        let resolution = resolve_backend(IoBackend::Mio, false);
        assert_eq!(resolution.requested, IoBackend::Mio);
        assert_eq!(resolution.active, IoBackend::Mio);
        assert_eq!(resolution.fallback, None);
    }

    #[test]
    fn resolves_ringline_when_available() {
        let resolution = resolve_backend(IoBackend::Ringline, true);
        assert_eq!(resolution.requested, IoBackend::Ringline);
        assert_eq!(resolution.active, IoBackend::Ringline);
        assert_eq!(resolution.fallback, None);
    }

    #[test]
    fn falls_back_when_ringline_is_unavailable() {
        let resolution = resolve_backend(IoBackend::Ringline, false);
        assert_eq!(resolution.requested, IoBackend::Ringline);
        assert_eq!(resolution.active, IoBackend::Mio);
        assert_eq!(resolution.fallback, Some(FallbackReason::Unavailable));
    }
}
