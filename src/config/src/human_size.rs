//! Deserialize byte sizes from either integers or human-readable strings.
//!
//! Supports GB, MB, KB suffixes (case-insensitive) or raw integer bytes.
//! Values must be whole numbers (e.g. "1GB", "512MB", "100KB", 4096).
//!
//! Three submodules for different target types:
//! - `as_u64` — for u64 fields (e.g. klog max_size)
//! - `as_usize` — for usize fields (e.g. seg heap_size)
//! - `as_i32` — for i32 fields with range validation (e.g. seg segment_size)

use serde::{self, Deserialize, Deserializer, Serialize, Serializer};

const KB: u64 = 1024;
const MB: u64 = 1024 * KB;
const GB: u64 = 1024 * MB;

fn parse_size(s: &str) -> Result<u64, String> {
    let s = s.trim();
    let upper = s.to_uppercase();

    let (num_str, multiplier) = if let Some(n) = upper.strip_suffix("GB") {
        (n, GB)
    } else if let Some(n) = upper.strip_suffix("MB") {
        (n, MB)
    } else if let Some(n) = upper.strip_suffix("KB") {
        (n, KB)
    } else {
        return s
            .parse::<u64>()
            .map_err(|_| format!("invalid size: {s} (expected integer or <N>KB/MB/GB)"));
    };

    num_str
        .trim()
        .parse::<u64>()
        .map(|n| n * multiplier)
        .map_err(|_| format!("invalid size: {s} (expected integer before unit)"))
}

fn deserialize_u64<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum SizeValue {
        Num(u64),
        Str(String),
    }

    match SizeValue::deserialize(deserializer)? {
        SizeValue::Num(n) => Ok(n),
        SizeValue::Str(s) => parse_size(&s).map_err(serde::de::Error::custom),
    }
}

/// For `#[serde(with = "crate::human_size::as_u64")]` on u64 fields.
pub mod as_u64 {
    use super::*;

    pub fn deserialize<'de, D>(deserializer: D) -> Result<u64, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserialize_u64(deserializer)
    }

    pub fn serialize<S>(value: &u64, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        value.serialize(serializer)
    }
}

/// For `#[serde(with = "crate::human_size::as_usize")]` on usize fields.
pub mod as_usize {
    use super::*;

    pub fn deserialize<'de, D>(deserializer: D) -> Result<usize, D::Error>
    where
        D: Deserializer<'de>,
    {
        let v = deserialize_u64(deserializer)?;
        usize::try_from(v)
            .map_err(|_| serde::de::Error::custom(format!("size {v} exceeds usize::MAX")))
    }

    pub fn serialize<S>(value: &usize, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        value.serialize(serializer)
    }
}

/// For `#[serde(with = "crate::human_size::as_i32")]` on i32 fields.
/// Validates that the parsed value fits within i32 range.
pub mod as_i32 {
    use super::*;

    pub fn deserialize<'de, D>(deserializer: D) -> Result<i32, D::Error>
    where
        D: Deserializer<'de>,
    {
        let v = deserialize_u64(deserializer)?;
        i32::try_from(v).map_err(|_| {
            serde::de::Error::custom(format!("size {v} exceeds i32::MAX ({})", i32::MAX))
        })
    }

    pub fn serialize<S>(value: &i32, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        value.serialize(serializer)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_gb() {
        assert_eq!(parse_size("1GB").unwrap(), 1024 * 1024 * 1024);
        assert_eq!(parse_size("2gb").unwrap(), 2 * 1024 * 1024 * 1024);
    }

    #[test]
    fn parse_mb() {
        assert_eq!(parse_size("512MB").unwrap(), 512 * 1024 * 1024);
    }

    #[test]
    fn parse_kb() {
        assert_eq!(parse_size("100KB").unwrap(), 100 * 1024);
    }

    #[test]
    fn parse_raw_number() {
        assert_eq!(parse_size("4096").unwrap(), 4096);
    }

    #[test]
    fn parse_with_spaces() {
        assert_eq!(parse_size(" 1 GB ").unwrap(), 1024 * 1024 * 1024);
    }

    #[test]
    fn parse_invalid() {
        assert!(parse_size("abc").is_err());
        assert!(parse_size("1TB").is_err());
        assert!(parse_size("1.5GB").is_err());
    }

    #[test]
    fn i32_overflow() {
        // 2GB = 2147483648, which is i32::MAX + 1
        assert!(parse_size("2GB").unwrap() > i32::MAX as u64);
        // 1GB fits
        assert!(parse_size("1GB").unwrap() <= i32::MAX as u64);
    }
}
