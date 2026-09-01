fn main() {
    let target_os = std::env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();
    let force_mio = std::env::var("CARGO_FEATURE_FORCE_MIO").is_ok();

    if target_os == "linux" && !force_mio && kernel_version_sufficient() {
        println!("cargo:rustc-cfg=has_io_uring");
    }
}

/// Check that the running kernel is 6.0+ (required for SendMsgZc, multishot
/// recv with provided buffers, and other io_uring features ringline depends on).
///
/// When cross-compiling, `/proc/sys/kernel/osrelease` reflects the *host* kernel
/// which may differ from the target. In that case we optimistically emit the cfg
/// and let the runtime fail fast if the target kernel is too old.
fn kernel_version_sufficient() -> bool {
    let Ok(release) = std::fs::read_to_string("/proc/sys/kernel/osrelease") else {
        // Not on Linux (cross-compile from macOS) or /proc not mounted.
        // Optimistically enable — the io-uring crate itself will fail to
        // compile if the target truly cannot support it.
        return true;
    };
    let mut parts = release.trim().split('.');
    let major: u32 = parts.next().and_then(|s| s.parse().ok()).unwrap_or(0);
    let minor: u32 = parts
        .next()
        .and_then(|s| {
            // Strip trailing non-digit suffix (e.g., "0-generic" → "0").
            let digits: String = s.chars().take_while(|c| c.is_ascii_digit()).collect();
            digits.parse().ok()
        })
        .unwrap_or(0);
    (major, minor) >= (6, 0)
}
