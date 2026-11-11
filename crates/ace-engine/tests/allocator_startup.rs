//! The daemon binary starts without allocator diagnostics. A library test does not link the
//! binary's global allocator or its exported `malloc_conf`, so this runs the real executable.

use std::process::Stdio;
use std::time::Duration;

#[tokio::test]
async fn version_starts_without_allocator_diagnostics() {
    let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_outpace"));
    command
        .arg("--version")
        // jemalloc reads either name depending on symbol prefixing; an inherited override must
        // not mask broken compiled-in defaults.
        .env_remove("MALLOC_CONF")
        .env_remove("_RJEM_MALLOC_CONF")
        .stdin(Stdio::null())
        .kill_on_drop(true);

    let output = tokio::time::timeout(Duration::from_secs(15), command.output())
        .await
        .expect("outpace --version must finish within 15 seconds")
        .expect("outpace --version must launch");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "status={}, stderr={stderr:?}",
        output.status
    );
    assert_eq!(
        stdout.trim(),
        format!("outpace {}", env!("CARGO_PKG_VERSION"))
    );
    assert!(stderr.is_empty(), "startup diagnostics: {stderr:?}");
}
