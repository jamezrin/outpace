use std::process::Command;

#[test]
fn options51_cli_rejects_before_runtime() {
    let dir = std::env::temp_dir().join(format!("outpace-options51-cli-{}", rand::random::<u64>()));
    std::fs::create_dir_all(&dir).unwrap();
    let marker = "SYNTHETIC_SECRET_MARKER";
    let output = Command::new(env!("CARGO_BIN_EXE_outpace"))
        .args(["broadcast", "synthetic-origin"])
        .env(
            "OUTPACE_BROADCAST_OPTIONS",
            format!(r#"{{"synthetic-origin":{{"unknown":"{marker}"}}}}"#),
        )
        .env("OUTPACE_DATA_DIR", &dir)
        .env("OUTPACE_CACHE_DIR", dir.join("cache"))
        .env("OUTPACE_BIND", "127.0.0.1:0")
        .env("OUTPACE_RTMP_BIND", "127.0.0.1:0")
        .output()
        .unwrap();
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!output.status.success());
    assert!(
        stderr.contains("invalid_broadcast_options"),
        "child stderr did not contain the sanitized error: {stderr:?}"
    );
    assert!(!stderr.contains(marker));
    assert!(!dir.join("identity.seed").exists());
    assert!(!dir.join("broadcasts").exists());
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn options51_cli_rejects_a_precreated_profile_conflict() {
    let dir = std::env::temp_dir().join(format!(
        "outpace-options51-cli-conflict-{}",
        rand::random::<u64>()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let registry = ace_engine::broadcast::BroadcastRegistry::with_persist(
        &dir,
        ace_engine::config::CacheType::Memory,
        std::path::PathBuf::new(),
    );
    let seed = ace_swarm::listen::SeedRegistry::new();
    registry
        .mint("synthetic-origin", &[], &seed, 8 << 20)
        .await
        .unwrap();
    registry.shutdown().await.unwrap();

    let output = Command::new(env!("CARGO_BIN_EXE_outpace"))
        .args(["broadcast", "synthetic-origin"])
        .env(
            "OUTPACE_BROADCAST_OPTIONS",
            r#"{"synthetic-origin":{"title":"Conflicting synthetic title"}}"#,
        )
        .env("OUTPACE_DATA_DIR", &dir)
        .env("OUTPACE_CACHE_DIR", dir.join("cache"))
        .env("OUTPACE_BIND", "127.0.0.1:0")
        .env("OUTPACE_RTMP_BIND", "127.0.0.1:0")
        .output()
        .unwrap();
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!output.status.success());
    assert!(stderr.contains("broadcast_options_conflict"), "{stderr:?}");
    assert!(!stderr.contains("Conflicting synthetic title"));
    assert!(!dir.join("identity.seed").exists());
    let _ = std::fs::remove_dir_all(dir);
}
