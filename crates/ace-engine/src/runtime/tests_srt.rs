//! SRT preflight belongs before persistent state, and listeners belong to serve cleanup.
use super::*;

#[tokio::test]
async fn srt_preflight_rejects_enabled_listener_before_identity_or_cache_mutation() {
    let dir = std::env::temp_dir().join(format!("outpace-srt-preflight-{}", rand::random::<u64>()));
    let mut config = Config {
        data_dir: dir.clone(),
        cache_dir: dir.join("cache"),
        networks: vec![],
        enable_inbound: false,
        ..Config::default()
    };
    config.srt.bind = Some("127.0.0.1:0".parse().unwrap());
    let result = build_runtime(config, vec![]).await;
    let expected = if cfg!(feature = "srt") {
        "srt_passphrase_file_required"
    } else {
        "srt_feature_unavailable"
    };
    assert!(matches!(result, Err(ref error) if error.to_string() == expected));
    assert!(
        !dir.exists(),
        "SRT preflight failure cannot create identity/cache state"
    );
}

#[tokio::test]
async fn disabled_srt_does_not_read_a_configured_missing_secret() {
    let dir = std::env::temp_dir().join(format!("outpace-srt-disabled-{}", rand::random::<u64>()));
    let mut config = Config {
        data_dir: dir.clone(),
        cache_dir: dir.join("cache"),
        networks: vec![],
        enable_inbound: false,
        ..Config::default()
    };
    config.srt.passphrase_file = Some(dir.join("missing-key"));
    let runtime = build_runtime(config, vec![]).await.unwrap();
    assert!(runtime.srt_config.is_none());
    assert!(runtime.manager.shutdown().await);
    runtime.broadcasts.shutdown().await.unwrap();
    std::fs::remove_dir_all(dir).unwrap();
}

#[cfg(feature = "srt")]
#[tokio::test]
async fn srt_bind_failure_returns_through_the_real_runtime_cleanup_path() {
    use std::os::unix::fs::PermissionsExt;
    let dir = std::env::temp_dir().join(format!("outpace-srt-bind-{}", rand::random::<u64>()));
    std::fs::create_dir(&dir).unwrap();
    std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700)).unwrap();
    let key = dir.join("key");
    std::fs::write(&key, b"synthetic-runtime-passphrase").unwrap();
    std::fs::set_permissions(&key, std::fs::Permissions::from_mode(0o600)).unwrap();
    let occupied = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    let mut config = Config {
        data_dir: dir.join("data"),
        cache_dir: dir.join("cache"),
        networks: vec![],
        enable_inbound: false,
        bind: "127.0.0.1:0".parse().unwrap(),
        rtmp_bind: "127.0.0.1:0".parse().unwrap(),
        ..Config::default()
    };
    config.srt.bind = Some(occupied.local_addr().unwrap());
    config.srt.passphrase_file = Some(key);
    let runtime = build_runtime(config, vec![]).await.unwrap();
    let manager = runtime.manager.clone();
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        serve_http_until(runtime, std::future::pending()),
    )
    .await;
    assert!(
        matches!(result, Ok(Err(_))),
        "SRT bind failure must not silently leave HTTP running"
    );
    assert_eq!(manager.admission_status().0, 0);
    std::fs::remove_dir_all(dir).unwrap();
}

#[cfg(feature = "srt")]
#[tokio::test]
async fn srt_immediate_shutdown_joins_listener_without_rereading_the_secret() {
    use std::os::unix::fs::PermissionsExt;
    let dir = std::env::temp_dir().join(format!("outpace-srt-shutdown-{}", rand::random::<u64>()));
    std::fs::create_dir(&dir).unwrap();
    std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700)).unwrap();
    let key = dir.join("key");
    std::fs::write(&key, b"synthetic-runtime-passphrase").unwrap();
    std::fs::set_permissions(&key, std::fs::Permissions::from_mode(0o600)).unwrap();
    let occupied = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    let mut config = Config {
        data_dir: dir.join("data"),
        cache_dir: dir.join("cache"),
        networks: vec![],
        enable_inbound: false,
        bind: "127.0.0.1:0".parse().unwrap(),
        rtmp_bind: "127.0.0.1:0".parse().unwrap(),
        ..Config::default()
    };
    config.srt.bind = Some(occupied.local_addr().unwrap());
    config.srt.passphrase_file = Some(key);
    drop(occupied);
    let address = config.srt.bind.unwrap();
    let runtime = build_runtime(config, vec![]).await.unwrap();
    std::fs::remove_file(dir.join("key")).unwrap();
    let manager = runtime.manager.clone();
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        serve_http_until(runtime, async {}),
    )
    .await;
    assert!(
        matches!(result, Ok(Ok(()))),
        "SRT startup shutdown must join even after the original key file disappears"
    );
    assert_eq!(manager.admission_status().0, 0);
    assert!(
        std::net::UdpSocket::bind(address).is_ok(),
        "shutdown releases the native address"
    );
    std::fs::remove_dir_all(dir).unwrap();
}
