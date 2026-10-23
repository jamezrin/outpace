use super::*;
use ace_swarm::listen::{InboundAdmission, SeedRegistry};
use ace_swarm::store::PieceStore;
use ace_wire::handshake::Handshake;
use ace_wire::identity::Identity;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::task::JoinHandle;
use tokio::time::timeout;

fn fixture_registry(keys: &[[u8; 20]]) -> SeedRegistry {
    let registry = SeedRegistry::new();
    for key in keys {
        registry.register(
            *key,
            Arc::new(tokio::sync::Mutex::new(PieceStore::new(4, 4, 1024))),
        );
    }
    registry
}

fn admission(config: &Config) -> InboundAdmission {
    inbound_admission_from_config(config)
}

async fn connect(
    admission: &InboundAdmission,
    registry: SeedRegistry,
    key: [u8; 20],
) -> Option<(tokio::io::DuplexStream, JoinHandle<ace_peer::Result<()>>)> {
    let (client, server) = tokio::io::duplex(256);
    let future = admission.accept(
        server,
        registry,
        [8; 20],
        [0; 8],
        Arc::new(Identity::from_seed([7; 32])),
        [127, 0, 0, 1],
    )?;
    let task = tokio::spawn(future);
    let mut client = client;
    client
        .write_all(&Handshake::new(key, [9; 20]).encode())
        .await
        .unwrap();
    Some((client, task))
}

async fn handshake_reply(client: &mut tokio::io::DuplexStream) {
    let mut response = [0; ace_wire::handshake::HANDSHAKE_LEN];
    timeout(Duration::from_secs(1), client.read_exact(&mut response))
        .await
        .expect("handshake response deadline")
        .unwrap();
}

async fn no_handshake_reply(client: &mut tokio::io::DuplexStream) {
    let mut response = [0; ace_wire::handshake::HANDSHAKE_LEN];
    assert_eq!(
        timeout(Duration::from_secs(1), client.read(&mut response))
            .await
            .expect("rejection close deadline")
            .unwrap(),
        0,
        "full canonical stream rejects before handshake reply"
    );
}

fn env_result(value: Option<&str>) -> Result<String, std::env::VarError> {
    value
        .map(str::to_owned)
        .ok_or(std::env::VarError::NotPresent)
}

#[tokio::test]
async fn runtime_policy_controls_admission() {
    let key = [81; 20];
    let other = [82; 20];
    let registry = fixture_registry(&[key, other]);

    // Absence preserves the default 16 and passes the exact config-created domain to the real
    // generic listener connection method.
    let mut config = Config::default();
    apply_inbound_per_stream_env(&mut config, env_result(None)).unwrap();
    assert_eq!(config.max_inbound_per_stream, 16);
    let domain = admission(&config);
    let mut active = Vec::new();
    for _ in 0..16 {
        let (mut client, task) = connect(&domain, registry.clone(), key).await.unwrap();
        handshake_reply(&mut client).await;
        active.push((client, task));
    }
    let (mut rejected, rejected_task) = connect(&domain, registry.clone(), key).await.unwrap();
    no_handshake_reply(&mut rejected).await;
    assert!(rejected_task.await.unwrap().is_err());
    for (client, task) in active {
        drop(client);
        task.abort();
        let _ = task.await;
    }

    // N=0 disables only per-canonical admission. More than 16 same-key peers reach the actual
    // handler while G remains the independent global bound.
    let mut unlimited_config = Config::default();
    apply_inbound_per_stream_env(&mut unlimited_config, env_result(Some("0"))).unwrap();
    let unlimited = admission(&unlimited_config);
    let mut active = Vec::new();
    for _ in 0..17 {
        let (mut client, task) = connect(&unlimited, registry.clone(), key).await.unwrap();
        handshake_reply(&mut client).await;
        active.push((client, task));
    }
    assert!(active.len() > 16);
    for (client, task) in active {
        drop(client);
        task.abort();
        let _ = task.await;
    }

    // N=1 rejects only the second alias of one canonical stream; a distinct stream is admitted.
    let mut one_config = Config::default();
    apply_inbound_per_stream_env(&mut one_config, env_result(Some("1"))).unwrap();
    let one = admission(&one_config);
    let (mut first, first_task) = connect(&one, registry.clone(), key).await.unwrap();
    handshake_reply(&mut first).await;
    let (mut second, second_task) = connect(&one, registry.clone(), key).await.unwrap();
    no_handshake_reply(&mut second).await;
    assert!(second_task.await.unwrap().is_err());
    let (mut distinct, distinct_task) = connect(&one, registry.clone(), other).await.unwrap();
    handshake_reply(&mut distinct).await;
    drop((first, distinct));
    first_task.abort();
    distinct_task.abort();
    let _ = first_task.await;
    let _ = distinct_task.await;

    for n in ["16", "65"] {
        let mut parsed = Config::default();
        apply_inbound_per_stream_env(&mut parsed, env_result(Some(n))).unwrap();
        assert_eq!(parsed.max_inbound_per_stream, n.parse::<usize>().unwrap());
        let exact = admission(&parsed);
        let (mut client, task) = connect(&exact, registry.clone(), key).await.unwrap();
        handshake_reply(&mut client).await;
        drop(client);
        task.abort();
        let _ = task.await;
    }

    // N > G is valid, but the production global try-acquire still bounds retained handler futures.
    let mut bounded = Config {
        max_inbound_peers: 2,
        ..Config::default()
    };
    apply_inbound_per_stream_env(&mut bounded, env_result(Some("65"))).unwrap();
    let bounded_domain = admission(&bounded);
    let (mut g1, t1) = connect(&bounded_domain, registry.clone(), key)
        .await
        .unwrap();
    handshake_reply(&mut g1).await;
    let (mut g2, t2) = connect(&bounded_domain, registry.clone(), key)
        .await
        .unwrap();
    handshake_reply(&mut g2).await;
    assert!(connect(&bounded_domain, registry.clone(), other)
        .await
        .is_none());
    drop((g1, g2));
    t1.abort();
    t2.abort();
    let _ = t1.await;
    let _ = t2.await;

    let mut zero_global = Config {
        max_inbound_peers: 0,
        max_inbound_per_stream: 0,
        ..Config::default()
    };
    apply_inbound_per_stream_env(&mut zero_global, env_result(None)).unwrap();
    let zero_global_domain = admission(&zero_global);
    let retained = connect(&zero_global_domain, registry.clone(), key)
        .await
        .unwrap();
    assert!(connect(&zero_global_domain, registry.clone(), other)
        .await
        .is_none());
    drop(retained);

    // Invalid input fails strictly, including present-but-non-Unicode; no process env mutation.
    let overflow = ((usize::MAX as u128) + 1).to_string();
    for invalid in ["", "-1", "not-a-number", &overflow] {
        let mut config = Config::default();
        assert!(apply_inbound_per_stream_env(&mut config, env_result(Some(invalid))).is_err());
    }
    let mut config = Config::default();
    assert!(apply_inbound_per_stream_env(
        &mut config,
        Err(std::env::VarError::NotUnicode(std::ffi::OsString::from(
            "invalid"
        )))
    )
    .is_err());
}
