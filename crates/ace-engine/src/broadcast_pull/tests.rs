use crate::broadcast_pull::*;

use crate::broadcast::{BroadcastError, BroadcastRegistry, ProducerKind};
use crate::http::BroadcastState;
use ace_swarm::http_fetch::FetchError;
use ace_swarm::http_fetch::FetchPolicy;
use ace_swarm::listen::SeedRegistry;
use async_trait::async_trait;
use bytes::Bytes;
use serde_json::json;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{mpsc, watch};

const POLICY: FetchPolicy = FetchPolicy {
    allow_non_global: true,
};

fn source(url: &str) -> BroadcastSourceConfig {
    BroadcastSourceConfig {
        url: url.to_owned(),
        connect_timeout_secs: 5,
        read_timeout_secs: 15,
        reconnect_initial_secs: 1,
        reconnect_max_secs: 30,
    }
}

fn ts_packets(count: usize, marker: u8) -> Bytes {
    let mut bytes = Vec::with_capacity(count * 188);
    for _ in 0..count {
        bytes.push(0x47);
        bytes.extend(std::iter::repeat_n(marker, 187));
    }
    Bytes::from(bytes)
}

struct EndBody;
#[async_trait]
impl TestSourceBody for EndBody {
    async fn next_chunk(&mut self) -> Result<Option<Bytes>, FetchError> {
        Ok(None)
    }
}

struct PendingBody;
#[async_trait]
impl TestSourceBody for PendingBody {
    async fn next_chunk(&mut self) -> Result<Option<Bytes>, FetchError> {
        std::future::pending().await
    }
}

struct StepsBody(VecDeque<Step>);
enum Step {
    Empty,
    Stall,
}
#[async_trait]
impl TestSourceBody for StepsBody {
    async fn next_chunk(&mut self) -> Result<Option<Bytes>, FetchError> {
        match self.0.pop_front().unwrap_or(Step::Stall) {
            Step::Empty => Ok(Some(Bytes::new())),
            Step::Stall => std::future::pending().await,
        }
    }
}

struct ChannelBody(mpsc::UnboundedReceiver<Result<Option<Bytes>, FetchError>>);
#[async_trait]
impl TestSourceBody for ChannelBody {
    async fn next_chunk(&mut self) -> Result<Option<Bytes>, FetchError> {
        self.0.recv().await.unwrap_or(Err(FetchError::Cancelled))
    }
}

fn attached_state(registry: Arc<BroadcastRegistry>, policy: FetchPolicy) -> BroadcastState {
    registry.set_source_policy(policy);
    BroadcastState {
        registry,
        seed_registry: SeedRegistry::new(),
        trackers: vec![],
        store_bytes: 4 << 20,
        inbound_peer_port: watch::channel(None).1,
    }
}

async fn wait_status(
    receiver: &mut mpsc::UnboundedReceiver<SourceStatus>,
    predicate: impl Fn(&SourceStatus) -> bool,
) -> SourceStatus {
    for _ in 0..10_000 {
        if let Ok(status) = receiver.try_recv() {
            if predicate(&status) {
                return status;
            }
        }
        tokio::task::yield_now().await;
    }
    panic!("timed out waiting for source status transition")
}

fn test_broadcast_state(registry: Arc<BroadcastRegistry>, policy: FetchPolicy) -> BroadcastState {
    attached_state(registry, policy)
}

async fn attach_test_source(
    state: &BroadcastState,
    name: &str,
    config: BroadcastSourceConfig,
    policy: FetchPolicy,
) -> (crate::broadcast::Broadcast, SourceStatus) {
    state
        .attach_source(name, validate_source(config, policy).unwrap())
        .await
        .unwrap()
}

async fn send_test_frame(
    frames: &mpsc::UnboundedSender<Result<Option<Bytes>, FetchError>>,
    progress: &mut mpsc::UnboundedReceiver<crate::broadcast_ingest::IngestProgress>,
    bytes: Bytes,
) -> crate::broadcast_ingest::IngestProgress {
    frames.send(Ok(Some(bytes))).unwrap();
    for _ in 0..1_000 {
        if let Ok(progress) = progress.try_recv() {
            return progress;
        }
        tokio::task::yield_now().await;
    }
    panic!("worker should consume the injected body frame and report real ingest progress")
}

#[test]
fn pull206_source_config_contract() {
    let defaults: BroadcastSourceConfig =
        serde_json::from_str(r#"{"url":"https://source.invalid/live"}"#).unwrap();
    assert_eq!(defaults.connect_timeout_secs, 5);
    assert_eq!(defaults.read_timeout_secs, 15);
    assert_eq!(defaults.reconnect_initial_secs, 1);
    assert_eq!(defaults.reconnect_max_secs, 30);
    assert!(serde_json::from_str::<BroadcastSourceConfig>("{}").is_err());

    for invalid in [
        r#"{"url":"https://source.invalid/live","connect_timeout_secs":null}"#,
        r#"{"url":"https://source.invalid/live","connect_timeout_secs":1.5}"#,
        r#"{"url":"https://source.invalid/live","connect_timeout_secs":-1}"#,
        r#"{"url":"https://source.invalid/live","connect_timeout_secs":0}"#,
        r#"{"url":"https://source.invalid/live","connect_timeout_secs":61}"#,
        r#"{"url":"https://source.invalid/live","read_timeout_secs":301}"#,
        r#"{"url":"https://source.invalid/live","reconnect_initial_secs":301}"#,
        r#"{"url":"https://source.invalid/live","reconnect_max_secs":0}"#,
        r#"{"url":"https://source.invalid/live","reconnect_max_secs":301}"#,
        r#"{"url":"https://source.invalid/live","unexpected":true}"#,
    ] {
        let parsed = serde_json::from_str::<BroadcastSourceConfig>(invalid);
        if let Ok(config) = parsed {
            assert!(
                validate_source(config, POLICY).is_err(),
                "accepted {invalid}"
            );
        }
    }

    let mut config = source("https://source.invalid/live");
    config.reconnect_initial_secs = 31;
    assert!(validate_source(config, POLICY).is_err());
    for value in [1, 60] {
        let mut config = source("https://source.invalid/live");
        config.connect_timeout_secs = value;
        assert!(validate_source(config, POLICY).is_ok());
    }
    for value in [1, 300] {
        let mut config = source("https://source.invalid/live");
        config.read_timeout_secs = value;
        config.reconnect_max_secs = 300;
        assert!(validate_source(config, POLICY).is_ok());

        let mut config = source("https://source.invalid/live");
        config.reconnect_initial_secs = value;
        config.reconnect_max_secs = 300;
        assert!(validate_source(config, POLICY).is_ok());

        let mut config = source("https://source.invalid/live");
        config.reconnect_max_secs = value;
        config.reconnect_initial_secs = value.min(30);
        assert!(validate_source(config, POLICY).is_ok());
    }
}

#[test]
fn pull206_source_config_contract_map_limits_duplicates_names_and_urls() {
    let parsed = parse_source_map(
        Some(r#"{"alpha":{"url":"https://SOURCE.invalid:443/live"}}"#),
        POLICY,
    )
    .unwrap();
    assert_eq!(parsed.len(), 1);
    let normalized = validate_source(source("https://source.invalid/live"), POLICY).unwrap();
    assert_eq!(parsed.get("alpha"), Some(&normalized));

    assert!(parse_source_map(None, POLICY).unwrap().is_empty());
    for invalid in [
        "",
        "not-json",
        r#"{"alpha":{"url":"https://source.invalid"},"alpha":{"url":"https://other.invalid"}}"#,
        r#"{".":{"url":"https://source.invalid"}}"#,
        r#"{"..":{"url":"https://source.invalid"}}"#,
        r#"{"bad name":{"url":"https://source.invalid"}}"#,
        r#"{"alpha":{"url":"file:///tmp/source"}}"#,
        r#"{"alpha":{"url":""}}"#,
        r#"{"alpha":{"url":"http:///live"}}"#,
        r#"{"alpha":{"url":"https://user:pass@source.invalid/"}}"#,
        r#"{"alpha":{"url":"https://source.invalid/#fragment"}}"#,
        r#"{"alpha":{"url":"https://source.invalid:0/live"}}"#,
        r#"{"alpha":{"url":"http://169.254.1.2/live"}}"#,
    ] {
        assert!(
            parse_source_map(Some(invalid), POLICY).is_err(),
            "accepted {invalid}"
        );
    }
    assert!(validate_source(
        source("http://127.0.0.1/live"),
        FetchPolicy {
            allow_non_global: false
        }
    )
    .is_err());
    assert!(validate_source(source("http://127.0.0.1/live"), POLICY).is_ok());

    let many = (0..65)
        .map(|index| format!(r#""n{index}":{{"url":"https://source.invalid/{index}"}}"#))
        .collect::<Vec<_>>()
        .join(",");
    assert!(parse_source_map(Some(&format!("{{{many}}}")), POLICY).is_err());
    assert!(parse_source_map(Some(&format!("\"{}\"", "x".repeat(256 * 1024))), POLICY).is_err());

    let mut overlong = source(&format!("https://source.invalid/{}", "x".repeat(8192)));
    assert!(validate_source(overlong.clone(), POLICY).is_err());
    overlong.url = "https://source.invalid/live".into();
    assert!(validate_source(overlong, POLICY).is_ok());
}

#[test]
fn pull206_source_config_contract_redacts_url_and_sanitizes_errors() {
    let marker_url = "https://source.invalid/live?token=SYNTHETIC_PRIVATE_MARKER";
    let config = source(marker_url);
    let debug = format!("{config:?}");
    assert!(!debug.contains("SYNTHETIC_PRIVATE_MARKER"));
    assert!(!debug.contains("source.invalid"));

    let error = validate_source(source("https://user:password@source.invalid/x"), POLICY)
        .unwrap_err()
        .to_string();
    assert!(!error.contains("password"));
    assert!(!error.contains("source.invalid"));
    assert!(!error.contains("SYNTHETIC_PRIVATE_MARKER"));
}

#[test]
fn pull206_source_config_contract_non_global_opt_in_is_exactly_one_without_env_mutation() {
    assert!(!parse_non_global_source_opt_in(None));
    assert!(!parse_non_global_source_opt_in(Some("")));
    assert!(!parse_non_global_source_opt_in(Some("0")));
    assert!(!parse_non_global_source_opt_in(Some("true")));
    assert!(!parse_non_global_source_opt_in(Some(" 1")));
    assert!(parse_non_global_source_opt_in(Some("1")));
}

#[test]
fn pull206_source_status_contract_has_fixed_states_codes_and_four_fields() {
    let status = SourceStatus {
        state: serde_json::from_value(json!("backoff")).unwrap(),
        attempts: u64::MAX,
        last_error: Some(serde_json::from_value(json!("upstream_eof")).unwrap()),
        retry_in_ms: Some(321),
    };
    let value = serde_json::to_value(status).unwrap();
    assert_eq!(
        value,
        json!({
            "state": "backoff",
            "attempts": u64::MAX,
            "last_error": "upstream_eof",
            "retry_in_ms": 321,
        })
    );
    assert_eq!(value.as_object().unwrap().len(), 4);

    for state in [
        "connecting",
        "streaming",
        "backoff",
        "blocked",
        "failed",
        "stopping",
    ] {
        assert!(serde_json::from_value::<SourceState>(json!(state)).is_ok());
    }
    for error in [
        "dns_timeout",
        "dns_failed",
        "address_blocked",
        "connect_timeout",
        "connect_failed",
        "tls_failed",
        "headers_timeout",
        "redirect_rejected",
        "http_status",
        "unsupported_response",
        "body_failed",
        "body_frame_too_large",
        "read_timeout",
        "progress_timeout",
        "upstream_eof",
        "persistence_failed",
        "piece_index_exhausted",
        "ingest_failed",
        "worker_failed",
    ] {
        assert!(
            serde_json::from_value::<SourceErrorCode>(json!(error)).is_ok(),
            "{error}"
        );
    }
    assert!(serde_json::from_value::<SourceState>(json!("unknown")).is_err());
    assert!(serde_json::from_value::<SourceErrorCode>(json!("upstream reason text")).is_err());
}

#[tokio::test(start_paused = true)]
async fn pull206_retry_progress_and_cancel() {
    let policy = POLICY;
    let registry = BroadcastRegistry::new();
    let open_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let open_count_hook = open_count.clone();
    let open: TestSourceOpen = Arc::new(move |_, _, _, _| {
        let count = open_count_hook.clone();
        Box::pin(async move {
            count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(Box::new(EndBody) as Box<dyn TestSourceBody>)
        })
    });
    let bounds = Arc::new(Mutex::new(Vec::new()));
    let bounds_hook = bounds.clone();
    let jitter = Arc::new(move |low: u64, high: u64| {
        bounds_hook.lock().unwrap().push((low, high));
        low
    });
    let (status_tx, mut status_rx) = mpsc::unbounded_channel();
    registry.set_source_test_hooks(SourceTestHooks {
        open: Some(open),
        jitter: Some(jitter),
        on_status: Some(Arc::new(move |status| {
            let _ = status_tx.send(status);
        })),
        ..SourceTestHooks::default()
    });
    let state = test_broadcast_state(registry, policy);
    let config = BroadcastSourceConfig {
        url: "http://127.0.0.1:1/retry".into(),
        reconnect_initial_secs: 1,
        reconnect_max_secs: 3,
        ..source("http://127.0.0.1:1/retry")
    };

    attach_test_source(&state, "retry-state", config, policy).await;
    let first = wait_status(&mut status_rx, |status| {
        status.state == SourceState::Backoff
    })
    .await;
    assert_eq!(first.attempts, 1);
    assert_eq!(first.last_error, Some(SourceErrorCode::UpstreamEof));
    assert!(first
        .retry_in_ms
        .is_some_and(|left| (1..=900).contains(&left)));
    assert_eq!(*bounds.lock().unwrap(), vec![(900, 1_000)]);

    tokio::time::advance(Duration::from_millis(899)).await;
    tokio::task::yield_now().await;
    assert_eq!(open_count.load(std::sync::atomic::Ordering::SeqCst), 1);
    tokio::time::advance(Duration::from_millis(1)).await;
    let second = wait_status(&mut status_rx, |status| {
        status.state == SourceState::Backoff && status.attempts == 2
    })
    .await;
    assert_eq!(second.last_error, Some(SourceErrorCode::UpstreamEof));
    assert_eq!(*bounds.lock().unwrap(), vec![(900, 1_000), (1_800, 2_000)]);

    tokio::time::advance(Duration::from_millis(1_800)).await;
    let third = wait_status(&mut status_rx, |status| {
        status.state == SourceState::Backoff && status.attempts == 3
    })
    .await;
    assert_eq!(third.retry_in_ms, Some(2_700));
    assert_eq!(
        *bounds.lock().unwrap(),
        vec![(900, 1_000), (1_800, 2_000), (2_700, 3_000)]
    );

    state.detach_source("retry-state").await.unwrap();
    assert!(state.source_status("retry-state").await.unwrap().is_none());

    let read_registry = BroadcastRegistry::new();
    let (read_opened_tx, mut read_opened_rx) = mpsc::unbounded_channel();
    let read_open: TestSourceOpen = Arc::new(move |_, _, _, _| {
        let opened = read_opened_tx.clone();
        Box::pin(async move {
            let _ = opened.send(());
            Ok(Box::new(PendingBody) as Box<dyn TestSourceBody>)
        })
    });
    read_registry.set_source_test_hooks(SourceTestHooks {
        open: Some(read_open),
        ..SourceTestHooks::default()
    });
    let read_state = test_broadcast_state(read_registry, policy);
    attach_test_source(
        &read_state,
        "read-cancel",
        source("http://127.0.0.1:1/read-cancel"),
        policy,
    )
    .await;
    tokio::time::timeout(Duration::from_secs(2), read_opened_rx.recv())
        .await
        .expect("worker should enter the pending body read")
        .expect("open notification should arrive");
    read_state.detach_source("read-cancel").await.unwrap();
    assert!(read_state
        .source_status("read-cancel")
        .await
        .unwrap()
        .is_none());
}

#[tokio::test(start_paused = true)]
async fn pull206_retry_progress_separates_empty_junk_and_aligned_ts() {
    let policy = POLICY;
    let registry = BroadcastRegistry::new();
    let open_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let open_count_hook = open_count.clone();
    let (frames_tx, frames_rx) = mpsc::unbounded_channel();
    let frames_rx = Arc::new(Mutex::new(Some(frames_rx)));
    let frames_rx_hook = frames_rx.clone();
    let open: TestSourceOpen = Arc::new(move |_, _, _, _| {
        let count = open_count_hook.clone();
        let receiver = frames_rx_hook.clone();
        Box::pin(async move {
            match count.fetch_add(1, std::sync::atomic::Ordering::SeqCst) {
                0 => Ok(
                    Box::new(StepsBody(VecDeque::from([Step::Empty, Step::Stall])))
                        as Box<dyn TestSourceBody>,
                ),
                1 => Ok(
                    Box::new(ChannelBody(receiver.lock().unwrap().take().unwrap()))
                        as Box<dyn TestSourceBody>,
                ),
                _ => Ok(Box::new(PendingBody) as Box<dyn TestSourceBody>),
            }
        })
    });
    let (status_tx, mut status_rx) = mpsc::unbounded_channel();
    let (feed_tx, mut feed_rx) = mpsc::unbounded_channel();
    let jitter = Arc::new(|low: u64, _high: u64| low);
    registry.set_source_test_hooks(SourceTestHooks {
        open: Some(open),
        jitter: Some(jitter),
        on_status: Some(Arc::new(move |status| {
            let _ = status_tx.send(status);
        })),
        on_ingest: Some(Arc::new(move |progress| {
            let _ = feed_tx.send(progress);
        })),
        ..SourceTestHooks::default()
    });
    let state = test_broadcast_state(registry, policy);
    let mut config = source("http://127.0.0.1:1/progress");
    // Keep idle below the fixed progress deadline so the initial stalled attempt can prove
    // the body-idle classification while leaving room for scheduling the next body.
    config.read_timeout_secs = 29;
    attach_test_source(&state, "progress-state", config, policy).await;

    // The scripted empty read does not reset idle; the following blocked read times out.
    tokio::time::advance(Duration::from_secs(29)).await;
    let idle = wait_status(&mut status_rx, |status| {
        status.state == SourceState::Backoff
    })
    .await;
    assert_eq!(idle.last_error, Some(SourceErrorCode::ReadTimeout));
    tokio::time::advance(Duration::from_millis(900)).await;
    let connecting = wait_status(&mut status_rx, |status| {
        status.state == SourceState::Connecting && status.attempts == 2
    })
    .await;
    assert_eq!(connecting.last_error, Some(SourceErrorCode::ReadTimeout));

    let junk = Bytes::from(vec![0x11; 188]);
    let first_junk = send_test_frame(&frames_tx, &mut feed_rx, junk.clone()).await;
    assert_eq!(first_junk.aligned_bytes, 0);
    for _ in 1..59 {
        tokio::time::advance(Duration::from_millis(500)).await;
        let progress = send_test_frame(&frames_tx, &mut feed_rx, junk.clone()).await;
        assert_eq!(progress.aligned_bytes, 0);
    }
    let still_no_ts = state
        .source_status("progress-state")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(still_no_ts.state, SourceState::Connecting);
    assert_eq!(still_no_ts.last_error, Some(SourceErrorCode::ReadTimeout));

    let aligned = ts_packets(8, 0x5a);
    let aligned_progress = send_test_frame(&frames_tx, &mut feed_rx, aligned).await;
    assert!(aligned_progress.aligned_bytes > 0);
    let streaming = wait_status(&mut status_rx, |status| {
        status.state == SourceState::Streaming
    })
    .await;
    assert_eq!(streaming.last_error, None);

    tokio::time::advance(Duration::from_millis(500)).await;
    tokio::task::yield_now().await;
    assert_eq!(
        state
            .source_status("progress-state")
            .await
            .unwrap()
            .unwrap()
            .state,
        SourceState::Streaming,
        "aligned TS progress reset the 30-second deadline past the original one"
    );
    for index in 0..58 {
        tokio::time::advance(Duration::from_millis(500)).await;
        let progress = send_test_frame(&frames_tx, &mut feed_rx, junk.clone()).await;
        if index == 0 {
            // Resync may emit its final carried packet when it detects the boundary loss.
            assert!(progress.aligned_bytes <= 188);
        } else {
            assert_eq!(progress.aligned_bytes, 0);
        }
    }
    tokio::time::advance(Duration::from_millis(500)).await;
    tokio::time::advance(Duration::from_millis(500)).await;
    tokio::time::advance(Duration::from_millis(500)).await;
    let timed_out = wait_status(&mut status_rx, |status| {
        status.state == SourceState::Backoff
            && status.last_error == Some(SourceErrorCode::ProgressTimeout)
    })
    .await;
    assert_eq!(timed_out.attempts, 2);
    state.detach_source("progress-state").await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn pull206_healthy_piece_resets_retry_exponent_and_detach_cancels_ingest_gate() {
    let registry = BroadcastRegistry::new();
    let open_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let open_count_hook = open_count.clone();
    let (frames_tx, frames_rx) = mpsc::unbounded_channel();
    let frames_rx = Arc::new(Mutex::new(Some(frames_rx)));
    let frames_rx_hook = frames_rx.clone();
    let open: TestSourceOpen = Arc::new(move |_, _, _, _| {
        let count = open_count_hook.clone();
        let receiver = frames_rx_hook.clone();
        Box::pin(async move {
            match count.fetch_add(1, std::sync::atomic::Ordering::SeqCst) {
                0 => Ok(Box::new(EndBody) as Box<dyn TestSourceBody>),
                _ => Ok(
                    Box::new(ChannelBody(receiver.lock().unwrap().take().unwrap()))
                        as Box<dyn TestSourceBody>,
                ),
            }
        })
    });
    let jitter_bounds = Arc::new(Mutex::new(Vec::new()));
    let jitter_bounds_hook = jitter_bounds.clone();
    let jitter = Arc::new(move |low: u64, high: u64| {
        jitter_bounds_hook.lock().unwrap().push((low, high));
        low
    });
    let (progress_tx, mut progress_rx) = mpsc::unbounded_channel();
    registry.set_source_test_hooks(SourceTestHooks {
        open: Some(open),
        jitter: Some(jitter),
        on_ingest: Some(Arc::new(move |progress| {
            let _ = progress_tx.send(progress);
        })),
        ..SourceTestHooks::default()
    });
    let state = test_broadcast_state(registry, POLICY);
    let config = BroadcastSourceConfig {
        reconnect_initial_secs: 1,
        reconnect_max_secs: 8,
        ..source("http://127.0.0.1:1/healthy-reset")
    };
    let (broadcast, _) = attach_test_source(&state, "healthy-reset", config, POLICY).await;

    wait_status_with_state(&state, "healthy-reset", |status| {
        status.state == SourceState::Backoff && status.attempts == 1
    })
    .await;
    tokio::time::advance(Duration::from_secs(1)).await;
    wait_status_with_state(&state, "healthy-reset", |status| {
        status.state == SourceState::Connecting && status.attempts == 2
    })
    .await;

    let packets_for_piece = (65_536usize - broadcast.auth.signature_len()).div_ceil(188) + 8;
    let first = ts_packets(packets_for_piece, 0x61);
    frames_tx.send(Ok(Some(first))).unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if progress_rx.recv().await.unwrap().committed_pieces > 0 {
                break;
            }
        }
    })
    .await
    .expect("the ordinary ingest path must commit a complete signed piece");

    for _ in 0..2 {
        tokio::time::advance(Duration::from_secs(10)).await;
        frames_tx.send(Ok(Some(ts_packets(8, 0x62)))).unwrap();
        tokio::time::timeout(Duration::from_secs(3), progress_rx.recv())
            .await
            .expect("worker should ingest periodic aligned TS")
            .expect("real ingest observer should stay open");
    }
    tokio::time::advance(Duration::from_secs(10)).await;
    tokio::task::yield_now().await;
    frames_tx.send(Ok(None)).unwrap();
    let reset_backoff = wait_status_with_state(&state, "healthy-reset", |status| {
        status.state == SourceState::Backoff && status.attempts == 2
    })
    .await;
    assert_eq!(reset_backoff.last_error, Some(SourceErrorCode::UpstreamEof));
    assert_eq!(reset_backoff.retry_in_ms, Some(900));
    assert_eq!(
        *jitter_bounds.lock().unwrap(),
        vec![(900, 1_000), (900, 1_000)],
        "a complete committed piece plus 30 streaming seconds resets retry exponent"
    );
    state.detach_source("healthy-reset").await.unwrap();

    // Cancellation also wins while a real ingest is held at its narrow pre-push test gate.
    let gate_registry = BroadcastRegistry::new();
    let (entered_tx, mut entered_rx) = mpsc::unbounded_channel();
    let before_ingest = Arc::new(move |_| {
        let entered = entered_tx.clone();
        Box::pin(async move {
            let _ = entered.send(());
            std::future::pending().await
        })
            as futures::future::BoxFuture<'static, Result<(), crate::broadcast_ingest::IngestError>>
    });
    let (gate_frames_tx, gate_frames_rx) = mpsc::unbounded_channel();
    let gate_frames_rx = Arc::new(Mutex::new(Some(gate_frames_rx)));
    let gate_frames_rx_hook = gate_frames_rx.clone();
    let gate_open: TestSourceOpen = Arc::new(move |_, _, _, _| {
        let receiver = gate_frames_rx_hook.clone();
        Box::pin(async move {
            Ok(
                Box::new(ChannelBody(receiver.lock().unwrap().take().unwrap()))
                    as Box<dyn TestSourceBody>,
            )
        })
    });
    gate_registry.set_source_test_hooks(SourceTestHooks {
        open: Some(gate_open),
        before_ingest: Some(before_ingest),
        ..SourceTestHooks::default()
    });
    let gate_state = test_broadcast_state(gate_registry, POLICY);
    attach_test_source(
        &gate_state,
        "ingest-cancel",
        source("http://127.0.0.1:1/ingest-cancel"),
        POLICY,
    )
    .await;
    gate_frames_tx.send(Ok(Some(ts_packets(8, 0x63)))).unwrap();
    tokio::time::timeout(Duration::from_secs(2), entered_rx.recv())
        .await
        .expect("source worker should reach the actual ingest gate")
        .expect("ingest gate notification should be sent");
    gate_state.detach_source("ingest-cancel").await.unwrap();
    assert!(gate_state
        .source_status("ingest-cancel")
        .await
        .unwrap()
        .is_none());
}

#[tokio::test(start_paused = true)]
async fn pull206_local_ingest_timeout_is_terminal_and_retains_source_claim() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    let registry = BroadcastRegistry::new();
    let (frames_tx, frames_rx) = mpsc::unbounded_channel();
    let frames_rx = Arc::new(Mutex::new(Some(frames_rx)));
    let frames_rx_hook = frames_rx.clone();
    let opens = Arc::new(AtomicUsize::new(0));
    let opens_hook = opens.clone();
    let open: TestSourceOpen = Arc::new(move |_, _, _, _| {
        let receiver = frames_rx_hook.clone();
        let opens = opens_hook.clone();
        Box::pin(async move {
            opens.fetch_add(1, Ordering::SeqCst);
            Ok(
                Box::new(ChannelBody(receiver.lock().unwrap().take().unwrap()))
                    as Box<dyn TestSourceBody>,
            )
        })
    });
    let (entered_tx, mut entered_rx) = mpsc::unbounded_channel();
    let before_ingest = Arc::new(move |_| {
        let entered = entered_tx.clone();
        Box::pin(async move {
            let _ = entered.send(());
            std::future::pending().await
        })
            as futures::future::BoxFuture<'static, Result<(), crate::broadcast_ingest::IngestError>>
    });
    let logs = Arc::new(AtomicUsize::new(0));
    let logs_hook = logs.clone();
    registry.set_source_test_hooks(SourceTestHooks {
        open: Some(open),
        before_ingest: Some(before_ingest),
        on_log: Some(Arc::new(move |_| {
            logs_hook.fetch_add(1, Ordering::SeqCst);
        })),
        ..SourceTestHooks::default()
    });
    let state = test_broadcast_state(registry.clone(), POLICY);
    let config = source("http://127.0.0.1:1/ingest-timeout");
    let (broadcast, _) = attach_test_source(&state, "ingest-timeout", config.clone(), POLICY).await;
    frames_tx.send(Ok(Some(ts_packets(8, 0x41)))).unwrap();
    tokio::time::timeout(Duration::from_secs(2), entered_rx.recv())
        .await
        .expect("real worker should enter the guarded ingest call")
        .expect("ingest gate notification should arrive");

    tokio::time::advance(Duration::from_secs(15)).await;
    let failed = wait_status_with_state(&state, "ingest-timeout", |status| {
        status.state == SourceState::Failed
    })
    .await;
    assert_eq!(failed.attempts, 1);
    assert_eq!(failed.last_error, Some(SourceErrorCode::IngestFailed));
    assert_eq!(failed.retry_in_ms, None);
    assert_eq!(logs.load(Ordering::SeqCst), 1, "terminal failure logs once");

    let (same, same_status) = attach_test_source(&state, "ingest-timeout", config, POLICY).await;
    assert_eq!(same.infohash, broadcast.infohash);
    assert_eq!(same.content_id, broadcast.content_id);
    assert_eq!(
        same_status.attempts, 1,
        "identical PUT does not restart a failed worker"
    );
    assert_eq!(
        registry
            .acquire_producer(
                "ingest-timeout",
                &[],
                &state.seed_registry,
                state.store_bytes,
                ProducerKind::RawPut
            )
            .await
            .err()
            .unwrap(),
        BroadcastError::ProducerConflict
    );
    assert_eq!(
        registry
            .acquire_producer(
                "ingest-timeout",
                &[],
                &state.seed_registry,
                state.store_bytes,
                ProducerKind::Rtmp
            )
            .await
            .err()
            .unwrap(),
        BroadcastError::ProducerConflict
    );

    tokio::time::advance(Duration::from_secs(300)).await;
    tokio::task::yield_now().await;
    assert_eq!(
        opens.load(Ordering::SeqCst),
        1,
        "terminal worker must not auto-retry"
    );
    assert_eq!(logs.load(Ordering::SeqCst), 1);
    assert_eq!(
        state
            .source_status("ingest-timeout")
            .await
            .unwrap()
            .unwrap()
            .attempts,
        1
    );
    state.detach_source("ingest-timeout").await.unwrap();
    assert!(state
        .source_status("ingest-timeout")
        .await
        .unwrap()
        .is_none());
}

async fn wait_status_with_state(
    state: &BroadcastState,
    name: &str,
    predicate: impl Fn(&SourceStatus) -> bool,
) -> SourceStatus {
    for _ in 0..10_000 {
        let status = state.source_status(name).await.unwrap().unwrap();
        if predicate(&status) {
            return status;
        }
        tokio::task::yield_now().await;
    }
    panic!("source status did not reach the expected transition")
}
