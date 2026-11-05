use super::*;
use std::time::Duration;

fn directory() -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "outpace-support-runtime-{}-{}",
        std::process::id(),
        rand::random::<u64>()
    ))
}
fn config(path: &std::path::Path) -> Config {
    Config {
        bind: "127.0.0.1:0".parse().unwrap(),
        rtmp_bind: "127.0.0.1:0".parse().unwrap(),
        peer_listen: "127.0.0.1:0".parse().unwrap(),
        data_dir: path.into(),
        cache_dir: path.join("cache"),
        networks: Vec::new(),
        enable_inbound: false,
        enable_seeding: false,
        ..Default::default()
    }
}
#[tokio::test]
async fn rtmp_readiness_does_not_accept_a_failed_dependency_bind() {
    let path = directory();
    let runtime = build_runtime(config(&path), Vec::new()).await.unwrap();
    let occupied = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let result = OwnedRtmpListener::start_checked(
        occupied.local_addr().unwrap(),
        runtime.broadcasts.clone(),
    )
    .await;
    let refused = match result {
        Err(_) => true,
        Ok(mut listener) => {
            listener
                .stop(tokio::time::Instant::now() + Duration::from_secs(1))
                .await;
            false
        }
    };
    runtime.broadcasts.shutdown().await.unwrap();
    assert!(runtime.manager.shutdown().await);
    drop(runtime);
    drop(occupied);
    std::fs::remove_dir_all(path).unwrap();
    assert!(
        refused,
        "spawned RTMP task must not count as successful listener readiness"
    );
}

use crate::provider::{ProviderError, ProviderRegistry, SourceStats, StreamProvider, TsSource};
use async_trait::async_trait;
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Default)]
struct Probe {
    opens: AtomicUsize,
    closes: AtomicUsize,
    started: tokio::sync::Notify,
    cleaning: tokio::sync::Notify,
    release: tokio::sync::Notify,
    pause_cleanup: bool,
    fail_cleanup: bool,
}
struct ProbeSource(Arc<Probe>);
#[async_trait]
impl TsSource for ProbeSource {
    async fn next(&mut self) -> Option<bytes::Bytes> {
        self.0.started.notify_one();
        std::future::pending().await
    }
    async fn shutdown(&mut self) {
        self.0.cleaning.notify_one();
        if self.0.pause_cleanup {
            self.0.release.notified().await;
        }
        assert!(!self.0.fail_cleanup, "synthetic source cleanup failure");
        self.0.closes.fetch_add(1, Ordering::SeqCst);
    }
    fn stats(&self) -> SourceStats {
        SourceStats::default()
    }
}
struct SharedProbe(Arc<Probe>);
#[async_trait]
impl StreamProvider for SharedProbe {
    fn network(&self) -> &'static str {
        "ace"
    }
    async fn open(&self, _: &str) -> Result<Box<dyn TsSource>, ProviderError> {
        self.0.opens.fetch_add(1, Ordering::SeqCst);
        Ok(Box::new(ProbeSource(self.0.clone())))
    }
}
async fn declared_runtime(path: &std::path::Path, probe: Arc<Probe>) -> EngineRuntime {
    // Build a network-free runtime, then install the synthetic provider/declaration.
    let mut runtime = build_runtime(config(path), Vec::new()).await.unwrap();
    let mut providers = ProviderRegistry::new();
    providers.register(Arc::new(SharedProbe(probe)));
    runtime.manager = StreamManager::with_session_limit(providers, 1);
    runtime.config.support_sources =
        crate::support_config::SupportSources::parse(Some(r#"{"synthetic":"local:synthetic"}"#))
            .unwrap();
    runtime
}
#[tokio::test]
async fn declared_support_starts_without_viewers_and_joins_on_runtime_shutdown() {
    let path = directory();
    let probe = Arc::new(Probe::default());
    let runtime = declared_runtime(&path, probe.clone()).await;
    let (shutdown, receiver) = tokio::sync::oneshot::channel();
    let observe = async {
        let started = tokio::time::timeout(Duration::from_secs(1), probe.started.notified())
            .await
            .is_ok();
        shutdown.send(()).unwrap();
        started
    };
    let (result, started) = tokio::join!(
        serve_http_until(runtime, async {
            let _ = receiver.await;
        }),
        observe
    );
    std::fs::remove_dir_all(path).unwrap();
    assert!(result.is_ok(), "runtime returned {result:?}");
    assert!(
        started,
        "declared support must start without a playback request"
    );
    assert_eq!(probe.opens.load(Ordering::SeqCst), 1);
    assert_eq!(
        probe.closes.load(Ordering::SeqCst),
        1,
        "source cleanup must complete before runtime returns"
    );
}
#[tokio::test]
async fn failed_required_listener_bind_never_opens_declared_support() {
    for kind in ["http", "peer", "rtmp"] {
        let path = directory();
        let probe = Arc::new(Probe::default());
        let mut runtime = declared_runtime(&path, probe.clone()).await;
        let occupied = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = occupied.local_addr().unwrap();
        match kind {
            "http" => runtime.config.bind = address,
            "peer" => {
                runtime.config.enable_inbound = true;
                runtime.config.peer_listen = address;
            }
            _ => runtime.config.rtmp_bind = address,
        }
        let result = serve_http_until(runtime, std::future::pending()).await;
        std::fs::remove_dir_all(path).unwrap();
        assert!(result.is_err(), "{kind} bind must fail");
        assert_eq!(
            probe.opens.load(Ordering::SeqCst),
            0,
            "{kind} failure started source work"
        );
    }
}

#[tokio::test]
async fn runtime_shutdown_retains_admission_until_paused_support_cleanup_finishes() {
    let path = directory();
    let probe = Arc::new(Probe {
        pause_cleanup: true,
        ..Default::default()
    });
    let runtime = declared_runtime(&path, probe.clone()).await;
    let manager = runtime.manager.clone();
    let (shutdown, receiver) = tokio::sync::oneshot::channel();
    let mut serving = Box::pin(serve_http_until(runtime, async {
        let _ = receiver.await;
    }));
    let observe = async {
        tokio::time::timeout(Duration::from_secs(1), probe.started.notified())
            .await
            .unwrap();
        shutdown.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(1), probe.cleaning.notified())
            .await
            .unwrap();
    };
    tokio::select! {
        result = &mut serving => panic!("runtime ended before paused cleanup: {result:?}"),
        () = observe => {},
    }
    let pending = tokio::time::timeout(Duration::from_millis(20), &mut serving)
        .await
        .is_err();
    let held = manager.admission_status().0;
    let closes = probe.closes.load(Ordering::SeqCst);
    probe.release.notify_one();
    let result = tokio::time::timeout(Duration::from_secs(1), &mut serving)
        .await
        .unwrap();
    drop(serving);
    std::fs::remove_dir_all(path).unwrap();
    assert!(pending, "a canceled wait must not finish runtime cleanup");
    assert_eq!(held, 1);
    assert_eq!(closes, 0);
    assert!(result.is_ok());
    assert_eq!(probe.closes.load(Ordering::SeqCst), 1);
    assert_eq!(manager.admission_status().0, 0);
}
#[tokio::test]
async fn failed_support_cleanup_is_reported_and_manager_shutdown_still_runs() {
    let path = directory();
    let probe = Arc::new(Probe {
        fail_cleanup: true,
        ..Default::default()
    });
    let runtime = declared_runtime(&path, probe.clone()).await;
    let manager = runtime.manager.clone();
    let (shutdown, receiver) = tokio::sync::oneshot::channel();
    let observe = async {
        tokio::time::timeout(Duration::from_secs(1), probe.started.notified())
            .await
            .unwrap();
        shutdown.send(()).unwrap();
    };
    let (result, ()) = tokio::join!(
        serve_http_until(runtime, async {
            let _ = receiver.await;
        }),
        observe
    );
    std::fs::remove_dir_all(path).unwrap();
    assert_eq!(result.unwrap_err().to_string(), "support cleanup failed");
    assert_eq!(
        manager.admission_status().0,
        1,
        "uncertain work must retain capacity"
    );
    // Shutdown closes manager admissions even when source cleanup failed.
    assert!(manager.get_or_start("ace", "local:another").await.is_err());
    assert_eq!(probe.opens.load(Ordering::SeqCst), 1);
}
