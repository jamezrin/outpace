//! Graceful HTTP drain must close provider streams before waiting for their bodies.

use super::*;
use crate::provider::{ProviderError, SourceStats, StreamProvider, TsSource};
use async_trait::async_trait;
use bytes::Bytes;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{oneshot, Notify};
use tokio::time::{timeout, Duration};

struct PendingProvider {
    reading: Arc<Notify>,
    dropped: Arc<AtomicBool>,
}

struct PendingSource {
    reading: Arc<Notify>,
    dropped: Arc<AtomicBool>,
}

#[async_trait]
impl StreamProvider for PendingProvider {
    fn network(&self) -> &'static str {
        "shutdown-test"
    }

    async fn open(&self, _id: &str) -> Result<Box<dyn TsSource>, ProviderError> {
        Ok(Box::new(PendingSource {
            reading: self.reading.clone(),
            dropped: self.dropped.clone(),
        }))
    }
}

#[async_trait]
impl TsSource for PendingSource {
    async fn next(&mut self) -> Option<Bytes> {
        self.reading.notify_one();
        std::future::pending().await
    }

    fn stats(&self) -> SourceStats {
        SourceStats::default()
    }
}

impl Drop for PendingSource {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::SeqCst);
    }
}

struct TestDirectory(PathBuf);

impl Drop for TestDirectory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

async fn shutdown_with_open_stream(path: &str) {
    tokio::task::LocalSet::new()
        .run_until(async {
            let dir = TestDirectory(
                std::env::temp_dir()
                    .join(format!("outpace-http-shutdown-{}", rand::random::<u64>())),
            );
            let probe = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = probe.local_addr().unwrap();
            let mut runtime = build_runtime(
                Config {
                    bind: address,
                    rtmp_bind: "127.0.0.1:0".parse().unwrap(),
                    data_dir: dir.0.clone(),
                    networks: vec![],
                    enable_inbound: false,
                    seed_ttl_secs: 0,
                    experimental_ace_compat: true,
                    ..Config::default()
                },
                vec![],
            )
            .await
            .unwrap();
            // Replace the empty provider registry with a source that never reaches EOF.
            runtime.manager.shutdown().await;
            let reading = Arc::new(Notify::new());
            let dropped = Arc::new(AtomicBool::new(false));
            let mut providers = ProviderRegistry::new();
            providers.register(Arc::new(PendingProvider {
                reading: reading.clone(),
                dropped: dropped.clone(),
            }));
            let manager = StreamManager::new(providers);
            runtime.manager = manager.clone();
            runtime.networks = vec!["shutdown-test".into()];
            let (stop, stopped) = oneshot::channel();
            drop(probe);
            let serving = tokio::task::spawn_local(async move {
                serve_http_until(runtime, async move {
                    let _ = stopped.await;
                })
                .await
                .map_err(|error| error.to_string())
            });
            let mut client = timeout(Duration::from_secs(5), async {
                loop {
                    match TcpStream::connect(address).await {
                        Ok(client) => break client,
                        Err(error) if error.kind() == std::io::ErrorKind::ConnectionRefused => {
                            tokio::time::sleep(Duration::from_millis(5)).await;
                        }
                        Err(error) => panic!("local HTTP connection failed: {error}"),
                    }
                }
            })
            .await
            .expect("the runtime must start its HTTP listener");
            client
                .write_all(
                    format!("GET {path} HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n")
                        .as_bytes(),
                )
                .await
                .unwrap();
            timeout(Duration::from_secs(5), reading.notified())
                .await
                .expect("the response must subscribe and start the pending source");
            let active = manager.list().await;
            assert_eq!(active.len(), 1);
            assert!(!dropped.load(Ordering::SeqCst));

            // Keep the client socket and its unfinished response open: graceful drain must
            // not depend on the client disconnecting or the source reaching EOF.
            stop.send(()).unwrap();
            timeout(Duration::from_secs(5), serving)
                .await
                .expect("shutdown must close the provider before waiting for HTTP drain")
                .unwrap()
                .unwrap();
            let mut response = Vec::new();
            timeout(Duration::from_secs(2), client.read_to_end(&mut response))
                .await
                .expect("shutdown must finish the response without client disconnection")
                .unwrap();
            assert!(response.starts_with(b"HTTP/1.1 200 OK\r\n"));
            assert!(dropped.load(Ordering::SeqCst));
            assert!(manager.list().await.is_empty());
            // No rebind check: once released, the ephemeral port can be taken at once by a
            // parallel test. The completed serve and the client's EOF prove the drain finished.
        })
        .await;
}

#[tokio::test]
async fn shutdown_finishes_with_open_native_provider_response() {
    shutdown_with_open_stream("/streams/shutdown-test/pending.ts").await;
}

#[tokio::test]
async fn shutdown_finishes_with_open_compatibility_provider_response() {
    shutdown_with_open_stream("/ace/getstream?infohash=0123456789abcdef0123456789abcdef01234567")
        .await;
}
