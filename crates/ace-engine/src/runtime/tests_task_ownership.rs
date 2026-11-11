//! The runtime owns its maintenance and inbound-listener tasks through every exit path.

use super::*;
use std::path::PathBuf;
use tokio::io::AsyncReadExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{oneshot, Notify};
use tokio::time::{timeout, Duration};

struct TestDirectory(PathBuf);

impl TestDirectory {
    fn new(label: &str) -> Self {
        Self(std::env::temp_dir().join(format!("outpace-{label}-{}", rand::random::<u64>())))
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn inbound_config(dir: &TestDirectory, bind: SocketAddr, peer_listen: SocketAddr) -> Config {
    Config {
        bind,
        peer_listen,
        rtmp_bind: "127.0.0.1:0".parse().unwrap(),
        data_dir: dir.0.clone(),
        networks: vec![],
        enable_inbound: true,
        ..Config::default()
    }
}

async fn wait_until(what: &str, mut done: impl FnMut() -> bool) {
    timeout(Duration::from_secs(2), async {
        while !done() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{what}"));
}

#[tokio::test]
async fn occupied_http_bind_releases_inbound_listener() {
    let dir = TestDirectory::new("inbound-bind");
    let occupied_http = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let peer_probe = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let peer_address = peer_probe.local_addr().unwrap();
    let mut config = inbound_config(&dir, occupied_http.local_addr().unwrap(), peer_address);
    config.seed_ttl_secs = 0;
    let runtime = build_runtime(config, vec![]).await.unwrap();
    drop(peer_probe);
    let result = timeout(
        Duration::from_secs(5),
        serve_http_until(runtime, std::future::pending()),
    )
    .await
    .expect("an occupied HTTP bind must fail promptly");
    assert!(result.is_err());
    assert!(
        TcpListener::bind(peer_address).await.is_ok(),
        "HTTP startup failure must release the already-bound inbound listener"
    );
}

#[tokio::test]
async fn dropping_prebuilt_runtime_cancels_maintenance_tasks() {
    let dir = TestDirectory::new("runtime-drop");
    let runtime = build_runtime(
        Config {
            data_dir: dir.0.clone(),
            networks: vec![],
            enable_inbound: true,
            ..Config::default()
        },
        vec![],
    )
    .await
    .unwrap();
    let maintenance: Vec<_> = runtime
        .background
        .tasks
        .iter()
        .map(|task| task.abort_handle())
        .collect();
    assert_eq!(maintenance.len(), 2, "seed reaper and rechoke");
    tokio::task::yield_now().await;
    assert!(maintenance.iter().all(|task| !task.is_finished()));
    drop(runtime);
    wait_until(
        "dropping an unserved runtime must cancel its maintenance tasks",
        || maintenance.iter().all(|task| task.is_finished()),
    )
    .await;
}

#[tokio::test]
async fn cancelled_background_join_keeps_abort_ownership() {
    let mut background = RuntimeTasks::default();
    let entered = Arc::new(Notify::new());
    let stopped = entered.clone();
    background.spawn_until(move |mut cancel| async move {
        let _ = cancel.changed().await;
        stopped.notify_one();
        std::future::pending::<()>().await;
    });
    let task = background.tasks[0].abort_handle();
    let mut shutdown = Box::pin(background.shutdown());
    assert!(futures::poll!(shutdown.as_mut()).is_pending());
    timeout(Duration::from_secs(2), entered.notified())
        .await
        .unwrap();
    drop(shutdown);
    assert!(!task.is_finished());
    drop(background);
    wait_until(
        "a cancelled join must not detach its unfinished task",
        || task.is_finished(),
    )
    .await;
}

async fn finish_with_idle_inbound_connection(cancel_serving: bool) {
    tokio::task::LocalSet::new()
        .run_until(async {
            let dir = TestDirectory::new("inbound-stop");
            let http_probe = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let peer_probe = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let http_address = http_probe.local_addr().unwrap();
            let peer_address = peer_probe.local_addr().unwrap();
            let runtime = build_runtime(inbound_config(&dir, http_address, peer_address), vec![])
                .await
                .unwrap();
            let monitor = runtime.reachability.clone().unwrap();
            let weak_manager = Arc::downgrade(&runtime.manager);
            let maintenance: Vec<_> = runtime
                .background
                .tasks
                .iter()
                .map(|task| task.abort_handle())
                .collect();
            let (stop, stopped) = oneshot::channel();
            drop((http_probe, peer_probe));
            let serving = tokio::task::spawn_local(async move {
                serve_http_until(runtime, async move {
                    let _ = stopped.await;
                })
                .await
                .map_err(|error| error.to_string())
            });
            let mut peer = timeout(Duration::from_secs(5), async {
                loop {
                    match TcpStream::connect(peer_address).await {
                        Ok(peer) => break peer,
                        Err(error) if error.kind() == std::io::ErrorKind::ConnectionRefused => {
                            tokio::time::sleep(Duration::from_millis(5)).await;
                        }
                        Err(error) => panic!("local peer connection failed: {error}"),
                    }
                }
            })
            .await
            .expect("the inbound listener must start");
            // No protocol bytes: the accepted socket idles in its handshake read.
            wait_until("the listener must accept the idle peer", || {
                monitor.inbound_peers() != 0
            })
            .await;
            if cancel_serving {
                serving.abort();
                assert!(serving.await.unwrap_err().is_cancelled());
            } else {
                stop.send(()).unwrap();
                timeout(Duration::from_secs(5), serving)
                    .await
                    .expect("graceful shutdown must join inbound tasks")
                    .unwrap()
                    .unwrap();
            }
            let mut byte = [0];
            assert_eq!(
                timeout(Duration::from_secs(2), peer.read(&mut byte))
                    .await
                    .expect("the accepted idle socket must be closed")
                    .unwrap(),
                0
            );
            assert!(TcpListener::bind(peer_address).await.is_ok());
            assert!(TcpListener::bind(http_address).await.is_ok());
            wait_until("all runtime maintenance tasks must finish", || {
                maintenance.iter().all(|task| task.is_finished())
            })
            .await;
            wait_until("serving must release the manager after it ends", || {
                weak_manager.upgrade().is_none()
            })
            .await;
        })
        .await;
}

#[tokio::test]
async fn graceful_shutdown_closes_inbound_listener_and_idle_connection() {
    finish_with_idle_inbound_connection(false).await;
}

#[tokio::test]
async fn cancelled_serving_closes_inbound_listener_and_idle_connection() {
    finish_with_idle_inbound_connection(true).await;
}
