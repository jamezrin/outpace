//! Bounded native socket owner, with joins retained independently of async waiters.

use super::{
    config::PreparedConfig,
    native::{NativeLibrary, NativeRetirement, NativeSocket, ReadStatus},
};
use crate::manager::{CleanupTracker, WorkerControl};
use bytes::Bytes;
use std::{
    net::SocketAddr,
    sync::{
        atomic::{AtomicBool, AtomicU8, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use tokio::sync::{mpsc, oneshot, watch};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum OwnerError {
    Runtime,
    Policy,
    Spawn,
    Native,
    Join,
    Retirement,
}

// Monotone pending -> active -> retiring, independent of media channel capacity.
const PENDING: u8 = 0;
const ACTIVE: u8 = 1;
const RETIRING: u8 = 2;
pub(crate) struct NativeIncoming {
    pub(crate) name: String,
    pub(crate) receiver: mpsc::Receiver<Bytes>,
    pub(crate) retirement: watch::Receiver<bool>,
    phase: Arc<AtomicU8>,
}
impl NativeIncoming {
    pub(crate) fn admit(&self) -> bool {
        self.phase
            .compare_exchange(PENDING, ACTIVE, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }
}
impl Drop for NativeIncoming {
    fn drop(&mut self) {
        self.phase.store(RETIRING, Ordering::Release);
    }
}
struct Connection<'a> {
    socket: Option<NativeSocket<'a>>,
    retirement: Option<NativeRetirement<'a>>,
    phase: Arc<AtomicU8>,
    sender: mpsc::Sender<Bytes>,
    retired: watch::Sender<bool>,
}
impl Connection<'_> {
    fn turn(&mut self, scratch: &mut [u8]) -> Result<bool, OwnerError> {
        if self.sender.is_closed() {
            self.phase.store(RETIRING, Ordering::Release);
        }
        match self.phase.load(Ordering::Acquire) {
            ACTIVE => {
                // Reserve before reading or allocating. A full consumer never blocks
                // cancellation, another publisher, or the native protocol worker.
                if let Ok(permit) = self.sender.try_reserve() {
                    match self
                        .socket
                        .as_ref()
                        .ok_or(OwnerError::Native)?
                        .receive(scratch)
                    {
                        Ok(ReadStatus::Data(length)) => {
                            permit.send(Bytes::copy_from_slice(&scratch[..length]));
                            return Ok(true);
                        }
                        Ok(ReadStatus::WouldBlock) => {}
                        _ => {
                            self.phase.store(RETIRING, Ordering::Release);
                        }
                    }
                }
            }
            RETIRING => {
                if let Some(mut socket) = self.socket.take() {
                    self.retirement = Some(socket.begin_close().map_err(|_| OwnerError::Native)?);
                }
            }
            _ => {}
        }
        Ok(false)
    }
    fn collected(&self) -> bool {
        self.retirement
            .as_ref()
            .is_some_and(NativeRetirement::confirmed)
    }
}

pub(crate) struct NativeOwner {
    address: SocketAddr,
    pub(crate) incoming: mpsc::Receiver<NativeIncoming>,
    control: Option<WorkerControl>,
    stop: Arc<AtomicBool>,
    completion: watch::Receiver<bool>,
    outcome: watch::Receiver<Option<Result<(), OwnerError>>>,
    _cleanup: CleanupTracker,
}
impl NativeOwner {
    pub(crate) async fn start(config: PreparedConfig) -> Result<Self, OwnerError> {
        tokio::runtime::Handle::try_current().map_err(|_| OwnerError::Runtime)?;
        if !(1..=16).contains(&config.max_connections) || !(20..=2000).contains(&config.latency_ms)
        {
            return Err(OwnerError::Policy);
        }
        let stop = Arc::new(AtomicBool::new(false));
        let native_stop = stop.clone();
        let (ready, received) = oneshot::channel();
        let address = config.bind;
        let (incoming_sender, incoming) = mpsc::channel(config.max_connections);
        let thread = std::thread::Builder::new()
            .name("outpace-srt".into())
            .spawn(move || run_listener(config, native_stop, ready, incoming_sender))
            .map_err(|_| OwnerError::Spawn)?;
        // No suspension occurs between spawning the thread and registering its join.
        let cleanup = CleanupTracker::default();
        let (published, outcome) = watch::channel(None);
        let cleanup_stop = stop.clone();
        let mut control = cleanup.spawn(move |mut cancel| async move {
            let mut joined = tokio::task::spawn_blocking(move || thread.join());
            let result = tokio::select! {
                result = &mut joined => result,
                _ = cancel.changed() => {
                    cleanup_stop.store(true, Ordering::Release);
                    joined.await
                }
            };
            let result = match result {
                Ok(Ok(result)) => result,
                _ => Err(OwnerError::Join),
            };
            published.send_replace(Some(result));
        });
        let completion = control.completion_receiver();
        control.transfer();
        let mut owner = Self {
            address,
            incoming,
            control: Some(control),
            stop,
            completion,
            outcome,
            _cleanup: cleanup,
        };
        match received.await {
            Ok(Ok(address)) => {
                owner.address = address;
                Ok(owner)
            }
            _ => {
                let _ = owner.shutdown().await;
                Err(OwnerError::Native)
            }
        }
    }
    pub(super) fn stop_signal(&self) -> Arc<AtomicBool> {
        self.stop.clone()
    }
    pub(crate) fn local_addr(&self) -> SocketAddr {
        self.address
    }
    pub(crate) async fn shutdown(&mut self) -> Result<(), OwnerError> {
        self.stop.store(true, Ordering::Release);
        drop(self.control.take());
        if !crate::manager::wait_cleanup(&mut self.completion).await {
            return Err(OwnerError::Join);
        }
        (*self.outcome.borrow()).unwrap_or(Err(OwnerError::Join))
    }
}

impl Drop for NativeOwner {
    fn drop(&mut self) {
        // The native owner must terminate even if Tokio never polls a cancellation
        // watch again. WorkerControl still retains the exact thread-join lifetime.
        self.stop.store(true, Ordering::Release);
    }
}

fn run_listener(
    config: PreparedConfig,
    stop: Arc<AtomicBool>,
    ready: oneshot::Sender<Result<SocketAddr, OwnerError>>,
    incoming: mpsc::Sender<NativeIncoming>,
) -> Result<(), OwnerError> {
    let library = NativeLibrary::acquire().map_err(|_| OwnerError::Native)?;
    let result = (|| {
        let mut listener = library
            .receiver(config.latency_ms as i32, config.passphrase.as_bytes())
            .map_err(|_| OwnerError::Native)?;
        let address = listener
            .listen(config.bind, config.max_connections)
            .map_err(|_| OwnerError::Native)?;
        if ready.send(Ok(address)).is_err() {
            stop.store(true, Ordering::Release);
        }
        let mut connections: Vec<Connection<'_>> = Vec::with_capacity(config.max_connections);
        let mut scratch = [0u8; 65536];
        let mut listener = Some(listener);
        let mut listener_retirement = None;
        let mut deadline = None;
        loop {
            if incoming.is_closed() {
                stop.store(true, Ordering::Release);
            }
            let stopping = stop.load(Ordering::Acquire);
            if stopping && deadline.is_none() {
                deadline = Some(Instant::now() + Duration::from_secs(10));
                if let Some(mut socket) = listener.take() {
                    listener_retirement =
                        Some(socket.begin_close().map_err(|_| OwnerError::Native)?);
                }
                for connection in &connections {
                    connection.phase.store(RETIRING, Ordering::Release);
                }
            }
            let mut progress = false;
            if !stopping && connections.len() < config.max_connections {
                // The SDK listen backlog is separately bounded. Never accept into
                // an overflow retirement list when application admission is full.
                if let Some(socket) = listener
                    .as_ref()
                    .ok_or(OwnerError::Native)?
                    .accept()
                    .map_err(|_| OwnerError::Native)?
                {
                    let (sender, receiver) = mpsc::channel(4);
                    let (retired, retirement) = watch::channel(false);
                    let phase = Arc::new(AtomicU8::new(PENDING));
                    match socket.stream_id() {
                        Ok(name) => {
                            let offered = NativeIncoming {
                                name,
                                receiver,
                                retirement,
                                phase: phase.clone(),
                            };
                            // Failure drops the offer and monotonically requests retirement.
                            let _ = incoming.try_send(offered);
                        }
                        Err(_) => {
                            phase.store(RETIRING, Ordering::Release);
                        }
                    }
                    connections.push(Connection {
                        socket: Some(socket),
                        retirement: None,
                        phase,
                        sender,
                        retired,
                    });
                    progress = true;
                }
            }
            // One receive per admitted connection per turn, then recheck control.
            for connection in &mut connections {
                progress |= connection.turn(&mut scratch)?;
            }
            connections.retain(|connection| {
                if connection.collected() {
                    connection.retired.send_replace(true);
                    false
                } else {
                    true
                }
            });
            if stopping
                && connections.is_empty()
                && listener_retirement
                    .as_ref()
                    .is_some_and(NativeRetirement::confirmed)
            {
                break;
            }
            if deadline.is_some_and(|until| Instant::now() >= until) {
                return Err(OwnerError::Retirement);
            }
            if progress {
                std::thread::yield_now();
            } else {
                std::thread::sleep(Duration::from_millis(5));
            }
        }
        Ok(())
    })();
    let cleaned = library.shutdown().map_err(|_| OwnerError::Native);
    result.and(cleaned)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::srt::config::SrtConfig;
    use std::{path::PathBuf, time::Duration};

    struct KeyFixture(PathBuf);
    impl Drop for KeyFixture {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    fn config() -> (crate::srt::config::PreparedConfig, KeyFixture) {
        use std::os::unix::fs::PermissionsExt;
        let directory =
            std::env::temp_dir().join(format!("outpace-srt-owner-{}", rand::random::<u64>()));
        std::fs::create_dir(&directory).unwrap();
        std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o700)).unwrap();
        let key = directory.join("key");
        std::fs::write(&key, b"synthetic-owner-passphrase").unwrap();
        std::fs::set_permissions(&key, std::fs::Permissions::from_mode(0o600)).unwrap();
        let config = SrtConfig {
            bind: Some("127.0.0.1:0".parse().unwrap()),
            passphrase_file: Some(key),
            ..SrtConfig::default()
        }
        .prepare(true)
        .unwrap()
        .unwrap();
        (config, KeyFixture(directory))
    }

    fn broadcast_state() -> crate::http::BroadcastState {
        crate::http::BroadcastState {
            peer_policy: Default::default(),
            registry: crate::broadcast::BroadcastRegistry::new(),
            seed_registry: ace_swarm::listen::SeedRegistry::new(),
            trackers: vec![],
            store_bytes: 4 << 20,
            inbound_peer_port: watch::channel(None).1,
        }
    }
    fn offered(name: &str) -> (mpsc::Sender<Bytes>, watch::Sender<bool>, NativeIncoming) {
        let (sender, receiver) = mpsc::channel(4);
        let (confirm, retirement) = watch::channel(false);
        (
            sender,
            confirm,
            NativeIncoming {
                name: name.into(),
                receiver,
                retirement,
                phase: Arc::new(AtomicU8::new(PENDING)),
            },
        )
    }
    #[tokio::test]
    async fn srt_conflict_retires_without_admitting_or_replacing_the_existing_producer() {
        use crate::broadcast::{BroadcastError, ProducerKind};
        let state = broadcast_state();
        let (original, producer) = state
            .acquire_producer("synthetic-conflict", ProducerKind::RawPut)
            .await
            .unwrap();
        let (_data, _confirm, connection) = offered("synthetic-conflict");
        let phase = connection.phase.clone();
        let result = super::super::ingest::attach_connection(connection, state.clone()).await;
        assert!(matches!(result, Err(BroadcastError::ProducerConflict)));
        assert_eq!(phase.load(Ordering::Acquire), RETIRING);
        assert!(producer.check().is_ok());
        assert_eq!(
            state
                .registry
                .get("synthetic-conflict")
                .await
                .unwrap()
                .transport_bytes,
            original.transport_bytes
        );
        drop(producer);
        state.registry.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn srt_cancel_while_store_is_locked_retires_without_late_publication() {
        let state = broadcast_state();
        let (data, confirm, connection) = offered("synthetic-cancel");
        let phase = connection.phase.clone();
        let task = super::super::ingest::attach_connection(connection, state.clone())
            .await
            .unwrap();
        let broadcast = state.registry.get("synthetic-cancel").await.unwrap();
        let store = broadcast.store.lock().await;
        let mut bytes = vec![0u8; 188 * 300];
        for packet in bytes.chunks_mut(188) {
            packet[0] = 0x47;
        }
        let bytes = Bytes::from(bytes);
        data.send(bytes.clone()).await.unwrap();
        data.send(bytes).await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if broadcast.cursor.publication.try_lock().is_err() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("actual publication job owns its fence while the store is locked");
        // A canceled delete waiter must not lose the entry-owned stop continuation.
        let pending = tokio::time::timeout(
            Duration::from_millis(20),
            state.registry.delete("synthetic-cancel"),
        )
        .await;
        assert!(pending.is_err());
        tokio::time::timeout(Duration::from_secs(2), task.wait())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(phase.load(Ordering::Acquire), RETIRING);
        drop(store);
        confirm.send_replace(true);
        tokio::time::timeout(
            Duration::from_secs(2),
            state.registry.delete("synthetic-cancel"),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(broadcast.store.lock().await.chunk(0, 0).is_none());
        drop(data);
        state.registry.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn srt_live_disconnect_discards_short_tail_and_holds_claim_until_retired() {
        use crate::broadcast::{BroadcastError, ProducerKind};
        let state = broadcast_state();
        let (data, confirm, connection) = offered("synthetic-ingest");
        let task = super::super::ingest::attach_connection(connection, state.clone())
            .await
            .unwrap();
        let broadcast = state.registry.get("synthetic-ingest").await.unwrap();
        let mut bytes = vec![0u8; 188 * 8];
        for packet in bytes.chunks_mut(188) {
            packet[0] = 0x47;
        }
        data.send(Bytes::from(bytes)).await.unwrap();
        drop(data);
        tokio::time::timeout(Duration::from_secs(2), task.wait())
            .await
            .unwrap()
            .unwrap();
        assert!(
            broadcast.store.lock().await.chunk(0, 0).is_none(),
            "live disconnect cannot flush an incomplete finite tail"
        );
        assert!(matches!(
            state
                .acquire_producer("synthetic-ingest", ProducerKind::Rtmp)
                .await,
            Err(BroadcastError::ProducerConflict)
        ));
        confirm.send_replace(true);
        let (again, producer) = state
            .acquire_producer("synthetic-ingest", ProducerKind::RawPut)
            .await
            .unwrap();
        assert_eq!(broadcast.transport_bytes, again.transport_bytes);
        drop(producer);
        state.registry.shutdown().await.unwrap();
    }
    #[tokio::test]
    async fn srt_ingest_reuses_real_signed_publication_across_message_boundaries() {
        let state = broadcast_state();
        let (data, confirm, connection) = offered("synthetic-signed");
        let task = super::super::ingest::attach_connection(connection, state.clone())
            .await
            .unwrap();
        let broadcast = state.registry.get("synthetic-signed").await.unwrap();
        let mut bytes = vec![0u8; 188 * 900];
        for (index, packet) in bytes.chunks_mut(188).enumerate() {
            packet.fill((index % 251) as u8);
            packet[0] = 0x47;
        }
        for part in bytes.chunks(997) {
            data.send(Bytes::copy_from_slice(part)).await.unwrap();
        }
        drop(data);
        tokio::time::timeout(Duration::from_secs(3), task.wait())
            .await
            .unwrap()
            .unwrap();
        let store = broadcast.store.lock().await;
        assert!(store.has_piece(0));
        let mut piece = Vec::new();
        for chunk in 0..store.chunks_per_piece() {
            piece.extend_from_slice(&store.chunk(0, chunk).unwrap());
        }
        drop(store);
        let (payload, signature) =
            ace_wire::live_auth::split_piece(&piece, broadcast.auth.signature_len()).unwrap();
        assert!(ace_wire::live_auth::verify_piece(
            &broadcast.auth.pubkey_der(),
            payload,
            signature
        ));
        confirm.send_replace(true);
        state.registry.shutdown().await.unwrap();
    }

    // Removing reserve-before-read or making shutdown wait on a media send must
    // break this actual two-publisher pressure/control schedule.
    #[tokio::test]
    #[ignore = "requires FFmpeg with SRT support; generated loopback media only"]
    async fn full_handoff_preserves_other_publisher_and_owned_shutdown() {
        use std::process::{Command, Stdio};
        struct Publisher(std::process::Child);
        impl Drop for Publisher {
            fn drop(&mut self) {
                if self.0.try_wait().ok().flatten().is_none() {
                    let _ = self.0.kill();
                }
                let _ = self.0.wait();
            }
        }
        assert_eq!(std::env::var("OUTPACE_TEST_SRT_FFMPEG").as_deref(), Ok("1"));
        let (mut config, _key) = config();
        config.max_connections = 2;
        let mut owner = NativeOwner::start(config).await.unwrap();
        let mut publishers = Vec::new();
        let mut incoming = Vec::new();
        for name in ["synthetic-blocked", "synthetic-healthy"] {
            let url = format!("srt://{}?mode=caller&passphrase=synthetic-owner-passphrase&streamid={name}&pkt_size=1316&connect_timeout=1000", owner.local_addr());
            publishers.push(Publisher(
                Command::new("ffmpeg")
                    .args([
                        "-hide_banner",
                        "-loglevel",
                        "error",
                        "-re",
                        "-f",
                        "lavfi",
                        "-i",
                        "testsrc=size=64x64:rate=20",
                        "-an",
                        "-t",
                        "20",
                        "-c:v",
                        "mpeg2video",
                        "-f",
                        "mpegts",
                        &url,
                    ])
                    .stdin(Stdio::null())
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .spawn()
                    .unwrap(),
            ));
            let connection = tokio::time::timeout(Duration::from_secs(4), owner.incoming.recv())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(connection.name, name);
            assert_eq!(
                connection.receiver.len(),
                0,
                "unclaimed connections cannot read media"
            );
            assert!(connection.admit());
            incoming.push(connection);
        }
        let deadline = tokio::time::Instant::now() + Duration::from_secs(4);
        while incoming[0].receiver.len() < 4 && tokio::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let filled = incoming[0].receiver.len();
        // Discard already queued data: the witness must arrive after the other
        // publisher is full, rather than merely having progressed before pressure.
        while incoming[1].receiver.try_recv().is_ok() {}
        let healthy =
            tokio::time::timeout(Duration::from_secs(2), incoming[1].receiver.recv()).await;
        let mut retired = incoming
            .iter()
            .map(|c| c.retirement.clone())
            .collect::<Vec<_>>();
        let stopped = tokio::time::timeout(Duration::from_secs(10), owner.shutdown()).await;
        drop(incoming);
        drop(publishers);
        assert_eq!(
            filled, 4,
            "first real native handoff must reach its bounded capacity"
        );
        assert!(healthy
            .is_ok_and(|data| data.is_some_and(|data| !data.is_empty() && data.len() <= 65536)));
        assert!(
            matches!(stopped, Ok(Ok(()))),
            "media pressure cannot retain the native join"
        );
        for done in &mut retired {
            assert!(
                *done.borrow_and_update(),
                "retirement must be confirmed after join"
            );
        }
    }

    // A cancellation watch that needs another runtime poll cannot own native
    // termination when a receiver is retained across runtime teardown.
    #[test]
    fn native_owner_drop_latches_stop_without_another_runtime_poll() {
        if std::env::var_os("OUTPACE_TEST_SRT_RUNTIME_DROP_CHILD").is_some() {
            let (config, _key) = config();
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            let mut owner = runtime.block_on(NativeOwner::start(config)).unwrap();
            let (_, replacement) = mpsc::channel(1);
            let retained = std::mem::replace(&mut owner.incoming, replacement);
            drop(owner);
            // No block_on/yield after cancellation; runtime must still reap its
            // blocking join because native termination was latched synchronously.
            drop(runtime);
            drop(retained);
            return;
        }
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "srt::owner::tests::native_owner_drop_latches_stop_without_another_runtime_poll",
                "--exact",
            ])
            .env("OUTPACE_TEST_SRT_RUNTIME_DROP_CHILD", "1")
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        let exited = loop {
            if let Some(status) = child.try_wait().unwrap() {
                break Some(status);
            }
            if Instant::now() >= deadline {
                child.kill().unwrap();
                child.wait().unwrap();
                break None;
            }
            std::thread::sleep(Duration::from_millis(5));
        };
        assert!(
            exited.is_some_and(|status| status.success()),
            "owned runtime teardown must finish without polling the async cancellation worker"
        );
    }

    #[tokio::test]
    async fn native_owner_shutdown_joins_and_releases_listen_socket() {
        let (config, _key) = config();
        let mut owner = NativeOwner::start(config).await.unwrap();
        let address = owner.local_addr();
        let result = tokio::time::timeout(Duration::from_secs(10), owner.shutdown()).await;
        assert!(
            matches!(result, Ok(Ok(()))),
            "native owner must confirm its join"
        );
        let rebound = std::net::UdpSocket::bind(address);
        assert!(
            rebound.is_ok(),
            "native listener no longer owns the UDP address"
        );
    }

    #[tokio::test]
    async fn native_owner_drop_transfers_join_until_collected() {
        let (config, _key) = config();
        let owner = NativeOwner::start(config).await.unwrap();
        let mut completion = owner.completion.clone();
        let outcome = owner.outcome.clone();
        let address = owner.local_addr();
        drop(owner);
        let joined = tokio::time::timeout(
            Duration::from_secs(10),
            crate::manager::wait_cleanup(&mut completion),
        )
        .await;
        assert_eq!(joined, Ok(true));
        assert!(matches!(*outcome.borrow(), Some(Ok(()))));
        assert!(std::net::UdpSocket::bind(address).is_ok());
    }

    #[tokio::test]
    async fn native_owner_bind_error_returns_after_owned_cleanup() {
        let (mut config, _key) = config();
        let occupied = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        config.bind = occupied.local_addr().unwrap();
        let result =
            tokio::time::timeout(Duration::from_secs(10), NativeOwner::start(config)).await;
        assert!(matches!(result, Ok(Err(_))));
    }
}
