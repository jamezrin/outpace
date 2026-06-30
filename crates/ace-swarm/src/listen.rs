//! Inbound seeding: a registry of infohashes we serve, and a `PeerListener` that accepts
//! connections, verifies the requested infohash against the registry, and hands the socket to
//! `SeederSession::serve`.
use crate::seed::SeederSession;
use crate::store::PieceStore;
use ace_peer::session::PeerSession;
use std::collections::HashMap;
use std::sync::{Arc, Mutex as StdMutex};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpListener;
use tokio::sync::Mutex;

/// Per-infohash store, shared with whatever feeds pieces (a download loop, a broadcast source).
type SharedStore = Arc<Mutex<PieceStore>>;

/// Maps infohash -> the `PieceStore` we'd serve it from. Shared between whatever feeds pieces
/// (a download loop, a broadcast source) and the inbound listener.
#[derive(Clone, Default)]
pub struct SeedRegistry {
    stores: Arc<StdMutex<HashMap<[u8; 20], SharedStore>>>,
}

impl SeedRegistry {
    pub fn new() -> Self {
        SeedRegistry::default()
    }

    /// Register (or replace) the store for `infohash`.
    pub fn register(&self, infohash: [u8; 20], store: SharedStore) {
        self.stores.lock().unwrap().insert(infohash, store);
    }

    /// The store for `infohash`, if we serve it.
    pub fn get(&self, infohash: &[u8; 20]) -> Option<SharedStore> {
        self.stores.lock().unwrap().get(infohash).cloned()
    }

    /// True iff we serve `infohash` (used as the inbound handshake's accept predicate).
    pub fn serves(&self, infohash: &[u8; 20]) -> bool {
        self.stores.lock().unwrap().contains_key(infohash)
    }
}

/// Accepts inbound peer connections, verifies the requested infohash against `registry`, and
/// serves accepted peers via `SeederSession::serve`. Bounded to `max_inbound` concurrent peers.
pub struct PeerListener;

impl PeerListener {
    /// Run the accept loop until `listener` errors. Per-connection errors (failed handshake,
    /// unknown infohash, peer disconnect) are non-fatal — logged and dropped, the loop continues.
    /// `max_inbound == 0` is treated as `1` (a fully-closed listener would look like a hang).
    pub async fn serve(
        listener: TcpListener,
        registry: SeedRegistry,
        our_peer_id: [u8; 20],
        piece_header: [u8; 8],
        max_inbound: usize,
    ) {
        let sem = Arc::new(tokio::sync::Semaphore::new(max_inbound.max(1)));
        loop {
            let (stream, _addr) = match listener.accept().await {
                Ok(pair) => pair,
                Err(e) => {
                    // A backoff matters here: under fd exhaustion (EMFILE/ENFILE) accept()
                    // fails repeatedly with no natural delay, and a tight retry loop would
                    // busy-spin a core instead of giving the system room to recover.
                    eprintln!("[seed-listener] accept error: {e}");
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                    continue;
                }
            };
            // Bounded concurrency: try_acquire_owned (non-blocking) rather than
            // acquire_owned().await. At capacity we want to drop the new connection
            // immediately and keep accepting, not stall the accept loop queuing up
            // sockets the kernel has already handed us — a slow/idle peer holding a
            // permit must not back up every other inbound connection behind it.
            let permit = match sem.clone().try_acquire_owned() {
                Ok(p) => p,
                Err(_) => continue, // at capacity; drop the connection
            };
            let registry = registry.clone();
            tokio::spawn(async move {
                let _permit = permit;
                if let Err(e) = handle_inbound(stream, registry, our_peer_id, piece_header).await {
                    eprintln!("[seed-listener] peer error: {e:?}");
                }
            });
        }
    }
}

async fn handle_inbound<S: AsyncRead + AsyncWrite + Unpin>(
    stream: S,
    registry: SeedRegistry,
    our_peer_id: [u8; 20],
    piece_header: [u8; 8],
) -> ace_peer::Result<()> {
    let mut session = PeerSession::new(stream);
    let peer_hs = session.accept_handshake(our_peer_id, |ih| registry.serves(ih)).await?;
    let store = registry.get(&peer_hs.infohash).ok_or(ace_peer::PeerError::InfohashMismatch)?;
    SeederSession::serve(&mut session, store, piece_header).await
}
