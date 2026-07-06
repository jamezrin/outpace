//! Inbound seeding: a registry of infohashes we serve, and a `PeerListener` that accepts
//! connections, verifies the requested infohash against the registry, and hands the socket to
//! `SeederSession::serve`.
use crate::seed::SeederSession;
use crate::store::PieceStore;
use ace_peer::session::PeerSession;
use ace_wire::identity::Identity;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpListener;
use tokio::sync::Mutex;

/// Monotonic id stamped on each `SeedEntry` at creation so a `SeedLease` only ever mutates the
/// exact entry it was issued against — not a later entry that happens to reuse the same key after
/// a force-remove (the idle-TTL reaper). Guards against ABA lease-accounting corruption.
static NEXT_GENERATION: AtomicU64 = AtomicU64::new(1);

fn next_generation() -> u64 {
    NEXT_GENERATION.fetch_add(1, Ordering::Relaxed)
}

/// Per-infohash store, shared with whatever feeds pieces (a download loop, a broadcast source).
type SharedStore = Arc<Mutex<PieceStore>>;

/// Per-infohash metadata blob, shared by inbound seed sessions serving BEP-9 metadata.
type SharedMetadata = Arc<Vec<u8>>;

/// Who keeps a registry entry alive. `Leech` entries are refcounted by `SeedLease`s (the download
/// loop) and are also eligible for the idle-TTL backstop reaper; `Broadcast` entries are
/// operator-controlled (removed only when their lease drops on DELETE) and exempt from the reaper.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub enum OwnerKind {
    #[default]
    Leech,
    Broadcast,
}

#[derive(Default)]
struct SeedEntry {
    store: Option<SharedStore>,
    metadata: Option<SharedMetadata>,
    /// Number of live `SeedLease`s referencing this key; the entry is removed at zero.
    producers: usize,
    kind: OwnerKind,
    /// Unique id stamped at creation (see `next_generation`). A `SeedLease` records the generation
    /// it saw and only mutates the entry if it still matches — so a stale lease can't corrupt a
    /// different entry that reused the key. `Default` gives 0, but entries are never created via
    /// bare `or_default()`; always `or_insert_with(|| SeedEntry { generation: next_generation(), .. })`.
    generation: u64,
}

/// Maps infohash -> the store and/or metadata we'd serve. Entry lifetime is anchored by
/// `SeedLease` (producer refcount): the leech download loop and each broadcast hold a lease and
/// the entry is evicted when the last one drops. An idle-TTL reaper (added later) backstops leaked
/// leases for `Leech` entries.
#[derive(Clone, Default)]
pub struct SeedRegistry {
    stores: Arc<StdMutex<HashMap<[u8; 20], SeedEntry>>>,
}

impl SeedRegistry {
    pub fn new() -> Self {
        SeedRegistry::default()
    }

    /// Register (or replace) the store for `infohash`.
    pub fn register(&self, infohash: [u8; 20], store: SharedStore) {
        self.stores
            .lock()
            .unwrap()
            .entry(infohash)
            .or_insert_with(|| SeedEntry {
                generation: next_generation(),
                ..Default::default()
            })
            .store = Some(store);
    }

    /// Register (or replace) the BEP-9 metadata for `key`.
    pub fn register_metadata(&self, key: [u8; 20], metadata: Vec<u8>) {
        self.stores
            .lock()
            .unwrap()
            .entry(key)
            .or_insert_with(|| SeedEntry {
                generation: next_generation(),
                ..Default::default()
            })
            .metadata = Some(Arc::new(metadata));
    }

    /// The store for `infohash`, creating it via `make` (and registering it) if absent.
    /// Atomic: the whole get-or-insert happens under one lock acquisition.
    pub fn get_or_create(
        &self,
        infohash: [u8; 20],
        make: impl FnOnce() -> PieceStore,
    ) -> SharedStore {
        self.stores
            .lock()
            .unwrap()
            .entry(infohash)
            .or_insert_with(|| SeedEntry {
                generation: next_generation(),
                ..Default::default()
            })
            .store
            .get_or_insert_with(|| Arc::new(Mutex::new(make())))
            .clone()
    }

    /// Acquire a leech producer lease for `infohash`, creating the store via `make` if absent.
    /// The returned `SeedLease` refcounts the entry; when the last lease for this infohash drops,
    /// the entry (and its store) is evicted. Two concurrent leech sessions for one infohash share
    /// a single store and the entry survives until both leases drop.
    ///
    /// The registry's `StdMutex` is held across `make`, so `make` must not drop a `SeedLease` for
    /// this same registry (it would re-lock the non-reentrant mutex and deadlock).
    pub fn lease_store(
        &self,
        infohash: [u8; 20],
        make: impl FnOnce() -> PieceStore,
    ) -> (SharedStore, SeedLease) {
        let mut map = self.stores.lock().unwrap();
        let entry = map.entry(infohash).or_insert_with(|| SeedEntry {
            generation: next_generation(),
            ..Default::default()
        });
        let store = entry
            .store
            .get_or_insert_with(|| Arc::new(Mutex::new(make())))
            .clone();
        entry.producers += 1;
        let lease = SeedLease {
            registry: Arc::downgrade(&self.stores),
            keys: vec![(infohash, entry.generation)],
        };
        (store, lease)
    }

    /// Acquire a broadcast producer lease owning both the `infohash` (store) and `content_id`
    /// (BEP-9 metadata) keys, creating the store via `make` if absent. Marks the entries
    /// `Broadcast` (reaper-exempt). Dropping the returned lease evicts both keys.
    ///
    /// Overwrites any existing `content_id` metadata; content is content-addressed by `content_id`,
    /// so the blob is deterministic for a given key and the overwrite is a no-op in practice.
    ///
    /// The registry's `StdMutex` is held across `make`, so `make` must not drop a `SeedLease` for
    /// this same registry (it would re-lock the non-reentrant mutex and deadlock).
    pub fn lease_broadcast(
        &self,
        infohash: [u8; 20],
        content_id: [u8; 20],
        metadata: Vec<u8>,
        make: impl FnOnce() -> PieceStore,
    ) -> (SharedStore, SeedLease) {
        let mut map = self.stores.lock().unwrap();
        let (store, ih_generation) = {
            let entry = map.entry(infohash).or_insert_with(|| SeedEntry {
                generation: next_generation(),
                ..Default::default()
            });
            entry.producers += 1;
            entry.kind = OwnerKind::Broadcast;
            let store = entry
                .store
                .get_or_insert_with(|| Arc::new(Mutex::new(make())))
                .clone();
            (store, entry.generation)
        };
        let cid_generation = {
            let meta_entry = map.entry(content_id).or_insert_with(|| SeedEntry {
                generation: next_generation(),
                ..Default::default()
            });
            meta_entry.producers += 1;
            meta_entry.kind = OwnerKind::Broadcast;
            meta_entry.metadata = Some(Arc::new(metadata));
            meta_entry.generation
        };
        let lease = SeedLease {
            registry: Arc::downgrade(&self.stores),
            keys: vec![(infohash, ih_generation), (content_id, cid_generation)],
        };
        (store, lease)
    }

    /// The store for `infohash`, if we serve it.
    pub fn get(&self, infohash: &[u8; 20]) -> Option<SharedStore> {
        self.stores.lock().unwrap().get(infohash)?.store.clone()
    }

    /// The BEP-9 metadata for `key`, if we serve it.
    pub fn metadata(&self, key: &[u8; 20]) -> Option<Arc<Vec<u8>>> {
        self.stores.lock().unwrap().get(key)?.metadata.clone()
    }

    /// True iff we serve `infohash` (used as the inbound handshake's accept predicate).
    pub fn serves(&self, infohash: &[u8; 20]) -> bool {
        self.stores.lock().unwrap().contains_key(infohash)
    }

    /// Stop serving `key` (an infohash or a metadata content_id): drops both the piece store
    /// and any registered metadata under it. Idempotent — removing an absent key is a no-op.
    pub fn remove(&self, key: &[u8; 20]) {
        self.stores.lock().unwrap().remove(key);
    }
}

/// RAII producer handle for one or more `SeedRegistry` keys. Dropping it decrements each key's
/// producer count and evicts any entry that reaches zero. Not `Clone` — a clone would need to bump
/// the count; acquire another lease via the registry instead. Each key is paired with the entry
/// generation seen at issue, so a stale lease never touches a different entry that reused the key.
pub struct SeedLease {
    registry: std::sync::Weak<StdMutex<HashMap<[u8; 20], SeedEntry>>>,
    keys: Vec<([u8; 20], u64)>,
}

impl Drop for SeedLease {
    fn drop(&mut self) {
        let Some(map) = self.registry.upgrade() else {
            return;
        };
        let mut map = map.lock().unwrap();
        for (key, generation) in &self.keys {
            if let Some(entry) = map.get_mut(key) {
                // Only decrement the exact entry this lease was issued against; a force-remove +
                // recreate at the same key bumps the generation and makes this a no-op.
                if entry.generation == *generation {
                    entry.producers = entry.producers.saturating_sub(1);
                    if entry.producers == 0 {
                        map.remove(key);
                    }
                }
            }
        }
    }
}

/// Accepts inbound peer connections, verifies the requested infohash against `registry`, and
/// serves accepted peers via `SeederSession::serve`. Bounded to `max_inbound` concurrent peers.
pub struct PeerListener;

impl PeerListener {
    /// Run the accept loop until `listener` errors. Per-connection errors (failed handshake,
    /// unknown infohash, peer disconnect) are non-fatal — logged and dropped, the loop continues.
    /// `max_inbound == 0` is treated as `1` (a fully-closed listener would look like a hang).
    /// `identity` signs the extended handshake every accepted peer requires
    /// (`SeederSession::serve`) — without it a real leech client (including our own
    /// `AceProvider`) never proceeds past waiting for it.
    #[allow(clippy::too_many_arguments)]
    pub async fn serve(
        listener: TcpListener,
        registry: SeedRegistry,
        our_peer_id: [u8; 20],
        piece_header: [u8; 8],
        max_inbound: usize,
        identity: Arc<Identity>,
    ) {
        let sem = Arc::new(tokio::sync::Semaphore::new(max_inbound.max(1)));
        loop {
            let (stream, addr) = match listener.accept().await {
                Ok(pair) => pair,
                Err(e) => {
                    // A backoff matters here: under fd exhaustion (EMFILE/ENFILE) accept()
                    // fails repeatedly with no natural delay, and a tight retry loop would
                    // busy-spin a core instead of giving the system room to recover.
                    crate::swarm_log!("[seed-listener] accept error: {e}");
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
            let identity = identity.clone();
            let peer_ip = match addr.ip() {
                std::net::IpAddr::V4(v4) => v4.octets(),
                std::net::IpAddr::V6(_) => [0, 0, 0, 0],
            };
            tokio::spawn(async move {
                let _permit = permit;
                crate::swarm_log!("[seed-listener] accepted connection from {addr}");
                if let Err(e) = handle_inbound(
                    stream,
                    registry,
                    our_peer_id,
                    piece_header,
                    &identity,
                    peer_ip,
                )
                .await
                {
                    crate::swarm_log!("[seed-listener] peer error from {addr}: {e:?}");
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
    identity: &Identity,
    peer_ip: [u8; 4],
) -> ace_peer::Result<()> {
    let mut session = PeerSession::new(stream);
    let peer_hs = session
        .accept_handshake(our_peer_id, |ih| registry.serves(ih))
        .await?;
    let store = registry.get(&peer_hs.infohash);
    let metadata = registry.metadata(&peer_hs.infohash);
    if store.is_none() && metadata.is_none() {
        return Err(ace_peer::PeerError::InfohashMismatch);
    }
    SeederSession::serve(
        &mut session,
        store,
        metadata,
        piece_header,
        identity,
        peer_ip,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_or_create_is_idempotent() {
        let reg = SeedRegistry::new();
        let ih = [1u8; 20];
        let a = reg.get_or_create(ih, || PieceStore::new(4, 4, 1024));
        let b = reg.get_or_create(ih, || panic!("must not call make() again"));
        assert!(
            Arc::ptr_eq(&a, &b),
            "second call must return the SAME store, not create a new one"
        );
    }

    #[test]
    fn registry_serves_metadata_only_keys() {
        let reg = SeedRegistry::new();
        let key = [9u8; 20];
        reg.register_metadata(key, vec![1, 2, 3, 4]);
        assert!(reg.serves(&key));
        assert_eq!(&*reg.metadata(&key).unwrap(), &[1, 2, 3, 4]);
        assert!(reg.get(&key).is_none());
    }

    #[test]
    fn remove_drops_both_store_and_metadata_and_is_idempotent() {
        let reg = SeedRegistry::new();
        let key = [7u8; 20];
        reg.get_or_create(key, || PieceStore::new(4, 4, 1024));
        reg.register_metadata(key, vec![1, 2, 3, 4]);
        assert!(reg.serves(&key));

        reg.remove(&key);
        assert!(!reg.serves(&key), "removed key is no longer served");
        assert!(reg.get(&key).is_none());
        assert!(reg.metadata(&key).is_none());

        // Removing an absent key is a no-op.
        reg.remove(&key);
        assert!(!reg.serves(&key));
    }

    #[test]
    fn lease_evicts_entry_when_last_producer_drops() {
        let reg = SeedRegistry::new();
        let ih = [3u8; 20];
        let (_store, lease) = reg.lease_store(ih, || PieceStore::new(4, 4, 1024));
        assert!(reg.serves(&ih), "served while a producer holds the lease");
        drop(lease);
        assert!(
            !reg.serves(&ih),
            "entry evicted when the last producer drops"
        );
    }

    #[test]
    fn two_leases_refcount_the_same_entry() {
        let reg = SeedRegistry::new();
        let ih = [4u8; 20];
        let (a, l1) = reg.lease_store(ih, || PieceStore::new(4, 4, 1024));
        let (b, l2) = reg.lease_store(ih, || panic!("second lease must reuse the store"));
        assert!(Arc::ptr_eq(&a, &b), "both leases share one store");
        drop(l1);
        assert!(
            reg.serves(&ih),
            "entry survives while the second producer holds it"
        );
        drop(l2);
        assert!(
            !reg.serves(&ih),
            "entry evicted only when both producers drop"
        );
    }

    #[test]
    fn broadcast_lease_owns_both_infohash_and_content_id() {
        let reg = SeedRegistry::new();
        let ih = [5u8; 20];
        let cid = [6u8; 20];
        let (_store, lease) =
            reg.lease_broadcast(ih, cid, vec![1, 2, 3], || PieceStore::new(4, 4, 1024));
        assert!(reg.serves(&ih) && reg.serves(&cid));
        assert_eq!(&*reg.metadata(&cid).unwrap(), &[1, 2, 3]);
        drop(lease);
        assert!(!reg.serves(&ih), "infohash entry evicted");
        assert!(!reg.serves(&cid), "content_id entry evicted");
    }

    #[test]
    fn stale_lease_drop_does_not_evict_a_reborn_entry_at_the_same_key() {
        let reg = SeedRegistry::new();
        let ih = [12u8; 20];
        let (_s1, l1) = reg.lease_store(ih, || PieceStore::new(4, 4, 1024));
        // Simulate the reaper force-removing the entry while the lease is still outstanding.
        reg.remove(&ih);
        assert!(!reg.serves(&ih));
        // A new producer re-creates the entry (new generation).
        let (_s2, _l2) = reg.lease_store(ih, || PieceStore::new(4, 4, 1024));
        assert!(reg.serves(&ih));
        // Dropping the STALE lease must NOT evict the reborn entry.
        drop(l1);
        assert!(
            reg.serves(&ih),
            "stale lease drop must not touch the reborn entry"
        );
    }
}
