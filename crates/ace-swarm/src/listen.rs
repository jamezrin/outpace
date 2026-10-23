//! Inbound seeding: a registry of infohashes we serve, and a `PeerListener` that accepts
//! connections, verifies the requested infohash against the registry, and hands the socket to
//! `SeederSession::serve`.
use crate::reachability::ReachabilityMonitor;
use crate::seed::SeederSession;
use crate::seed::ServeCoordinator;
use crate::store::PieceStore;
use ace_peer::session::PeerSession;
use ace_wire::identity::Identity;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex, OnceLock};
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

fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Per-infohash store, shared with whatever feeds pieces (a download loop, a broadcast source).
type SharedStore = Arc<Mutex<PieceStore>>;
type StoreRendezvous = Arc<OnceLock<SharedStore>>;
type CreatingStores = Arc<StdMutex<HashMap<[u8; 20], StoreRendezvous>>>;

/// Per-infohash metadata blob, shared by inbound seed sessions serving BEP-9 metadata.
type SharedMetadata = Arc<Vec<u8>>;

/// Inbound connection occupancy and exact-key desired-unchoke policy.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct InboundPolicy {
    pub max_inbound: usize,
    /// Combined media/metadata occupancy per canonical stream; zero disables this cap.
    pub max_per_stream: usize,
    pub max_unchoked: usize,
}

impl InboundPolicy {
    /// Create one domain for a listener. Clones share global and canonical capacity.
    pub fn admission(self) -> InboundAdmission {
        InboundAdmission {
            policy: self,
            global: Arc::new(tokio::sync::Semaphore::new(self.max_inbound.max(1))),
            accounting: Arc::new(StdMutex::new(AdmissionState::default())),
        }
    }
}

/// Shared admission domain for accepted transports, independent of registry generations.
#[derive(Clone)]
pub struct InboundAdmission {
    policy: InboundPolicy,
    global: Arc<tokio::sync::Semaphore>,
    accounting: Arc<StdMutex<AdmissionState>>,
}

impl InboundAdmission {
    /// Reserve global capacity without waiting, returning an owning connection future.
    /// At capacity the transport closes immediately. Dropping an unpolled future also releases
    /// its reservation; once polled, it retains both reservations through every serving await.
    pub fn accept<S>(
        &self,
        stream: S,
        registry: SeedRegistry,
        our_peer_id: [u8; 20],
        piece_header: [u8; 8],
        identity: Arc<Identity>,
        peer_ip: [u8; 4],
    ) -> Option<impl std::future::Future<Output = ace_peer::Result<()>> + Send + 'static>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let permit = self.global.clone().try_acquire_owned().ok()?;
        let admission = self.clone();
        Some(async move {
            let _global_permit = permit;
            handle_inbound(
                stream,
                registry,
                our_peer_id,
                piece_header,
                &identity,
                peer_ip,
                &admission,
            )
            .await
        })
    }
}

#[derive(Default)]
struct AdmissionState {
    counts: HashMap<[u8; 20], usize>,
    bindings: HashMap<[u8; 20], [u8; 20]>,
}

/// Weak observers let broadcast registration extend a live canonical group's alias bindings,
/// even when the group's last snapshot predates that registration. No accounting owner is kept
/// alive by the registry, and this list's lock is released before taking an accounting lock.
type AdmissionDomains = Arc<StdMutex<Vec<std::sync::Weak<StdMutex<AdmissionState>>>>>;

struct InboundPermit {
    accounting: Arc<StdMutex<AdmissionState>>,
    canonical: [u8; 20],
}

impl Drop for InboundPermit {
    fn drop(&mut self) {
        // Never acquire the registry lock here: admission's lock order is registry -> accounting.
        let mut state = self.accounting.lock().unwrap();
        if let Some(count) = state.counts.get_mut(&self.canonical) {
            *count -= 1;
            if *count == 0 {
                state.counts.remove(&self.canonical);
                state
                    .bindings
                    .retain(|_, canonical| *canonical != self.canonical);
            }
        }
    }
}

struct AdmittedSeed {
    store: Option<SharedStore>,
    metadata: Option<SharedMetadata>,
    coordinator: Arc<ServeCoordinator>,
    _permit: InboundPermit,
}

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
    /// Millis-since-epoch timestamp of the last activity (creation or `get`), used by the
    /// idle-TTL reaper to decide whether a `Leech` entry has been abandoned.
    last_active: u64,
    /// Lazily-created multi-peer serve coordinator (S2). Shares the entry's lifetime.
    coordinator: Option<Arc<ServeCoordinator>>,
    /// None marks an inconsistent association, rejected until this entry is removed.
    canonical: Option<[u8; 20]>,
    /// Keep the broadcast group on both entries, including aliases removed before admission.
    /// Live admissions additionally retain these bindings outside the registry generation.
    admission_aliases: Vec<[u8; 20]>,
}

impl SeedEntry {
    fn require_canonical(&mut self, canonical: [u8; 20]) {
        if self.canonical != Some(canonical) {
            self.canonical = None;
        }
    }

    fn associate_broadcast(&mut self, canonical: [u8; 20], alias: [u8; 20]) {
        self.require_canonical(canonical);
        for key in [canonical, alias] {
            if !self.admission_aliases.contains(&key) {
                self.admission_aliases.push(key);
            }
        }
    }
}

/// Maps infohash -> the store and/or metadata we'd serve. Entry lifetime is anchored by
/// `SeedLease` (producer refcount): the leech download loop and each broadcast hold a lease and
/// the entry is evicted when the last one drops. An idle-TTL reaper (added later) backstops leaked
/// leases for `Leech` entries.
#[derive(Clone, Default)]
pub struct SeedRegistry {
    stores: Arc<StdMutex<HashMap<[u8; 20], SeedEntry>>>,
    admission_domains: AdmissionDomains,
    /// Per-key construction rendezvous. `OnceLock` runs the potentially blocking factory without
    /// holding `stores`; racing callers wait for and reuse the exact published store instead of
    /// constructing a disposable disk-backed candidate.
    creating: CreatingStores,
}

impl SeedRegistry {
    pub fn new() -> Self {
        SeedRegistry::default()
    }

    /// Register (or replace) the store for `infohash`.
    pub fn register(&self, infohash: [u8; 20], store: SharedStore) {
        let mut map = self.stores.lock().unwrap();
        let entry = map.entry(infohash).or_insert_with(|| SeedEntry {
            canonical: Some(infohash),
            generation: next_generation(),
            last_active: now_millis(),
            ..Default::default()
        });
        entry.require_canonical(infohash);
        let old = entry.store.replace(store);
        drop(map);
        drop(old);
    }

    /// Register (or replace) the BEP-9 metadata for `key`.
    pub fn register_metadata(&self, key: [u8; 20], metadata: Vec<u8>) {
        let mut map = self.stores.lock().unwrap();
        let entry = map.entry(key).or_insert_with(|| SeedEntry {
            canonical: Some(key),
            generation: next_generation(),
            last_active: now_millis(),
            ..Default::default()
        });
        entry.require_canonical(key);
        entry.metadata = Some(Arc::new(metadata));
    }

    /// The store for `infohash`, creating it via `make` (and registering it) if absent.
    /// Store construction deliberately happens outside the registry mutex: disk-backed stores
    /// may wipe and create directories, which must not stall inbound handshake lookup.
    pub fn get_or_create(
        &self,
        infohash: [u8; 20],
        make: impl FnOnce() -> PieceStore,
    ) -> SharedStore {
        {
            let mut map = self.stores.lock().unwrap();
            if let Some(entry) = map.get_mut(&infohash) {
                entry.require_canonical(infohash);
                entry.last_active = now_millis();
                if let Some(store) = &entry.store {
                    return store.clone();
                }
            }
        }
        let rendezvous = {
            let mut creating = self.creating.lock().unwrap();
            creating.entry(infohash).or_default().clone()
        };
        let candidate = rendezvous
            .get_or_init(|| Arc::new(Mutex::new(make())))
            .clone();
        let store = {
            let mut map = self.stores.lock().unwrap();
            let entry = map.entry(infohash).or_insert_with(|| SeedEntry {
                canonical: Some(infohash),
                generation: next_generation(),
                last_active: now_millis(),
                ..Default::default()
            });
            entry.require_canonical(infohash);
            entry.store.get_or_insert_with(|| candidate.clone()).clone()
        };
        // Removing the rendezvous can drop its final store reference, so do it outside `stores`.
        let finished = self.creating.lock().unwrap().remove(&infohash);
        drop(finished);
        store
    }

    /// Acquire a leech producer lease for `infohash`, creating the store via `make` if absent.
    /// The returned `SeedLease` refcounts the entry; when the last lease for this infohash drops,
    /// the entry (and its store) is evicted. Two concurrent leech sessions for one infohash share
    /// a single store and the entry survives until both leases drop.
    ///
    /// Store construction happens before the mutation lock is acquired.
    pub fn lease_store(
        &self,
        infohash: [u8; 20],
        make: impl FnOnce() -> PieceStore,
    ) -> (SharedStore, SeedLease) {
        let candidate = self.get_or_create(infohash, make);
        let mut map = self.stores.lock().unwrap();
        let entry = map.entry(infohash).or_insert_with(|| SeedEntry {
            canonical: Some(infohash),
            generation: next_generation(),
            last_active: now_millis(),
            ..Default::default()
        });
        entry.require_canonical(infohash);
        let store = entry.store.get_or_insert_with(|| candidate.clone()).clone();
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
    /// Store construction happens before the mutation lock is acquired.
    pub fn lease_broadcast(
        &self,
        infohash: [u8; 20],
        content_id: [u8; 20],
        metadata: Vec<u8>,
        make: impl FnOnce() -> PieceStore,
    ) -> (SharedStore, SeedLease) {
        let candidate = self.get_or_create(infohash, make);
        let mut map = self.stores.lock().unwrap();
        let (store, ih_generation) = {
            let entry = map.entry(infohash).or_insert_with(|| SeedEntry {
                canonical: Some(infohash),
                generation: next_generation(),
                last_active: now_millis(),
                ..Default::default()
            });
            entry.associate_broadcast(infohash, content_id);
            entry.producers += 1;
            entry.kind = OwnerKind::Broadcast;
            let store = entry.store.get_or_insert_with(|| candidate.clone()).clone();
            (store, entry.generation)
        };
        let cid_generation = {
            let meta_entry = map.entry(content_id).or_insert_with(|| SeedEntry {
                canonical: Some(infohash),
                generation: next_generation(),
                last_active: now_millis(),
                ..Default::default()
            });
            meta_entry.associate_broadcast(infohash, content_id);
            meta_entry.producers += 1;
            meta_entry.kind = OwnerKind::Broadcast;
            meta_entry.metadata = Some(Arc::new(metadata));
            meta_entry.generation
        };
        self.retain_live_aliases(&mut map, infohash);
        let lease = SeedLease {
            registry: Arc::downgrade(&self.stores),
            keys: vec![(infohash, ih_generation), (content_id, cid_generation)],
        };
        (store, lease)
    }

    /// Called under the registry mutation lock after associating both broadcast entries.
    /// Extending a group must also bind its new aliases for already-admitted media-only peers.
    fn retain_live_aliases(&self, map: &mut HashMap<[u8; 20], SeedEntry>, canonical: [u8; 20]) {
        let Some(aliases) = Self::canonical_aliases(map, canonical) else {
            return;
        };
        let domains: Vec<_> = {
            let mut domains = self.admission_domains.lock().unwrap();
            domains.retain(|domain| domain.strong_count() != 0);
            domains
                .iter()
                .filter_map(|domain| domain.upgrade())
                .collect()
        };
        for domain in domains {
            let mut state = domain.lock().unwrap();
            if !state.counts.contains_key(&canonical) {
                continue;
            }
            if aliases.iter().any(|alias| {
                state
                    .bindings
                    .get(alias)
                    .is_some_and(|bound| *bound != canonical)
            }) {
                // An extension that would join two live identities is inconsistent for this
                // registry generation, rather than silently moving either identity's aliases.
                for entry in map.values_mut() {
                    if entry.canonical == Some(canonical) {
                        entry.canonical = None;
                    }
                }
                return;
            }
            for alias in &aliases {
                state.bindings.insert(*alias, canonical);
            }
        }
    }

    fn canonical_aliases(
        map: &HashMap<[u8; 20], SeedEntry>,
        canonical: [u8; 20],
    ) -> Option<Vec<[u8; 20]>> {
        let mut aliases = vec![canonical];
        for (key, entry) in map.iter() {
            if entry.canonical == Some(canonical) {
                aliases.push(*key);
                aliases.extend_from_slice(&entry.admission_aliases);
            }
        }
        if aliases.iter().any(|alias| {
            map.get(alias)
                .is_some_and(|entry| entry.canonical != Some(canonical))
        }) {
            return None;
        }
        Some(aliases)
    }

    /// The store for `infohash`, if we serve it. Touches the entry's activity clock (used by the
    /// idle-TTL reaper) so an actively-served stream is never reaped.
    pub fn get(&self, infohash: &[u8; 20]) -> Option<SharedStore> {
        let mut map = self.stores.lock().unwrap();
        let entry = map.get_mut(infohash)?;
        entry.last_active = now_millis();
        entry.store.clone()
    }

    /// The BEP-9 metadata for `key`, if we serve it.
    pub fn metadata(&self, key: &[u8; 20]) -> Option<Arc<Vec<u8>>> {
        self.stores.lock().unwrap().get(key)?.metadata.clone()
    }

    /// The transport at an exact content-id key owned by a local broadcast. Verify the raw
    /// wrapped transport hash before granting local trust; generic seeded metadata is excluded.
    pub fn broadcast_transport_for_content_id(
        &self,
        content_id: &[u8; 20],
    ) -> Option<Arc<Vec<u8>>> {
        let metadata = {
            let map = self.stores.lock().unwrap();
            let entry = map.get(content_id)?;
            if entry.kind != OwnerKind::Broadcast {
                return None;
            }
            entry.metadata.clone()?
        };
        (ace_wire::infohash::transport_file_hash(&metadata) == *content_id).then_some(metadata)
    }

    /// The transport descriptor of a broadcast this registry originates whose swarm infohash is
    /// `infohash`, if any (#164). Broadcast transports are registered under their content id
    /// (the BEP-9 metadata key), so this scans broadcast-owned metadata and keeps the descriptor
    /// that hashes to `infohash`. A daemon originates a handful of broadcasts at most, so the
    /// scan is cheap; hashing happens after the registry lock is released.
    pub fn broadcast_transport_for_infohash(&self, infohash: &[u8; 20]) -> Option<Arc<Vec<u8>>> {
        let candidates: Vec<SharedMetadata> = {
            let map = self.stores.lock().unwrap();
            map.values()
                .filter(|e| e.kind == OwnerKind::Broadcast)
                .filter_map(|e| e.metadata.clone())
                .collect()
        };
        candidates.into_iter().find(|meta| {
            ace_wire::infohash::try_infohash_of_transport(meta).is_ok_and(|ih| &ih == infohash)
        })
    }

    /// True iff a registry entry exists. Inbound handshakes use coherent admission instead.
    pub fn serves(&self, infohash: &[u8; 20]) -> bool {
        self.stores.lock().unwrap().contains_key(infohash)
    }

    /// Resolve and reserve under one mutation lock; no later resource lookup is needed.
    fn admit_inbound(&self, key: &[u8; 20], admission: &InboundAdmission) -> Option<AdmittedSeed> {
        let mut map = self.stores.lock().unwrap();
        let entry = map.get(key)?;
        if entry.store.is_none() && entry.metadata.is_none() {
            return None;
        }
        let canonical = entry.canonical?;
        // A collision on any member invalidates admission for the whole canonical group,
        // including media-only requests that would otherwise avoid the conflicting alias.
        let aliases = Self::canonical_aliases(&map, canonical)?;
        let mut state = admission.accounting.lock().unwrap();
        if aliases.iter().any(|alias| {
            state
                .bindings
                .get(alias)
                .is_some_and(|bound| *bound != canonical)
        }) {
            return None;
        }
        let count = state.counts.get(&canonical).copied().unwrap_or(0);
        if admission.policy.max_per_stream != 0 && count >= admission.policy.max_per_stream {
            return None;
        }
        let next = count.checked_add(1)?;
        let entry = map.get_mut(key)?;
        let coordinator = entry
            .coordinator
            .get_or_insert_with(|| ServeCoordinator::new(admission.policy.max_unchoked))
            .clone();
        entry.last_active = now_millis();
        let store = entry.store.clone();
        let metadata = entry.metadata.clone();
        state.counts.insert(canonical, next);
        for alias in aliases {
            state.bindings.insert(alias, canonical);
        }
        drop(state);
        {
            let mut domains = self.admission_domains.lock().unwrap();
            domains.retain(|domain| domain.strong_count() != 0);
            let domain = Arc::downgrade(&admission.accounting);
            if !domains.iter().any(|existing| existing.ptr_eq(&domain)) {
                domains.push(domain);
            }
        }
        drop(map);
        Some(AdmittedSeed {
            store,
            metadata,
            coordinator,
            _permit: InboundPermit {
                accounting: admission.accounting.clone(),
                canonical,
            },
        })
    }

    /// The serve coordinator for `infohash`, creating it (with `max_unchoked`) on first use.
    /// Returns `None` if we don't serve this infohash (entry absent), so a racing eviction can't
    /// resurrect a bare entry.
    pub fn coordinator_for(
        &self,
        infohash: &[u8; 20],
        max_unchoked: usize,
    ) -> Option<Arc<ServeCoordinator>> {
        let mut map = self.stores.lock().unwrap();
        let entry = map.get_mut(infohash)?;
        Some(
            entry
                .coordinator
                .get_or_insert_with(|| ServeCoordinator::new(max_unchoked))
                .clone(),
        )
    }

    /// Advance every live coordinator's optimistic-unchoke rotation. Called by the listener's
    /// periodic rechoke ticker.
    pub fn rechoke_all(&self) {
        let coords: Vec<_> = {
            let map = self.stores.lock().unwrap();
            map.values().filter_map(|e| e.coordinator.clone()).collect()
        };
        for c in coords {
            c.rechoke();
        }
    }

    /// Stop serving `key` (an infohash or a metadata content_id): drops both the piece store
    /// and any registered metadata under it. Idempotent — removing an absent key is a no-op.
    pub fn remove(&self, key: &[u8; 20]) {
        let removed = self.stores.lock().unwrap().remove(key);
        drop(removed);
    }

    /// Force-evict idle **ownerless** `Leech` entries (`producers == 0`, i.e. created outside the
    /// lease API and never tied to a live producer). Entries held by a live `SeedLease`
    /// (`producers > 0`) and all `Broadcast` entries are NEVER reaped — normal teardown rides the
    /// lease drop. This is a backstop against orphans, not a way to reclaim a live-but-slow producer
    /// (a stuck task holding a lease is left alone; it can't be soundly told apart from a slow one).
    /// Returns how many entries were removed.
    pub fn reap(&self, ttl: std::time::Duration) -> usize {
        let now = now_millis();
        let ttl_ms = ttl.as_millis() as u64;
        let (removed, count) = {
            let mut map = self.stores.lock().unwrap();
            let keys: Vec<_> = map
                .iter()
                .filter(|(_, e)| {
                    e.kind != OwnerKind::Broadcast
                        && e.producers == 0
                        && now.saturating_sub(e.last_active) >= ttl_ms
                })
                .map(|(&key, _)| key)
                .collect();
            let removed: Vec<_> = keys.iter().filter_map(|key| map.remove(key)).collect();
            let count = removed.len();
            (removed, count)
        };
        drop(removed);
        count
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
        let removed = {
            let mut map = map.lock().unwrap();
            let mut removed = Vec::new();
            for (key, generation) in &self.keys {
                if let Some(entry) = map.get_mut(key) {
                    // Only decrement the exact entry this lease was issued against; a force-remove +
                    // recreate at the same key bumps the generation and makes this a no-op.
                    if entry.generation == *generation {
                        entry.producers = entry.producers.saturating_sub(1);
                        if entry.producers == 0 {
                            if let Some(entry) = map.remove(key) {
                                removed.push(entry);
                            }
                        }
                    }
                }
            }
            removed
        };
        drop(removed);
    }
}

/// Accepts inbound peer connections, verifies the requested infohash against `registry`, and
/// serves accepted peers via `SeederSession::serve`, retaining admission until connection exit.
pub struct PeerListener;

impl PeerListener {
    /// Run the accept loop until `listener` errors. Per-connection errors (failed handshake,
    /// unknown infohash, peer disconnect) are non-fatal — logged and dropped, the loop continues.
    /// `max_inbound == 0` is treated as `1` (a fully-closed listener would look like a hang).
    /// `identity` signs the extended handshake every accepted peer requires
    /// (`SeederSession::serve`) — without it a real leech client (including our own
    /// `AceProvider`) never proceeds past waiting for it.
    #[allow(clippy::too_many_arguments)]
    /// `reachability`, when present, counts each accepted inbound connection — the strongest
    /// signal that we are publicly reachable (issue #22). It is inert (never passed) when
    /// inbound serving is disabled, since then no listener runs at all.
    pub async fn serve(
        listener: TcpListener,
        registry: SeedRegistry,
        our_peer_id: [u8; 20],
        piece_header: [u8; 8],
        max_inbound: usize,
        identity: Arc<Identity>,
        max_unchoked: usize,
        reachability: Option<Arc<ReachabilityMonitor>>,
    ) {
        Self::serve_with_policy(
            listener,
            registry,
            our_peer_id,
            piece_header,
            Self::compatibility_policy(max_inbound, max_unchoked).admission(),
            identity,
            reachability,
        )
        .await;
    }

    fn compatibility_policy(max_inbound: usize, max_unchoked: usize) -> InboundPolicy {
        InboundPolicy {
            max_inbound,
            max_per_stream: 0,
            max_unchoked,
        }
    }

    /// Serve with an already-constructed shared policy domain. Aborting this listener does not
    /// abort its detached connection tasks; their permits and snapshots remain live until exit.
    pub async fn serve_with_policy(
        listener: TcpListener,
        registry: SeedRegistry,
        our_peer_id: [u8; 20],
        piece_header: [u8; 8],
        admission: InboundAdmission,
        identity: Arc<Identity>,
        reachability: Option<Arc<ReachabilityMonitor>>,
    ) {
        loop {
            let (stream, addr) = match listener.accept().await {
                Ok(pair) => pair,
                Err(e) => {
                    // A backoff matters here: under fd exhaustion (EMFILE/ENFILE) accept()
                    // fails repeatedly with no natural delay, and a tight retry loop would
                    // busy-spin a core instead of giving the system room to recover.
                    crate::alog!("[seed-listener] accept error: {e}");
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                    continue;
                }
            };
            // Every accepted connection is an unsolicited inbound dial — the strongest proof we
            // are publicly reachable (issue #22). Count it before the concurrency gate so even a
            // connection dropped at capacity still registers as "someone reached us".
            if let Some(monitor) = &reachability {
                monitor.note_inbound_peer();
            }
            let peer_ip = match addr.ip() {
                std::net::IpAddr::V4(v4) => v4.octets(),
                std::net::IpAddr::V6(_) => [0, 0, 0, 0],
            };
            // The same nonblocking gate owns the permit for TCP and other accepted transports.
            let Some(connection) = admission.accept(
                stream,
                registry.clone(),
                our_peer_id,
                piece_header,
                identity.clone(),
                peer_ip,
            ) else {
                continue;
            };
            tokio::spawn(async move {
                crate::alog!("[seed-listener] accepted connection from {addr}");
                if let Err(e) = connection.await {
                    crate::alog!("[seed-listener] peer error from {addr}: {e:?}");
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
    admission: &InboundAdmission,
) -> ace_peer::Result<()> {
    let mut session = PeerSession::new(stream);
    let mut admitted = None;
    session
        .accept_handshake(our_peer_id, |key| {
            admitted = registry.admit_inbound(key, admission);
            admitted.is_some()
        })
        .await?;
    let snapshot = admitted.ok_or(ace_peer::PeerError::InfohashMismatch)?;
    let result = SeederSession::serve(
        &mut session,
        snapshot.store.clone(),
        snapshot.metadata.clone(),
        piece_header,
        identity,
        peer_ip,
        Some(snapshot.coordinator.clone()),
    )
    .await;
    drop(snapshot);
    result
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
    fn concurrent_creators_publish_one_disk_store_and_preserve_its_data() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let reg = SeedRegistry::new();
        let ih = [2u8; 20];
        let root = tempfile::tempdir().unwrap();
        let dir = root.path().join("same-infohash");
        let starts = Arc::new(std::sync::Barrier::new(3));
        let factories = Arc::new(AtomicUsize::new(0));
        let mut threads = Vec::new();
        for _ in 0..2 {
            let reg = reg.clone();
            let dir = dir.clone();
            let starts = starts.clone();
            let factories = factories.clone();
            threads.push(std::thread::spawn(move || {
                starts.wait();
                reg.get_or_create(ih, || {
                    factories.fetch_add(1, Ordering::SeqCst);
                    let mut store = PieceStore::new_disk(4, 4, 1024, dir).unwrap();
                    store.put_chunk(0, 0, b"data");
                    std::thread::sleep(std::time::Duration::from_millis(20));
                    store
                })
            }));
        }
        starts.wait();
        let a = threads.remove(0).join().unwrap();
        let b = threads.remove(0).join().unwrap();
        assert_eq!(factories.load(Ordering::SeqCst), 1);
        assert!(Arc::ptr_eq(&a, &b));
        assert_eq!(
            a.blocking_lock().chunk(0, 0).unwrap().as_ref(),
            b"data",
            "a racing caller must not wipe or drop the published disk store"
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
    fn broadcast_transport_is_found_by_its_swarm_infohash_only() {
        use ace_wire::bencode::Bencode;
        let mut d = std::collections::BTreeMap::new();
        d.insert(b"name".to_vec(), Bencode::Bytes(b"Synthetic".to_vec()));
        d.insert(b"authmethod".to_vec(), Bencode::Bytes(b"RSA".to_vec()));
        d.insert(b"pubkey".to_vec(), Bencode::Bytes(b"k".to_vec()));
        d.insert(b"piece_length".to_vec(), Bencode::Int(65_536));
        d.insert(b"chunk_length".to_vec(), Bencode::Int(16_384));
        d.insert(b"bitrate".to_vec(), Bencode::Int(1));
        let transport = ace_wire::transport::encode_transport(&Bencode::Dict(d));
        let ih = ace_wire::infohash::infohash_of_transport(&transport);
        let cid = ace_wire::infohash::transport_file_hash(&transport);

        let reg = SeedRegistry::new();
        assert!(reg.broadcast_transport_for_infohash(&ih).is_none());
        assert!(reg.broadcast_transport_for_content_id(&cid).is_none());
        let (_store, lease) =
            reg.lease_broadcast(ih, cid, transport.clone(), || PieceStore::new(4, 4, 1024));
        assert_eq!(
            reg.broadcast_transport_for_infohash(&ih).as_deref(),
            Some(&transport)
        );
        assert_eq!(
            reg.broadcast_transport_for_content_id(&cid).as_deref(),
            Some(&transport)
        );
        assert!(reg.broadcast_transport_for_content_id(&ih).is_none());
        assert!(
            reg.broadcast_transport_for_infohash(&cid).is_none(),
            "a content id is not a swarm infohash"
        );
        drop(lease);
        assert!(reg.broadcast_transport_for_infohash(&ih).is_none());
        // Metadata registered outside a broadcast lease is not an originated broadcast.
        reg.register_metadata(cid, transport.clone());
        assert!(reg.broadcast_transport_for_infohash(&ih).is_none());
        assert!(reg.broadcast_transport_for_content_id(&cid).is_none());
        let wrong_cid = [8; 20];
        let (_, _lease) =
            reg.lease_broadcast(ih, wrong_cid, transport, || PieceStore::new(4, 4, 1024));
        assert!(
            reg.broadcast_transport_for_content_id(&wrong_cid).is_none(),
            "broadcast ownership cannot bypass raw hash verification"
        );
        assert!(
            reg.broadcast_transport_for_content_id(&cid).is_none(),
            "another key's broadcast owner cannot authorize generic metadata"
        );
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

    #[test]
    fn reaper_evicts_idle_ownerless_leech_entry() {
        let reg = SeedRegistry::new();
        let ih = [1u8; 20];
        // An ownerless entry (created outside the lease API — producers == 0).
        reg.get_or_create(ih, || PieceStore::new(4, 4, 1024));
        let evicted = reg.reap(std::time::Duration::from_secs(0));
        assert_eq!(evicted, 1, "idle ownerless leech entry is reaped");
        assert!(!reg.serves(&ih));
    }

    #[test]
    fn reaper_spares_owned_entries_even_when_idle() {
        let reg = SeedRegistry::new();
        let leech = [1u8; 20];
        let bcast_ih = [2u8; 20];
        let bcast_cid = [3u8; 20];
        // Live leases (producers > 0). mem::forget so only the reaper could reclaim them.
        std::mem::forget(reg.lease_store(leech, || PieceStore::new(4, 4, 1024)).1);
        std::mem::forget(
            reg.lease_broadcast(bcast_ih, bcast_cid, vec![9], || PieceStore::new(4, 4, 1024))
                .1,
        );
        let evicted = reg.reap(std::time::Duration::from_secs(0));
        assert_eq!(evicted, 0, "owned entries are never reaped, even when idle");
        assert!(reg.serves(&leech), "actively-owned leech entry survives");
        assert!(
            reg.serves(&bcast_ih) && reg.serves(&bcast_cid),
            "broadcast survives"
        );
    }

    #[test]
    fn get_touches_last_active_so_an_ownerless_entry_is_not_reaped() {
        let reg = SeedRegistry::new();
        let ih = [8u8; 20];
        reg.get_or_create(ih, || PieceStore::new(4, 4, 1024)); // ownerless
        let _ = reg.get(&ih); // touch
        let evicted = reg.reap(std::time::Duration::from_secs(3600));
        assert_eq!(evicted, 0);
        assert!(reg.serves(&ih));
    }

    #[test]
    fn coordinator_for_is_stable_per_infohash() {
        let reg = SeedRegistry::new();
        let ih = [11u8; 20];
        let (_s, _l) = reg.lease_store(ih, || PieceStore::new(4, 4, 1024));
        let c1 = reg.coordinator_for(&ih, 4).unwrap();
        let c2 = reg.coordinator_for(&ih, 4).unwrap();
        assert!(Arc::ptr_eq(&c1, &c2), "one coordinator per served infohash");
        assert!(
            reg.coordinator_for(&[99u8; 20], 4).is_none(),
            "no coordinator for an unserved infohash"
        );
    }
}

#[cfg(test)]
mod inbound_policy_tests;
