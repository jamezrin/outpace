//! B1: HTTP broadcast ingest -> Acestream swarm origination.
//!
//! `PUT /broadcast/{name}` (see `http.rs`) accepts a chunked MPEG-TS body, chunks it into
//! Acestream-geometry pieces (`ace_wire::chunker::TsChunker`), mints a transport file +
//! infohash (`ace_wire::transport::encode_transport`), and registers the piece store in the
//! **existing** `SeedRegistry` shared with the S1/S2 serve path (`SeederSession`,
//! `PeerListener`) — so a freshly-originated broadcast is servable to peers with **no new
//! serving code**, the same way reciprocated/downloaded pieces already are.
//!
//! Per-piece signing (B0, cracked — see `docs/protocol/notes/27-b0-signing-cracked.md`):
//! pieces are signed for real with `ace_wire::signing_chunker::SigningChunker` (RSASSA-
//! PKCS1-v1_5 over SHA1 of the piece payload, signature embedded as the piece's trailing
//! bytes) using the broadcast's own `LiveSourceAuth` identity — not a placeholder.

use crate::ace_provider::build_piece_store;
use crate::broadcast_persist::{BroadcastPersist, PersistedBroadcast};
use crate::config::CacheType;
use ace_swarm::listen::SeedRegistry;
use ace_swarm::store::PieceStore;
use ace_wire::bencode::Bencode;
use ace_wire::infohash::{infohash_of_descriptor, infohash_of_transport, transport_file_hash};
use ace_wire::live_auth::LiveSourceAuth;
use ace_wire::transport::{decode_transport, encode_transport};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::Mutex;

/// Acestream source-node live geometry (captured from the official `--stream-source-node`
/// default for an 8375 kbps MPEG-TS source).
pub const PIECE_LENGTH: u64 = 65_536;
pub const CHUNK_LENGTH: u64 = 16_384;
pub const DEFAULT_BROADCAST_BITRATE: i64 = 8375;

/// How often (in pieces produced) the ingest cursor is flushed to disk during a live ingest.
/// On a daemon restart we resume at `persisted_next_piece + CURSOR_PERSIST_INTERVAL`, which
/// guarantees a piece number is never reused (reuse corrupts followers) at the cost of a
/// ≤`CURSOR_PERSIST_INTERVAL`-piece forward gap that the consumer's skip-evicted-gap path
/// already tolerates. 128 pieces ≈ 8 MiB ≈ ~8 s at the 8375 kbps default. See note 25's
/// `.restart` file, whose semantics this mirrors.
pub const CURSOR_PERSIST_INTERVAL: u64 = 128;

/// Immutable record parts a cursor needs to rewrite its broadcast's on-disk record when it
/// flushes a new `next_piece`.
struct CursorSink {
    persist: BroadcastPersist,
    name: String,
    transport: Vec<u8>,
    key_pkcs1_pem: String,
}

/// Tracks a broadcast's next piece index for ingest-resume continuity, and (when persistence
/// is enabled) throttle-persists it so numbering survives a daemon restart. Shared between the
/// registry's `Broadcast` and each `BroadcastIngest` task via `Arc`.
pub struct BroadcastCursor {
    next: AtomicU64,
    last_persisted: AtomicU64,
    removed: AtomicBool,
    sink: Option<CursorSink>,
}

impl BroadcastCursor {
    fn new(start: u64, sink: Option<CursorSink>) -> Arc<Self> {
        Arc::new(BroadcastCursor {
            next: AtomicU64::new(start),
            last_persisted: AtomicU64::new(start),
            removed: AtomicBool::new(false),
            sink,
        })
    }

    /// A cursor with no persistence sink — continuity tracking only (tests).
    #[cfg(test)]
    pub(crate) fn detached(start: u64) -> Arc<Self> {
        Self::new(start, None)
    }

    /// The piece index a new ingest's chunker should start from.
    pub fn start_piece(&self) -> u64 {
        self.next.load(Ordering::SeqCst)
    }

    /// Record that pieces up to (but not including) `next_piece` now exist. Monotonic — an
    /// out-of-order lower value is ignored. Persists to disk once the cursor has advanced at
    /// least `CURSOR_PERSIST_INTERVAL` past the last flush.
    pub fn advance_to(&self, next_piece: u64) {
        let prev = self.next.fetch_max(next_piece, Ordering::SeqCst);
        let now = prev.max(next_piece);
        if now >= self.last_persisted.load(Ordering::SeqCst) + CURSOR_PERSIST_INTERVAL {
            self.persist(now);
        }
    }

    /// Force-persist the current cursor (called when an ingest finishes).
    pub fn flush(&self) {
        self.persist(self.next.load(Ordering::SeqCst));
    }

    /// Stop persisting — a deleted broadcast must not have its file resurrected by a stale
    /// in-flight ingest's later flush.
    pub fn mark_removed(&self) {
        self.removed.store(true, Ordering::SeqCst);
    }

    fn persist(&self, next_piece: u64) {
        if self.removed.load(Ordering::SeqCst) {
            return;
        }
        let Some(sink) = &self.sink else { return };
        self.last_persisted.store(next_piece, Ordering::SeqCst);
        let rec = PersistedBroadcast {
            transport: sink.transport.clone(),
            key_pkcs1_pem: sink.key_pkcs1_pem.clone(),
            next_piece,
        };
        if let Err(e) = sink.persist.save(&sink.name, &rec) {
            crate::alog!("[broadcast] {}: cursor persist failed: {e}", sink.name);
        }
    }
}

/// A minted, originated broadcast: its infohash, the transport file bytes (for persistence /
/// future `ut_metadata` serving), the shared piece store peers are served from, and the
/// signing identity the ingest handler uses to sign each piece for real.
#[derive(Clone)]
pub struct Broadcast {
    pub infohash: [u8; 20],
    pub content_id: [u8; 20],
    pub transport_bytes: Arc<Vec<u8>>,
    pub store: Arc<Mutex<PieceStore>>,
    pub auth: Arc<LiveSourceAuth>,
    /// Piece-numbering cursor for ingest-resume continuity (and its throttled persistence).
    pub cursor: Arc<BroadcastCursor>,
}

/// Maps a human `{name}` (the ingest path segment) to its minted `Broadcast`. Separate from
/// `SeedRegistry` (which is keyed by infohash, for the wire protocol) — this is the
/// operator-facing name -> infohash lookup.
pub struct BroadcastRegistry {
    by_name: Mutex<BTreeMap<String, Broadcast>>,
    /// `None` disables persistence (unit tests / disk-less runs); `Some` reads and writes
    /// `<data_dir>/broadcasts/`.
    persist: Option<BroadcastPersist>,
    /// Backend the originated/reloaded piece stores use (daemon-global, unlike the per-call
    /// `store_bytes` budget). Defaults to `Memory`; production sets it from config.
    cache_type: CacheType,
    /// Root dir for disk-mode piece files (per-infohash subdir derived from this).
    cache_dir: PathBuf,
}

impl BroadcastRegistry {
    /// A disk-less registry: minted broadcasts live only in memory (used by tests). Piece data is
    /// always memory-backed here.
    pub fn new() -> Arc<Self> {
        Arc::new(BroadcastRegistry {
            by_name: Mutex::new(BTreeMap::new()),
            persist: None,
            cache_type: CacheType::Memory,
            cache_dir: PathBuf::new(),
        })
    }

    /// A registry that persists minted broadcasts under `<data_dir>/broadcasts/` and reloads
    /// them across restarts. `cache_type` / `cache_dir` select where originated piece data lives
    /// (mirroring the leech path's cache config).
    pub fn with_persist(data_dir: &Path, cache_type: CacheType, cache_dir: PathBuf) -> Arc<Self> {
        Arc::new(BroadcastRegistry {
            by_name: Mutex::new(BTreeMap::new()),
            persist: Some(BroadcastPersist::new(data_dir)),
            cache_type,
            cache_dir,
        })
    }

    /// The broadcast already minted under `name`, if any.
    pub async fn get(&self, name: &str) -> Option<Broadcast> {
        self.by_name.lock().await.get(name).cloned()
    }

    /// Remove the on-disk piece cache for `infohash` (a no-op in memory mode). Best-effort; called
    /// on broadcast teardown so a stopped broadcast does not leave its cache directory behind.
    pub fn remove_cache_dir(&self, infohash: &[u8; 20]) {
        if self.cache_type == CacheType::Disk {
            let dir = self.cache_dir.join(crate::ace_provider::infohash_hex(infohash));
            let _ = std::fs::remove_dir_all(dir);
        }
    }

    /// Mint, resume-from-memory, or reload-from-disk the broadcast for `name`, and register its
    /// `PieceStore` + metadata in the shared `seed_registry` (making it immediately servable via
    /// S1/S2's existing serve path). Idempotent per `name`: a re-`PUT` resumes the same
    /// broadcast (same infohash + continuing piece cursor) rather than minting a new one.
    ///
    /// Returns `(broadcast, freshly_minted)` — the bool is `true` only when a brand-new
    /// identity is minted (not on an in-memory resume or a disk reload), so a caller can do
    /// mint-only setup (like starting the tracker/DHT self-announce loop) exactly once.
    pub async fn start_or_resume(
        &self,
        name: &str,
        title: &str,
        trackers: &[String],
        seed_registry: &SeedRegistry,
        store_bytes: u64,
    ) -> (Broadcast, bool) {
        let mut map = self.by_name.lock().await;
        if let Some(existing) = map.get(name) {
            return (existing.clone(), false);
        }
        // Reload a persisted identity before minting a fresh one, so the infohash survives a
        // restart and the piece cursor continues.
        if let Some(persist) = &self.persist {
            if let Some(rec) = persist.load(name) {
                if let Some(bc) = self.reconstruct(name, &rec, seed_registry, store_bytes) {
                    map.insert(name.to_string(), bc.clone());
                    return (bc, false);
                }
                crate::alog!("[broadcast] {name}: persisted record invalid; re-minting");
            }
        }
        let auth = LiveSourceAuth::generate();
        let descriptor = build_descriptor(name, title, trackers, &auth.pubkey_der());
        let infohash = infohash_of_descriptor(&descriptor)
            .expect("broadcast descriptor has official infohash fields");
        let transport_bytes = encode_transport(&descriptor);
        let content_id = transport_file_hash(&transport_bytes);
        seed_registry.register_metadata(content_id, transport_bytes.clone());
        let store = seed_registry.get_or_create(infohash, || {
            build_piece_store(
                PIECE_LENGTH,
                CHUNK_LENGTH,
                store_bytes,
                self.cache_type,
                &self.cache_dir,
                &infohash,
            )
        });
        // Persist the fresh identity (cursor at 0) before wiring the cursor's sink, so the
        // record exists immediately (matching the engine writing `.acelive`/`.sauth` at mint).
        let key_pem = auth.to_pkcs1_pem();
        if let Some(persist) = &self.persist {
            let rec = PersistedBroadcast {
                transport: transport_bytes.clone(),
                key_pkcs1_pem: key_pem.clone(),
                next_piece: 0,
            };
            if let Err(e) = persist.save(name, &rec) {
                crate::alog!("[broadcast] {name}: identity persist failed: {e}");
            }
        }
        let cursor = BroadcastCursor::new(0, self.sink_for(name, &transport_bytes, key_pem));
        let broadcast = Broadcast {
            infohash,
            content_id,
            transport_bytes: Arc::new(transport_bytes),
            store,
            auth: Arc::new(auth),
            cursor,
        };
        map.insert(name.to_string(), broadcast.clone());
        (broadcast, true)
    }

    /// Reload all persisted broadcasts into memory + `seed_registry` at daemon startup.
    /// Returns the reconstructed broadcasts so the caller can restart their announce loops.
    /// A no-op when persistence is disabled.
    pub async fn reload_persisted(
        &self,
        seed_registry: &SeedRegistry,
        store_bytes: u64,
    ) -> Vec<Broadcast> {
        let Some(persist) = &self.persist else {
            return Vec::new();
        };
        let mut map = self.by_name.lock().await;
        let mut out = Vec::new();
        for (name, rec) in persist.load_all() {
            if map.contains_key(&name) {
                continue;
            }
            match self.reconstruct(&name, &rec, seed_registry, store_bytes) {
                Some(bc) => {
                    map.insert(name.clone(), bc.clone());
                    out.push(bc);
                }
                None => crate::alog!("[broadcast] {name}: persisted record invalid; skipping"),
            }
        }
        out
    }

    /// Forget `name`: drop it from memory (marking its cursor removed so a stale in-flight
    /// ingest can't rewrite the file) and delete its persisted record. Returns the removed
    /// broadcast so the caller can drop it from `seed_registry`.
    pub async fn delete(&self, name: &str) -> Option<Broadcast> {
        let removed = self.by_name.lock().await.remove(name);
        if let Some(bc) = &removed {
            bc.cursor.mark_removed();
        }
        if let Some(persist) = &self.persist {
            if let Err(e) = persist.delete(name) {
                crate::alog!("[broadcast] {name}: delete failed: {e}");
            }
        }
        removed
    }

    /// Build a cursor persistence sink for `name`, or `None` when persistence is disabled.
    fn sink_for(&self, name: &str, transport: &[u8], key_pkcs1_pem: String) -> Option<CursorSink> {
        self.persist.clone().map(|persist| CursorSink {
            persist,
            name: name.to_string(),
            transport: transport.to_vec(),
            key_pkcs1_pem,
        })
    }

    /// Rebuild a `Broadcast` from a persisted record: restore the signing key, re-derive
    /// identity + geometry from the transport bytes, register the store + metadata, and seed
    /// the cursor at `next_piece + CURSOR_PERSIST_INTERVAL` (the no-reuse resume margin).
    /// Returns `None` if the record is semantically invalid (bad key, undecodable transport,
    /// or a pubkey that does not match the key).
    fn reconstruct(
        &self,
        name: &str,
        rec: &PersistedBroadcast,
        seed_registry: &SeedRegistry,
        store_bytes: u64,
    ) -> Option<Broadcast> {
        let auth = LiveSourceAuth::from_pkcs1_pem(&rec.key_pkcs1_pem).ok()?;
        let decoded = decode_transport(&rec.transport).ok()?;
        if decoded.pubkey != auth.pubkey_der() {
            return None;
        }
        let infohash = infohash_of_transport(&rec.transport);
        let content_id = transport_file_hash(&rec.transport);
        let piece_length = decoded.piece_length;
        let chunk_length = decoded.chunk_length;
        seed_registry.register_metadata(content_id, rec.transport.clone());
        let store = seed_registry.get_or_create(infohash, || {
            build_piece_store(
                piece_length,
                chunk_length,
                store_bytes,
                self.cache_type,
                &self.cache_dir,
                &infohash,
            )
        });
        let resume_at = rec.next_piece.saturating_add(CURSOR_PERSIST_INTERVAL);
        let cursor = BroadcastCursor::new(
            resume_at,
            self.sink_for(name, &rec.transport, rec.key_pkcs1_pem.clone()),
        );
        Some(Broadcast {
            infohash,
            content_id,
            transport_bytes: Arc::new(rec.transport.clone()),
            store,
            auth: Arc::new(auth),
            cursor,
        })
    }
}

/// Build the bencode descriptor dict for a fresh MPEG-TS live broadcast. Field set matches a
/// real engine-produced source-node transport (note 25): `name`, `piece_length`,
/// `chunk_length`, `bitrate`, `quality`, `categories`, `authmethod`, `pubkey`, `trackers`,
/// `allow_public_trackers`. No `pieces` key — live, matching `TransportDescriptor::is_live`.
fn build_descriptor(name: &str, title: &str, trackers: &[String], pubkey_der: &[u8]) -> Bencode {
    let mut d: BTreeMap<Vec<u8>, Bencode> = BTreeMap::new();
    d.insert(b"name".to_vec(), Bencode::Bytes(title.as_bytes().to_vec()));
    d.insert(b"piece_length".to_vec(), Bencode::Int(PIECE_LENGTH as i64));
    d.insert(b"chunk_length".to_vec(), Bencode::Int(CHUNK_LENGTH as i64));
    d.insert(b"bitrate".to_vec(), Bencode::Int(DEFAULT_BROADCAST_BITRATE));
    d.insert(b"quality".to_vec(), Bencode::Bytes(b"auto".to_vec()));
    d.insert(
        b"categories".to_vec(),
        Bencode::List(vec![Bencode::Bytes(b"other".to_vec())]),
    );
    d.insert(b"authmethod".to_vec(), Bencode::Bytes(b"RSA".to_vec()));
    d.insert(b"pubkey".to_vec(), Bencode::Bytes(pubkey_der.to_vec()));
    d.insert(
        b"trackers".to_vec(),
        Bencode::List(
            trackers
                .iter()
                .map(|t| Bencode::Bytes(t.as_bytes().to_vec()))
                .collect(),
        ),
    );
    d.insert(b"allow_public_trackers".to_vec(), Bencode::Int(1));
    // `name` above is actually reused for outpace's internal identifier too; keep the
    // ingest path segment discoverable via a dedicated key for operator tooling.
    d.insert(
        b"outpace_name".to_vec(),
        Bencode::Bytes(name.as_bytes().to_vec()),
    );
    Bencode::Dict(d)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn registry() -> (Arc<BroadcastRegistry>, SeedRegistry) {
        (BroadcastRegistry::new(), SeedRegistry::new())
    }

    fn tmp_dir() -> std::path::PathBuf {
        let d = std::env::temp_dir().join(format!("outpace-bc-test-{}", rand::random::<u64>()));
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    #[tokio::test]
    async fn minted_broadcast_decodes_to_a_live_descriptor_with_our_pubkey() {
        let (reg, seed) = registry();
        let (bc, fresh) = reg
            .start_or_resume(
                "test",
                "Test Stream",
                &["udp://t.example:1/announce".into()],
                &seed,
                1 << 20,
            )
            .await;
        assert!(fresh, "first mint of this name must report fresh=true");
        let decoded = decode_transport(&bc.transport_bytes).unwrap();
        assert_eq!(decoded.name, "Test Stream");
        assert_eq!(decoded.piece_length, PIECE_LENGTH);
        assert_eq!(decoded.chunk_length, CHUNK_LENGTH);
        assert_eq!(
            decoded.bitrate,
            Some(8375),
            "official source-node transports carry a nonzero bitrate hint"
        );
        assert!(decoded.is_live, "no pieces key -> live");
        // DER SPKI-encoded RSA pubkey (note 25 captured 124 bytes for one specific 768-bit
        // key; the exact length varies +/- a couple bytes with modulus/exponent leading-zero
        // padding, so assert a range rather than pinning one instance's exact byte count).
        assert!(
            (120..=128).contains(&decoded.pubkey.len()),
            "expected a DER SPKI RSA pubkey, got {} bytes",
            decoded.pubkey.len()
        );
        assert_eq!(
            decoded.trackers,
            vec!["udp://t.example:1/announce".to_string()]
        );
    }

    #[tokio::test]
    async fn infohash_matches_official_descriptor_hash_of_the_minted_transport_bytes() {
        let (reg, seed) = registry();
        let (bc, _fresh) = reg
            .start_or_resume("test", "Test", &[], &seed, 1 << 20)
            .await;
        assert_eq!(bc.infohash, infohash_of_transport(&bc.transport_bytes));
    }

    #[tokio::test]
    #[ignore = "superseded by Task 5 lease-based cache cleanup (removes remove_cache_dir); see docs/superpowers/plans/2026-07-06-inbound-seeding-lifecycle.md"]
    async fn disk_mode_writes_piece_files_and_teardown_removes_them() {
        let data_dir = tmp_dir();
        let cache_dir = data_dir.join("cache");
        let reg = BroadcastRegistry::with_persist(&data_dir, CacheType::Disk, cache_dir.clone());
        let seed = SeedRegistry::new();
        let (bc, _fresh) = reg
            .start_or_resume("disk", "Disk", &[], &seed, 1 << 20)
            .await;

        // Feed a full piece into the (disk-backed) origination store.
        {
            let mut guard = bc.store.lock().await;
            let chunks = guard.chunks_per_piece();
            for c in 0..chunks {
                guard.put_chunk(0, c, &vec![7u8; CHUNK_LENGTH as usize]);
            }
            assert!(guard.has_piece(0));
            assert_eq!(
                guard.chunk(0, 0).as_deref(),
                Some(&vec![7u8; CHUNK_LENGTH as usize][..]),
                "chunk reads back from disk"
            );
        }

        let hex = crate::ace_provider::infohash_hex(&bc.infohash);
        let piece_file = cache_dir.join(&hex).join("0.piece");
        assert!(
            piece_file.exists(),
            "disk backend wrote a piece file at {}",
            piece_file.display()
        );

        reg.remove_cache_dir(&bc.infohash);
        assert!(
            !cache_dir.join(&hex).exists(),
            "teardown removed the per-infohash cache dir"
        );
        std::fs::remove_dir_all(&data_dir).ok();
    }

    #[tokio::test]
    async fn store_is_registered_in_the_shared_seed_registry_under_the_infohash() {
        let (reg, seed) = registry();
        let (bc, _fresh) = reg
            .start_or_resume("test", "Test", &[], &seed, 1 << 20)
            .await;
        assert!(
            seed.serves(&bc.infohash),
            "minted broadcast must be immediately servable"
        );
    }

    #[tokio::test]
    async fn minted_broadcast_has_content_id_and_registers_metadata() {
        let (reg, seed) = registry();
        let (bc, fresh) = reg
            .start_or_resume(
                "chan",
                "Channel",
                &["udp://tracker.example:2710/announce".to_string()],
                &seed,
                1024 * 1024,
            )
            .await;
        assert!(fresh);
        assert_eq!(
            bc.content_id,
            ace_wire::infohash::transport_file_hash(&bc.transport_bytes)
        );
        assert_eq!(
            seed.metadata(&bc.content_id).as_deref().map(Vec::as_slice),
            Some(bc.transport_bytes.as_slice())
        );
        assert!(seed.serves(&bc.infohash));
        assert!(seed.serves(&bc.content_id));
    }

    #[tokio::test]
    async fn re_putting_the_same_name_resumes_rather_than_reminting() {
        let (reg, seed) = registry();
        let (a, first_fresh) = reg
            .start_or_resume("test", "Test", &[], &seed, 1 << 20)
            .await;
        let (b, second_fresh) = reg
            .start_or_resume("test", "A different title now", &[], &seed, 1 << 20)
            .await;
        assert_eq!(
            a.infohash, b.infohash,
            "same name -> same, already-minted broadcast"
        );
        assert!(first_fresh, "first PUT mints");
        assert!(
            !second_fresh,
            "second PUT of the same name resumes, not a fresh mint"
        );
    }

    #[tokio::test]
    async fn different_names_get_different_infohashes() {
        let (reg, seed) = registry();
        let (a, _) = reg.start_or_resume("a", "A", &[], &seed, 1 << 20).await;
        let (b, _) = reg.start_or_resume("b", "B", &[], &seed, 1 << 20).await;
        assert_ne!(a.infohash, b.infohash);
    }

    // ---- continuity cursor ----

    #[test]
    fn cursor_starts_at_its_seed_and_is_monotonic() {
        let c = BroadcastCursor::new(100, None);
        assert_eq!(c.start_piece(), 100);
        c.advance_to(50); // out-of-order lower value ignored
        assert_eq!(c.start_piece(), 100);
        c.advance_to(150);
        assert_eq!(c.start_piece(), 150);
    }

    #[tokio::test]
    async fn cursor_persists_on_the_interval_and_on_flush() {
        let dir = tmp_dir();
        let seed = SeedRegistry::new();
        let reg = BroadcastRegistry::with_persist(&dir, CacheType::Memory, PathBuf::new());
        let (bc, _) = reg.start_or_resume("c", "C", &[], &seed, 1 << 20).await;
        let persist = BroadcastPersist::new(&dir);

        // Freshly minted: cursor at 0 on disk.
        assert_eq!(persist.load("c").unwrap().next_piece, 0);
        // Advancing less than the interval does not rewrite the cursor.
        bc.cursor.advance_to(CURSOR_PERSIST_INTERVAL - 1);
        assert_eq!(persist.load("c").unwrap().next_piece, 0);
        // Crossing the interval persists.
        bc.cursor.advance_to(CURSOR_PERSIST_INTERVAL);
        assert_eq!(
            persist.load("c").unwrap().next_piece,
            CURSOR_PERSIST_INTERVAL
        );
        // flush() persists the exact current value.
        bc.cursor.advance_to(CURSOR_PERSIST_INTERVAL + 5);
        bc.cursor.flush();
        assert_eq!(
            persist.load("c").unwrap().next_piece,
            CURSOR_PERSIST_INTERVAL + 5
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    // ---- persistence across "restart" (fresh registry, same data_dir) ----

    #[tokio::test]
    async fn identity_survives_a_restart() {
        let dir = tmp_dir();
        let seed1 = SeedRegistry::new();
        let reg1 = BroadcastRegistry::with_persist(&dir, CacheType::Memory, PathBuf::new());
        let (a, fresh1) = reg1
            .start_or_resume("news", "News", &["udp://t:1/announce".into()], &seed1, 1 << 20)
            .await;
        assert!(fresh1, "first ever PUT mints");

        // Simulate a daemon restart: brand-new registry + seed registry, same data_dir.
        let seed2 = SeedRegistry::new();
        let reg2 = BroadcastRegistry::with_persist(&dir, CacheType::Memory, PathBuf::new());
        let (b, fresh2) = reg2
            .start_or_resume("news", "Ignored On Resume", &[], &seed2, 1 << 20)
            .await;
        assert!(!fresh2, "reloaded from disk, not a fresh mint");
        assert_eq!(a.infohash, b.infohash, "infohash stable across restart");
        assert_eq!(a.content_id, b.content_id);
        assert_eq!(a.transport_bytes, b.transport_bytes);
        assert_eq!(a.auth.pubkey_der(), b.auth.pubkey_der(), "same signing key");
        assert!(seed2.serves(&b.infohash), "reloaded broadcast is servable");
        assert!(seed2.serves(&b.content_id));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn cursor_resumes_with_the_no_reuse_margin_after_restart() {
        let dir = tmp_dir();
        let seed1 = SeedRegistry::new();
        let reg1 = BroadcastRegistry::with_persist(&dir, CacheType::Memory, PathBuf::new());
        let (a, _) = reg1.start_or_resume("m", "M", &[], &seed1, 1 << 20).await;
        a.cursor.advance_to(500);
        a.cursor.flush();

        let seed2 = SeedRegistry::new();
        let reg2 = BroadcastRegistry::with_persist(&dir, CacheType::Memory, PathBuf::new());
        let (b, _) = reg2.start_or_resume("m", "M", &[], &seed2, 1 << 20).await;
        assert_eq!(
            b.cursor.start_piece(),
            500 + CURSOR_PERSIST_INTERVAL,
            "resume past the last persisted piece so numbers are never reused"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    // ---- delete / reset ----

    #[tokio::test]
    async fn delete_then_put_re_mints_a_fresh_identity() {
        let dir = tmp_dir();
        let seed = SeedRegistry::new();
        let reg = BroadcastRegistry::with_persist(&dir, CacheType::Memory, PathBuf::new());
        let (a, _) = reg.start_or_resume("x", "X", &[], &seed, 1 << 20).await;

        let removed = reg.delete("x").await;
        assert!(removed.is_some());
        assert!(BroadcastPersist::new(&dir).load("x").is_none(), "file purged");

        let (b, fresh) = reg.start_or_resume("x", "X", &[], &seed, 1 << 20).await;
        assert!(fresh, "after delete, next PUT mints fresh");
        assert_ne!(a.infohash, b.infohash, "fresh key -> different infohash");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn removed_cursor_does_not_resurrect_the_file() {
        let dir = tmp_dir();
        let seed = SeedRegistry::new();
        let reg = BroadcastRegistry::with_persist(&dir, CacheType::Memory, PathBuf::new());
        let (a, _) = reg.start_or_resume("y", "Y", &[], &seed, 1 << 20).await;
        reg.delete("y").await;
        // A stale in-flight ingest's late flush must not recreate the record.
        a.cursor.advance_to(1000);
        a.cursor.flush();
        assert!(BroadcastPersist::new(&dir).load("y").is_none());
        let _ = std::fs::remove_dir_all(&dir);
    }

    // ---- invalid persisted record ----

    #[tokio::test]
    async fn a_record_whose_pubkey_mismatches_the_key_is_rejected_and_re_minted() {
        let dir = tmp_dir();
        let seed = SeedRegistry::new();
        // Mint one broadcast to get valid transport bytes...
        let reg0 = BroadcastRegistry::with_persist(&dir, CacheType::Memory, PathBuf::new());
        let (a, _) = reg0.start_or_resume("bad", "Bad", &[], &seed, 1 << 20).await;
        // ...then corrupt the record: keep the transport but swap in a different key.
        let other = LiveSourceAuth::generate();
        let persist = BroadcastPersist::new(&dir);
        persist
            .save(
                "bad",
                &PersistedBroadcast {
                    transport: (*a.transport_bytes).clone(),
                    key_pkcs1_pem: other.to_pkcs1_pem(),
                    next_piece: 0,
                },
            )
            .unwrap();

        let seed2 = SeedRegistry::new();
        let reg = BroadcastRegistry::with_persist(&dir, CacheType::Memory, PathBuf::new());
        let (b, fresh) = reg.start_or_resume("bad", "Bad", &[], &seed2, 1 << 20).await;
        assert!(fresh, "an invalid record is discarded and a fresh identity minted");
        assert_ne!(a.infohash, b.infohash);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
