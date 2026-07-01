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

use ace_swarm::listen::SeedRegistry;
use ace_swarm::store::PieceStore;
use ace_wire::bencode::Bencode;
use ace_wire::infohash::infohash_of_transport;
use ace_wire::live_auth::LiveSourceAuth;
use ace_wire::transport::encode_transport;
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::Mutex;

/// Acestream's live geometry (matches the real engine's default for this bitrate class —
/// see notes 19/20; our own originated content doesn't need to match exactly, but doing so
/// avoids being trivially fingerprinted as a different implementation).
pub const PIECE_LENGTH: u64 = 1_048_576;
pub const CHUNK_LENGTH: u64 = 16_384;

/// A minted, originated broadcast: its infohash, the transport file bytes (for persistence /
/// future `ut_metadata` serving), the shared piece store peers are served from, and the
/// signing identity the ingest handler uses to sign each piece for real.
#[derive(Clone)]
pub struct Broadcast {
    pub infohash: [u8; 20],
    pub transport_bytes: Arc<Vec<u8>>,
    pub store: Arc<Mutex<PieceStore>>,
    pub auth: Arc<LiveSourceAuth>,
}

/// Maps a human `{name}` (the ingest path segment) to its minted `Broadcast`. Separate from
/// `SeedRegistry` (which is keyed by infohash, for the wire protocol) — this is the
/// operator-facing name -> infohash lookup.
pub struct BroadcastRegistry {
    by_name: Mutex<BTreeMap<String, Broadcast>>,
}

impl BroadcastRegistry {
    pub fn new() -> Arc<Self> {
        Arc::new(BroadcastRegistry { by_name: Mutex::new(BTreeMap::new()) })
    }

    /// The broadcast already minted under `name`, if any.
    pub async fn get(&self, name: &str) -> Option<Broadcast> {
        self.by_name.lock().await.get(name).cloned()
    }

    /// Mint (or return the existing) broadcast for `name`: build + encode the transport
    /// descriptor, compute its infohash, and register an empty `PieceStore` for it in the
    /// shared `seed_registry` (making it immediately servable via S1/S2's existing serve
    /// path). Idempotent per `name` for the life of this registry — re-`PUT`ting the same
    /// name resumes the same broadcast rather than minting a new infohash each time.
    ///
    /// Returns `(broadcast, freshly_minted)` — the bool is `true` only the first time a
    /// given `name` is minted, so a caller can do mint-only setup (like starting the
    /// tracker/DHT self-announce loop) exactly once instead of re-triggering it on every
    /// resumed `PUT`.
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
        let auth = LiveSourceAuth::generate();
        let descriptor = build_descriptor(name, title, trackers, &auth.pubkey_der());
        let transport_bytes = encode_transport(&descriptor);
        let infohash = infohash_of_transport(&transport_bytes);
        let store = seed_registry
            .get_or_create(infohash, || PieceStore::new(PIECE_LENGTH, CHUNK_LENGTH, store_bytes));
        let broadcast = Broadcast {
            infohash,
            transport_bytes: Arc::new(transport_bytes),
            store,
            auth: Arc::new(auth),
        };
        map.insert(name.to_string(), broadcast.clone());
        (broadcast, true)
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
    d.insert(b"bitrate".to_vec(), Bencode::Int(0));
    d.insert(b"quality".to_vec(), Bencode::Bytes(b"auto".to_vec()));
    d.insert(b"categories".to_vec(), Bencode::List(vec![Bencode::Bytes(b"other".to_vec())]));
    d.insert(b"authmethod".to_vec(), Bencode::Bytes(b"RSA".to_vec()));
    d.insert(b"pubkey".to_vec(), Bencode::Bytes(pubkey_der.to_vec()));
    d.insert(
        b"trackers".to_vec(),
        Bencode::List(trackers.iter().map(|t| Bencode::Bytes(t.as_bytes().to_vec())).collect()),
    );
    d.insert(b"allow_public_trackers".to_vec(), Bencode::Int(1));
    // `name` above is actually reused for outpace's internal identifier too; keep the
    // ingest path segment discoverable via a dedicated key for operator tooling.
    d.insert(b"outpace_name".to_vec(), Bencode::Bytes(name.as_bytes().to_vec()));
    Bencode::Dict(d)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ace_wire::transport::decode_transport;

    fn registry() -> (Arc<BroadcastRegistry>, SeedRegistry) {
        (BroadcastRegistry::new(), SeedRegistry::new())
    }

    #[tokio::test]
    async fn minted_broadcast_decodes_to_a_live_descriptor_with_our_pubkey() {
        let (reg, seed) = registry();
        let (bc, fresh) = reg
            .start_or_resume("test", "Test Stream", &["udp://t.example:1/announce".into()], &seed, 1 << 20)
            .await;
        assert!(fresh, "first mint of this name must report fresh=true");
        let decoded = decode_transport(&bc.transport_bytes).unwrap();
        assert_eq!(decoded.name, "Test Stream");
        assert_eq!(decoded.piece_length, PIECE_LENGTH);
        assert_eq!(decoded.chunk_length, CHUNK_LENGTH);
        assert!(decoded.is_live, "no pieces key -> live");
        // DER SPKI-encoded RSA pubkey (note 25 captured 124 bytes for one specific 768-bit
        // key; the exact length varies +/- a couple bytes with modulus/exponent leading-zero
        // padding, so assert a range rather than pinning one instance's exact byte count).
        assert!(
            (120..=128).contains(&decoded.pubkey.len()),
            "expected a DER SPKI RSA pubkey, got {} bytes",
            decoded.pubkey.len()
        );
        assert_eq!(decoded.trackers, vec!["udp://t.example:1/announce".to_string()]);
    }

    #[tokio::test]
    async fn infohash_matches_sha1_of_the_minted_transport_bytes() {
        let (reg, seed) = registry();
        let (bc, _fresh) = reg.start_or_resume("test", "Test", &[], &seed, 1 << 20).await;
        assert_eq!(bc.infohash, infohash_of_transport(&bc.transport_bytes));
    }

    #[tokio::test]
    async fn store_is_registered_in_the_shared_seed_registry_under_the_infohash() {
        let (reg, seed) = registry();
        let (bc, _fresh) = reg.start_or_resume("test", "Test", &[], &seed, 1 << 20).await;
        assert!(seed.serves(&bc.infohash), "minted broadcast must be immediately servable");
    }

    #[tokio::test]
    async fn re_putting_the_same_name_resumes_rather_than_reminting() {
        let (reg, seed) = registry();
        let (a, first_fresh) = reg.start_or_resume("test", "Test", &[], &seed, 1 << 20).await;
        let (b, second_fresh) =
            reg.start_or_resume("test", "A different title now", &[], &seed, 1 << 20).await;
        assert_eq!(a.infohash, b.infohash, "same name -> same, already-minted broadcast");
        assert!(first_fresh, "first PUT mints");
        assert!(!second_fresh, "second PUT of the same name resumes, not a fresh mint");
    }

    #[tokio::test]
    async fn different_names_get_different_infohashes() {
        let (reg, seed) = registry();
        let (a, _) = reg.start_or_resume("a", "A", &[], &seed, 1 << 20).await;
        let (b, _) = reg.start_or_resume("b", "B", &[], &seed, 1 << 20).await;
        assert_ne!(a.infohash, b.infohash);
    }
}
