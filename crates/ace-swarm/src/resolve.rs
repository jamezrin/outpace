//! Resolve a stream identifier to a downloadable [`StreamInfo`].
//!
//! Two identifier shapes:
//!   * **infohash** — a 40-hex BitTorrent infohash; usable directly with default live
//!     geometry (peers advertise their piece range in the handshake `mi`).
//!   * **content-id** — locates an `AceStreamTransport` metadata file which, once fetched
//!     over the network (BEP-9 ut_metadata) and decoded, yields the infohash + geometry +
//!     trackers. The fetch is network-native (no Acestream API); see [`stream_info_from_transport`]
//!     for the pure decode half. The ut_metadata exchange is the remaining live-gated step
//!     (documented in the design spec).

use crate::types::StreamInfo;
use ace_peer::session::PeerSession;
use ace_wire::extended::{ExtendedHandshake, NodeFields, OutgoingExtendedHandshake};
use ace_wire::handshake::random_peer_id;
use ace_wire::identity::Identity;
use ace_wire::infohash::infohash_of_transport;
use ace_wire::message::PeerMessage;
use ace_wire::transport::decode_transport;
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncWrite};

/// Default live geometry when only an infohash is known: 1 MiB pieces / 16 KiB chunks.
pub const DEFAULT_PIECE_LENGTH: u64 = 1_048_576;
pub const DEFAULT_CHUNK_LENGTH: u64 = 16_384;

/// The ext id we assign to `ut_metadata` in our `m` dict (what the peer addresses replies to).
const OUR_UT_METADATA_ID: i64 = 2;
/// How many messages to read while waiting for the peer's extended handshake.
const HANDSHAKE_READ_BUDGET: usize = 32;

#[derive(Debug, PartialEq, Eq)]
pub enum ResolveError {
    BadInfohash,
    Transport(&'static str),
    /// A peer/network step failed during content-id resolution.
    Peer(&'static str),
}

/// Build a [`StreamInfo`] from raw `AceStreamTransport` file bytes (the pure half of
/// content-id resolution): infohash = SHA1(file), geometry + trackers from the descriptor.
pub fn stream_info_from_transport(bytes: &[u8]) -> Result<StreamInfo, ResolveError> {
    let d = decode_transport(bytes).map_err(|_| ResolveError::Transport("decode failed"))?;
    Ok(StreamInfo {
        infohash: infohash_of_transport(bytes),
        piece_length: d.piece_length,
        chunk_length: d.chunk_length,
        trackers: d.trackers,
    })
}

/// Build a [`StreamInfo`] from a 40-char hex infohash with default live geometry. `trackers`
/// are supplied separately (config defaults / DHT), since a bare infohash carries none.
pub fn stream_info_from_infohash(hex: &str, trackers: Vec<String>) -> Result<StreamInfo, ResolveError> {
    let bytes = decode_hex20(hex).ok_or(ResolveError::BadInfohash)?;
    Ok(StreamInfo {
        infohash: bytes,
        piece_length: DEFAULT_PIECE_LENGTH,
        chunk_length: DEFAULT_CHUNK_LENGTH,
        trackers,
    })
}

/// Decode a 40-hex content-id/infohash into 20 bytes (the metadata-swarm handshake key).
pub fn hex20(hex: &str) -> Result<[u8; 20], ResolveError> {
    decode_hex20(hex).ok_or(ResolveError::BadInfohash)
}

/// Resolve a content-id to a [`StreamInfo`] over an already-connected peer, by fetching the
/// `AceStreamTransport` metadata via BEP-9 `ut_metadata` and decoding it.
///
/// The content-id (`handshake_infohash`) is the metadata-swarm key used for the BT handshake;
/// the returned `StreamInfo` carries the *real* infohash — `SHA1` of the fetched transport
/// file — plus geometry and trackers. No Acestream HTTP/index API is involved.
pub async fn resolve_via_peer<S: AsyncRead + AsyncWrite + Unpin>(
    session: &mut PeerSession<S>,
    handshake_infohash: [u8; 20],
    identity: &Identity,
) -> Result<StreamInfo, ResolveError> {
    session
        .perform_handshake(handshake_infohash, random_peer_id())
        .await
        .map_err(|_| ResolveError::Peer("BT handshake failed"))?;

    let hs = OutgoingExtendedHandshake {
        ace_metadata_version: 1,
        ut_metadata_id: OUR_UT_METADATA_ID,
        mi: None,
        node: NodeFields { ts: 5000, ..NodeFields::default() },
        peer_ip: None,
    };
    session
        .send_signed_extended_handshake(&hs, identity)
        .await
        .map_err(|_| ResolveError::Peer("send extended handshake failed"))?;

    let (peer_ut_id, metadata_size) = read_metadata_params(session).await?;
    let blob = session
        .fetch_metadata(peer_ut_id, metadata_size)
        .await
        .map_err(|_| ResolveError::Peer("ut_metadata fetch failed"))?;
    stream_info_from_transport(&blob)
}

/// Read messages until the peer's extended handshake arrives; extract its `ut_metadata` ext id
/// and advertised `metadata_size`.
async fn read_metadata_params<S: AsyncRead + AsyncWrite + Unpin>(
    session: &mut PeerSession<S>,
) -> Result<(u8, usize), ResolveError> {
    for _ in 0..HANDSHAKE_READ_BUDGET {
        let msg = session
            .read_message()
            .await
            .map_err(|_| ResolveError::Peer("closed before extended handshake"))?;
        if let PeerMessage::Extended { ext_id: 0, payload } = msg {
            let eh = ExtendedHandshake::parse(&payload)
                .map_err(|_| ResolveError::Peer("bad extended handshake"))?;
            let ut_id = eh.ut_metadata_id().ok_or(ResolveError::Peer("peer has no ut_metadata"))?;
            let size = eh.metadata_size().ok_or(ResolveError::Peer("peer sent no metadata_size"))?;
            if !(0..=255).contains(&ut_id) || size <= 0 {
                return Err(ResolveError::Peer("invalid ut_metadata params"));
            }
            return Ok((ut_id as u8, size as usize));
        }
    }
    Err(ResolveError::Peer("no extended handshake"))
}

/// A small TTL cache of resolved `content-id → StreamInfo` so repeated `open()`s of the same
/// stream don't re-fetch metadata.
pub struct ResolveCache {
    entries: Mutex<HashMap<String, (StreamInfo, Instant)>>,
    ttl: Duration,
}

impl ResolveCache {
    pub fn new(ttl: Duration) -> Self {
        ResolveCache { entries: Mutex::new(HashMap::new()), ttl }
    }

    /// Return a cached, unexpired `StreamInfo` for `key`, if any.
    pub fn get(&self, key: &str) -> Option<StreamInfo> {
        let map = self.entries.lock().unwrap();
        map.get(key).and_then(|(info, at)| (at.elapsed() < self.ttl).then(|| info.clone()))
    }

    /// Store `info` under `key` with the current time.
    pub fn put(&self, key: &str, info: StreamInfo) {
        self.entries.lock().unwrap().insert(key.to_string(), (info, Instant::now()));
    }
}

fn decode_hex20(hex: &str) -> Option<[u8; 20]> {
    if hex.len() != 40 {
        return None;
    }
    let mut out = [0u8; 20];
    for (i, b) in out.iter_mut().enumerate() {
        *b = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).ok()?;
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use cbc::cipher::{block_padding::Pkcs7, BlockModeEncrypt, KeyIvInit};

    // Wrap raw bencode into a transport file under the global key/IV (mirrors transport.rs).
    fn make_transport(plaintext: &[u8]) -> Vec<u8> {
        type Enc = cbc::Encryptor<aes::Aes128>;
        let body = Enc::new_from_slices(&ace_wire::transport::TRANSPORT_KEY, &ace_wire::transport::TRANSPORT_IV)
            .unwrap()
            .encrypt_padded_vec::<Pkcs7>(plaintext);
        let mut out = b"AceStreamTransport\x00\x02".to_vec();
        out.extend_from_slice(&body);
        out
    }

    #[test]
    fn transport_yields_stream_info() {
        let tf = make_transport(
            b"d12:chunk_lengthi16384e12:piece_lengthi1048576e8:trackersl18:udp://t.example:80ee",
        );
        let si = stream_info_from_transport(&tf).unwrap();
        assert_eq!(si.piece_length, 1_048_576);
        assert_eq!(si.chunk_length, 16_384);
        assert_eq!(si.chunks_per_piece(), 64);
        assert_eq!(si.trackers, vec!["udp://t.example:80".to_string()]);
        assert_eq!(si.infohash, infohash_of_transport(&tf));
    }

    #[test]
    fn infohash_form_uses_default_geometry() {
        let hex = "0123456789abcdef0123456789abcdef01234567";
        let si = stream_info_from_infohash(hex, vec!["udp://x:1".into()]).unwrap();
        assert_eq!(si.piece_length, DEFAULT_PIECE_LENGTH);
        assert_eq!(si.chunk_length, DEFAULT_CHUNK_LENGTH);
        assert_eq!(si.infohash[0], 0x01);
        assert_eq!(si.infohash[19], 0x67);
    }

    #[test]
    fn bad_infohash_rejected() {
        assert_eq!(stream_info_from_infohash("xyz", vec![]), Err(ResolveError::BadInfohash));
        assert_eq!(stream_info_from_infohash(&"z".repeat(40), vec![]), Err(ResolveError::BadInfohash));
    }

    #[test]
    fn cache_returns_stored_value_then_expires() {
        let c = ResolveCache::new(Duration::from_millis(40));
        let info = StreamInfo { infohash: [7; 20], piece_length: 1, chunk_length: 1, trackers: vec![] };
        assert_eq!(c.get("k"), None);
        c.put("k", info.clone());
        assert_eq!(c.get("k"), Some(info));
        std::thread::sleep(Duration::from_millis(55));
        assert_eq!(c.get("k"), None, "entry expires after the TTL");
    }
}
