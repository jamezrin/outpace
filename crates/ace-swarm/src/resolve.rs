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
use ace_wire::infohash::{infohash_of_descriptor, transport_file_hash};
use ace_wire::message::PeerMessage;
use ace_wire::transport::decode_transport;
use base64ct::{Base64, Encoding};
use rand::Rng;
use sha1::{Digest, Sha1};
use std::collections::HashMap;
use std::fmt::Write as _;
use std::sync::Mutex;
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;

/// Default live geometry when only an infohash is known: 1 MiB pieces / 16 KiB chunks.
pub const DEFAULT_PIECE_LENGTH: u64 = 1_048_576;
pub const DEFAULT_CHUNK_LENGTH: u64 = 16_384;

/// Upper bound on a metadata-swarm peer's advertised `metadata_size` (bytes).
///
/// The `metadata_size` in a peer's extended handshake is untrusted: it drives both the BEP-9
/// request fan-out (one request per 16 KiB block) and a `vec![0u8; metadata_size]` allocation
/// before any transport descriptor is decoded. Acestream `AceStreamTransport` files are small
/// bencoded dicts (geometry + trackers + pubkey), far under this ceiling — which also matches
/// the 1 MiB limit the signed catalog path imposes on the same descriptor. Anything larger is
/// a hostile peer trying to force a large fan-out/allocation, and is rejected before the fetch.
pub const MAX_METADATA_SIZE: usize = 1_048_576;

/// The ext id we assign to `ut_metadata` in our `m` dict (what the peer addresses replies to).
const OUR_UT_METADATA_ID: i64 = 2;
/// How many messages to read while waiting for the peer's extended handshake.
const HANDSHAKE_READ_BUDGET: usize = 32;
/// Official catalog hosts observed from the Linux engine's first `content_id` resolution.
const CATALOG_HOSTS: &[(&str, u16)] = &[
    ("5.252.161.191", 8081),
    ("77.120.105.88", 8081),
    ("64.227.119.64", 8081),
    ("163.172.187.185", 8081),
];
const CATALOG_HOST_TIMEOUT: Duration = Duration::from_secs(4);
const CATALOG_RESPONSE_LIMIT: usize = 1024 * 1024;
const CATALOG_SIGNING_SECRET: &[u8] =
    b"H!+:H1NnvvX\\x0bS'(;0/A\\nR{${\\n/3%1\\x0b*[r0o>QzNGKkXT@v\\x0b3DN;gx_66L2 {`F0,\\tKm>XoG~iY(\\x0bu]6E}\\t~07&H;9qE1d?d-A7S(";

#[derive(Debug, PartialEq, Eq)]
pub enum ResolveError {
    BadInfohash,
    Transport(&'static str),
    /// A signed catalog request/response step failed during content-id resolution.
    Catalog(&'static str),
    /// A peer/network step failed during content-id resolution.
    Peer(&'static str),
}

/// Build a [`StreamInfo`] from raw `AceStreamTransport` file bytes (the pure half of
/// content-id resolution): official swarm infohash + geometry + trackers from the descriptor.
pub fn stream_info_from_transport(bytes: &[u8]) -> Result<StreamInfo, ResolveError> {
    let d = decode_transport(bytes).map_err(|_| ResolveError::Transport("decode failed"))?;
    Ok(StreamInfo {
        infohash: infohash_of_descriptor(&d.raw)
            .map_err(|_| ResolveError::Transport("infohash failed"))?,
        piece_length: d.piece_length,
        chunk_length: d.chunk_length,
        trackers: d.trackers,
        // A valid RSA pubkey => pieces carry a modulus-length signature tail to strip; no
        // parseable pubkey => treat pieces as unsigned (don't strip).
        sig_len: ace_wire::live_auth::signature_len_from_pubkey_der(&d.pubkey).unwrap_or(0),
    })
}

/// Build a [`StreamInfo`] from a 40-char hex infohash with default live geometry. `trackers`
/// are supplied separately (config defaults / DHT), since a bare infohash carries none.
pub fn stream_info_from_infohash(
    hex: &str,
    trackers: Vec<String>,
) -> Result<StreamInfo, ResolveError> {
    let bytes = decode_hex20(hex).ok_or(ResolveError::BadInfohash)?;
    Ok(StreamInfo {
        infohash: bytes,
        piece_length: DEFAULT_PIECE_LENGTH,
        chunk_length: DEFAULT_CHUNK_LENGTH,
        trackers,
        // No transport => no pubkey to measure; assume the standard Acestream 768-bit
        // source key (96-byte signature tail). See DEFAULT_SIG_LEN.
        sig_len: crate::types::DEFAULT_SIG_LEN,
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
/// descriptor — plus geometry and trackers. No Acestream HTTP/index API is involved.
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
        node: NodeFields {
            ts: 5000,
            ..NodeFields::default()
        },
        peer_ip: None,
        metadata_size: None,
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

/// Resolve a content-id through the official signed transport catalog.
///
/// The catalog returns the encrypted `AceStreamTransport` bytes; checksum validation happens
/// before the existing pure transport decoder computes the real live swarm infohash.
pub async fn resolve_via_catalog(content_id: &str) -> Result<StreamInfo, ResolveError> {
    hex20(content_id)?;

    let mut last = ResolveError::Catalog("catalog request failed");
    for &(host, port) in CATALOG_HOSTS {
        match tokio::time::timeout(
            CATALOG_HOST_TIMEOUT,
            fetch_catalog_transport(host, port, content_id),
        )
        .await
        {
            Ok(Ok(transport)) => return stream_info_from_transport(&transport),
            Ok(Err(e)) => last = e,
            Err(_) => last = ResolveError::Catalog("catalog timeout"),
        }
    }
    Err(last)
}

async fn fetch_catalog_transport(
    host: &str,
    port: u16,
    content_id: &str,
) -> Result<Vec<u8>, ResolveError> {
    let request_random = {
        let mut rng = rand::thread_rng();
        rng.gen_range(1..=i64::MAX as u64)
    };
    let signature = catalog_signature(content_id, request_random);
    let path = format!(
        "/gettorrent?_n=3.2.11&_p=linux&_r={request_random}&_v=3021100&pid={content_id}&_s={signature}"
    );
    let request = format!(
        "GET {path} HTTP/1.1\r\nHost: {host}:{port}\r\nUser-Agent: outpace/0.1\r\nAccept: */*\r\nConnection: close\r\n\r\n"
    );

    let mut stream = TcpStream::connect((host, port))
        .await
        .map_err(|_| ResolveError::Catalog("catalog connect failed"))?;
    stream
        .write_all(request.as_bytes())
        .await
        .map_err(|_| ResolveError::Catalog("catalog write failed"))?;

    let mut response = Vec::new();
    let mut buf = [0u8; 8192];
    loop {
        let n = stream
            .read(&mut buf)
            .await
            .map_err(|_| ResolveError::Catalog("catalog read failed"))?;
        if n == 0 {
            break;
        }
        if response.len() + n > CATALOG_RESPONSE_LIMIT {
            return Err(ResolveError::Catalog("catalog response too large"));
        }
        response.extend_from_slice(&buf[..n]);
    }

    let body = http_response_body(&response)?;
    catalog_response_transport(&body)
}

fn catalog_signature(content_id: &str, request_random: u64) -> String {
    let mut hasher = Sha1::new();
    let signed = format!("_n=3.2.11#_p=linux#_r={request_random}#_v=3021100#pid={content_id}");
    hasher.update(signed.as_bytes());
    hasher.update(CATALOG_SIGNING_SECRET);
    hex_lower(&hasher.finalize())
}

fn catalog_response_transport(body: &[u8]) -> Result<Vec<u8>, ResolveError> {
    let xml = std::str::from_utf8(body).map_err(|_| ResolveError::Catalog("bad catalog utf-8"))?;
    let torrent = extract_xml_tag(xml, "torrent")?.trim();
    let checksum = extract_xml_tag(xml, "checksum")?.trim();
    let expected = decode_hex20(checksum).ok_or(ResolveError::Catalog("bad checksum"))?;
    let transport = Base64::decode_vec(torrent)
        .map_err(|_| ResolveError::Catalog("bad catalog torrent base64"))?;

    if transport_file_hash(&transport) != expected {
        return Err(ResolveError::Catalog("checksum mismatch"));
    }
    Ok(transport)
}

fn http_response_body(response: &[u8]) -> Result<Vec<u8>, ResolveError> {
    let header_end = response
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .ok_or(ResolveError::Catalog("bad catalog http response"))?;
    let headers = std::str::from_utf8(&response[..header_end])
        .map_err(|_| ResolveError::Catalog("bad catalog http headers"))?;
    let mut lines = headers.split("\r\n");
    let status = lines
        .next()
        .ok_or(ResolveError::Catalog("missing catalog http status"))?;
    if !status.starts_with("HTTP/") || !status.contains(" 200 ") {
        return Err(ResolveError::Catalog("catalog http status"));
    }
    let chunked = lines.any(|line| {
        let lower = line.to_ascii_lowercase();
        lower.starts_with("transfer-encoding:") && lower.contains("chunked")
    });
    let body = &response[header_end + 4..];
    if chunked {
        decode_chunked_body(body)
    } else {
        Ok(body.to_vec())
    }
}

fn decode_chunked_body(mut body: &[u8]) -> Result<Vec<u8>, ResolveError> {
    let mut out = Vec::new();
    loop {
        let line_end = find_crlf(body).ok_or(ResolveError::Catalog("bad chunk size"))?;
        let size_line = std::str::from_utf8(&body[..line_end])
            .map_err(|_| ResolveError::Catalog("bad chunk size"))?;
        let size = usize::from_str_radix(size_line.split(';').next().unwrap_or("").trim(), 16)
            .map_err(|_| ResolveError::Catalog("bad chunk size"))?;
        body = &body[line_end + 2..];

        if size == 0 {
            return Ok(out);
        }
        if body.len() < size + 2 {
            return Err(ResolveError::Catalog("truncated chunk"));
        }
        out.extend_from_slice(&body[..size]);
        if &body[size..size + 2] != b"\r\n" {
            return Err(ResolveError::Catalog("bad chunk terminator"));
        }
        body = &body[size + 2..];
    }
}

fn extract_xml_tag<'a>(xml: &'a str, tag: &str) -> Result<&'a str, ResolveError> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let start = xml
        .find(&open)
        .ok_or(ResolveError::Catalog("missing catalog tag"))?
        + open.len();
    let end = xml[start..]
        .find(&close)
        .ok_or(ResolveError::Catalog("missing catalog tag"))?
        + start;
    Ok(&xml[start..end])
}

fn find_crlf(bytes: &[u8]) -> Option<usize> {
    bytes.windows(2).position(|w| w == b"\r\n")
}

fn hex_lower(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        write!(out, "{b:02x}").expect("writing to string cannot fail");
    }
    out
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
            let ut_id = eh
                .ut_metadata_id()
                .ok_or(ResolveError::Peer("peer has no ut_metadata"))?;
            let size = eh
                .metadata_size()
                .ok_or(ResolveError::Peer("peer sent no metadata_size"))?;
            if !(0..=255).contains(&ut_id) {
                return Err(ResolveError::Peer("invalid ut_metadata params"));
            }
            return Ok((ut_id as u8, checked_metadata_size(size)?));
        }
    }
    Err(ResolveError::Peer("no extended handshake"))
}

/// Validate a peer-advertised `metadata_size` before it drives a BEP-9 fetch/allocation.
///
/// Rejects non-positive sizes and anything above [`MAX_METADATA_SIZE`] (a hostile peer trying
/// to force a large request fan-out / allocation before any descriptor is decoded).
fn checked_metadata_size(size: i64) -> Result<usize, ResolveError> {
    if size <= 0 || size > MAX_METADATA_SIZE as i64 {
        return Err(ResolveError::Peer("invalid ut_metadata params"));
    }
    Ok(size as usize)
}

/// A small TTL cache of resolved `content-id → StreamInfo` so repeated `open()`s of the same
/// stream don't re-fetch metadata.
pub struct ResolveCache {
    entries: Mutex<HashMap<String, (StreamInfo, Instant)>>,
    ttl: Duration,
}

impl ResolveCache {
    pub fn new(ttl: Duration) -> Self {
        ResolveCache {
            entries: Mutex::new(HashMap::new()),
            ttl,
        }
    }

    /// Return a cached, unexpired `StreamInfo` for `key`, if any.
    pub fn get(&self, key: &str) -> Option<StreamInfo> {
        let map = self.entries.lock().unwrap();
        map.get(key)
            .and_then(|(info, at)| (at.elapsed() < self.ttl).then(|| info.clone()))
    }

    /// Store `info` under `key` with the current time.
    pub fn put(&self, key: &str, info: StreamInfo) {
        self.entries
            .lock()
            .unwrap()
            .insert(key.to_string(), (info, Instant::now()));
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
        let body = Enc::new_from_slices(
            &ace_wire::transport::TRANSPORT_KEY,
            &ace_wire::transport::TRANSPORT_IV,
        )
        .unwrap()
        .encrypt_padded_vec::<Pkcs7>(plaintext);
        let mut out = b"AceStreamTransport\x00\x02".to_vec();
        out.extend_from_slice(&body);
        out
    }

    #[test]
    fn transport_yields_stream_info() {
        let tf = make_transport(
            b"d10:authmethod3:RSA7:bitratei100000e12:chunk_lengthi16384e4:name4:Test12:piece_lengthi1048576e6:pubkey3:abc8:trackersl18:udp://t.example:80ee",
        );
        let si = stream_info_from_transport(&tf).unwrap();
        assert_eq!(si.piece_length, 1_048_576);
        assert_eq!(si.chunk_length, 16_384);
        assert_eq!(si.chunks_per_piece(), 64);
        assert_eq!(si.trackers, vec!["udp://t.example:80".to_string()]);
        assert_eq!(
            si.infohash,
            [
                0x92, 0x6e, 0x81, 0x18, 0x33, 0x4d, 0xfa, 0x31, 0xa4, 0x5d, 0x5b, 0xd9, 0x02, 0x30,
                0x6d, 0x42, 0xae, 0xa7, 0xf8, 0x8c,
            ]
        );
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
        assert_eq!(
            stream_info_from_infohash("xyz", vec![]),
            Err(ResolveError::BadInfohash)
        );
        assert_eq!(
            stream_info_from_infohash(&"z".repeat(40), vec![]),
            Err(ResolveError::BadInfohash)
        );
    }

    #[test]
    fn catalog_signature_matches_engine_vectors() {
        let cid = "2123456789abcdef0123456789abcdef01234567";
        assert_eq!(
            catalog_signature(cid, 4_245_094_384_320_117_676),
            "ee6c8422795ced4cd5c1fb80f10276801ab636c6"
        );
        assert_eq!(
            catalog_signature(cid, 2_142_790_664_659_308_268),
            "08c4ba548457e883f8711d3f3cb846c20058d912"
        );
        assert_eq!(
            catalog_signature(cid, 3_472_462_845_767_567_311),
            "c7c3cd3c7268be1a48144653cdbd7912991cb337"
        );
    }

    #[test]
    fn catalog_response_extracts_transport_and_checks_checksum() {
        let body = br#"<?xml version="1.0"?>
<response sig="abc" r="1"><torrent>YWJj</torrent><checksum>a9993e364706816aba3e25717850c26c9cd0d89d</checksum></response>
"#;
        assert_eq!(catalog_response_transport(body).unwrap(), b"abc");

        let bad_checksum = br#"<response><torrent>YWJj</torrent><checksum>0000000000000000000000000000000000000000</checksum></response>"#;
        assert_eq!(
            catalog_response_transport(bad_checksum),
            Err(ResolveError::Catalog("checksum mismatch"))
        );
    }

    #[test]
    fn http_response_body_decodes_chunked_catalog_response() {
        let response = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n3\r\nabc\r\n4\r\ndefg\r\n0\r\n\r\n";
        assert_eq!(http_response_body(response).unwrap(), b"abcdefg");
    }

    #[test]
    fn metadata_size_accepts_plausible_values() {
        assert_eq!(checked_metadata_size(1), Ok(1));
        assert_eq!(checked_metadata_size(4096), Ok(4096));
        assert_eq!(
            checked_metadata_size(MAX_METADATA_SIZE as i64),
            Ok(MAX_METADATA_SIZE)
        );
    }

    #[test]
    fn metadata_size_rejects_non_positive() {
        assert_eq!(
            checked_metadata_size(0),
            Err(ResolveError::Peer("invalid ut_metadata params"))
        );
        assert_eq!(
            checked_metadata_size(-1),
            Err(ResolveError::Peer("invalid ut_metadata params"))
        );
    }

    #[test]
    fn metadata_size_rejects_oversize() {
        // A hostile peer advertising a huge metadata_size would force a large BEP-9 request
        // fan-out and a `vec![0u8; metadata_size]` allocation before any descriptor is decoded.
        assert_eq!(
            checked_metadata_size(MAX_METADATA_SIZE as i64 + 1),
            Err(ResolveError::Peer("invalid ut_metadata params"))
        );
        assert_eq!(
            checked_metadata_size(i64::MAX),
            Err(ResolveError::Peer("invalid ut_metadata params"))
        );
    }

    #[test]
    fn cache_returns_stored_value_then_expires() {
        let c = ResolveCache::new(Duration::from_millis(40));
        let info = StreamInfo {
            infohash: [7; 20],
            piece_length: 1,
            chunk_length: 1,
            trackers: vec![],
            sig_len: 0,
        };
        assert_eq!(c.get("k"), None);
        c.put("k", info.clone());
        assert_eq!(c.get("k"), Some(info));
        std::thread::sleep(Duration::from_millis(55));
        assert_eq!(c.get("k"), None, "entry expires after the TTL");
    }
}
