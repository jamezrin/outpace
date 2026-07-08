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

use crate::types::{StreamInfo, VodInfo};
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
use std::net::{IpAddr, SocketAddr};
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

/// Upper bound on a transport descriptor's advertised `piece_length` (bytes).
///
/// The piece length is untrusted and sizes the [`PieceReassembler`](ace_wire::reassembly)
/// per-piece buffer (`vec![0u8; piece_length]`, allocated on the first block of every
/// in-flight piece) as well as the request fan-out. Real Acestream geometry is small: 64 KiB
/// source-node pieces (`broadcast::PIECE_LENGTH`) and 1 MiB default live pieces
/// ([`DEFAULT_PIECE_LENGTH`]). This ceiling leaves generous headroom for higher-bitrate
/// sources while bounding the allocation a hostile transport can force at stream start.
pub const MAX_PIECE_LENGTH: u64 = 8 * 1_048_576;

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
    /// A transport-file `url=` fetch failed a safety check or the network step.
    Url(&'static str),
}

/// Build a [`StreamInfo`] from raw `AceStreamTransport` file bytes (the pure half of
/// content-id resolution): official swarm infohash + geometry + trackers from the descriptor.
pub fn stream_info_from_transport(bytes: &[u8]) -> Result<StreamInfo, ResolveError> {
    let d = decode_transport(bytes).map_err(|_| ResolveError::Transport("decode failed"))?;
    validate_geometry(d.piece_length, d.chunk_length)?;
    // A valid RSA pubkey => pieces carry a modulus-length signature tail to strip *and* a key
    // to verify each piece's in-band signature against (issue #10); no parseable pubkey =>
    // treat pieces as unsigned (don't strip, nothing to verify).
    let sig_len = ace_wire::live_auth::signature_len_from_pubkey_der(&d.pubkey);
    Ok(StreamInfo {
        infohash: infohash_of_descriptor(&d.raw)
            .map_err(|_| ResolveError::Transport("infohash failed"))?,
        piece_length: d.piece_length,
        chunk_length: d.chunk_length,
        trackers: d.trackers,
        sig_len: sig_len.unwrap_or(0),
        source_pubkey: if sig_len.is_some() {
            d.pubkey
        } else {
            Vec::new()
        },
    })
}

/// Build a [`VodInfo`] from raw `AceStreamTransport` bytes (the pure half of VOD resolution).
///
/// Errors if the descriptor is live (no `pieces`), advertises a multi-file layout (`files`
/// present — intentionally unsupported), fails geometry validation, or lacks a single-file
/// `length`.
pub fn vod_info_from_transport(bytes: &[u8]) -> Result<VodInfo, ResolveError> {
    let d = decode_transport(bytes).map_err(|_| ResolveError::Transport("decode failed"))?;
    if d.is_live {
        return Err(ResolveError::Transport("not a VOD transport (no pieces)"));
    }
    if d.is_multifile() {
        return Err(ResolveError::Transport("multi-file VOD is not supported"));
    }
    validate_geometry(d.piece_length, d.chunk_length)?;
    let total_length = d
        .vod_total_length()
        .ok_or(ResolveError::Transport("VOD descriptor missing length"))?;
    Ok(VodInfo {
        infohash: infohash_of_descriptor(&d.raw)
            .map_err(|_| ResolveError::Transport("infohash failed"))?,
        piece_length: d.piece_length,
        chunk_length: d.chunk_length,
        trackers: d.trackers,
        piece_hashes: d.pieces,
        total_length,
    })
}

/// Validate untrusted transport geometry before it becomes a [`StreamInfo`].
///
/// `piece_length`/`chunk_length` come from a metadata-swarm peer's transport descriptor. The
/// wire codec addresses chunks within a piece by a `u16` index and assumes the fixed Acestream
/// chunk size, so a downloadable descriptor must satisfy:
/// - `chunk_length == DEFAULT_CHUNK_LENGTH` — the 16 KiB protocol constant the wire codec and
///   both live/broadcast paths use; any other value is rejected rather than trusted.
/// - `piece_length <= MAX_PIECE_LENGTH` — bounds the per-piece reassembly allocation.
/// - `piece_length` is an exact multiple of `chunk_length` — whole chunks per piece, so request
///   generation stays consistent with the descriptor.
/// - `piece_length / chunk_length <= u16::MAX` — the chunks-per-piece count must fit the wire
///   chunk index without truncation (see [`StreamInfo::chunks_per_piece`]).
fn validate_geometry(piece_length: u64, chunk_length: u64) -> Result<(), ResolveError> {
    if chunk_length != DEFAULT_CHUNK_LENGTH {
        return Err(ResolveError::Transport("unexpected chunk_length"));
    }
    if piece_length > MAX_PIECE_LENGTH {
        return Err(ResolveError::Transport("piece_length too large"));
    }
    if !piece_length.is_multiple_of(chunk_length) {
        return Err(ResolveError::Transport(
            "piece_length not a multiple of chunk_length",
        ));
    }
    if piece_length / chunk_length > u16::MAX as u64 {
        return Err(ResolveError::Transport("too many chunks per piece"));
    }
    Ok(())
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
        // source key (96-byte signature tail). See DEFAULT_SIG_LEN. Without the actual pubkey
        // we can strip that tail but cannot *verify* it, so `source_pubkey` stays empty (#10).
        sig_len: crate::types::DEFAULT_SIG_LEN,
        source_pubkey: Vec::new(),
    })
}

/// Decode a 40-hex content-id/infohash into 20 bytes (the metadata-swarm handshake key).
pub fn hex20(hex: &str) -> Result<[u8; 20], ResolveError> {
    decode_hex20(hex).ok_or(ResolveError::BadInfohash)
}

/// Encode a 20-byte infohash/content-id as canonical lowercase hex.
pub fn infohash_hex(infohash: &[u8; 20]) -> String {
    hex::encode(&infohash[..])
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
    stream_info_from_transport(
        &transport_bytes_via_peer(session, handshake_infohash, identity).await?,
    )
}

/// Fetch the raw `AceStreamTransport` metadata bytes over a peer via BEP-9 `ut_metadata`. The
/// pure decode half is left to the caller ([`stream_info_from_transport`] for live,
/// [`vod_info_from_transport`] for VOD).
pub async fn transport_bytes_via_peer<S: AsyncRead + AsyncWrite + Unpin>(
    session: &mut PeerSession<S>,
    handshake_infohash: [u8; 20],
    identity: &Identity,
) -> Result<Vec<u8>, ResolveError> {
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
    session
        .fetch_metadata(peer_ut_id, metadata_size)
        .await
        .map_err(|_| ResolveError::Peer("ut_metadata fetch failed"))
}

/// Resolve a content-id through the official signed transport catalog.
///
/// The catalog returns the encrypted `AceStreamTransport` bytes; checksum validation happens
/// before the existing pure transport decoder computes the real live swarm infohash.
pub async fn resolve_via_catalog(content_id: &str) -> Result<StreamInfo, ResolveError> {
    stream_info_from_transport(&catalog_transport_bytes(content_id).await?)
}

/// Fetch the raw (decrypted) `AceStreamTransport` bytes for `content_id` from the signed
/// catalog, trying each catalog host in turn. The pure decode half is left to the caller
/// ([`stream_info_from_transport`] for live, [`vod_info_from_transport`] for VOD).
pub async fn catalog_transport_bytes(content_id: &str) -> Result<Vec<u8>, ResolveError> {
    hex20(content_id)?;

    let mut last = ResolveError::Catalog("catalog request failed");
    for &(host, port) in CATALOG_HOSTS {
        match tokio::time::timeout(
            CATALOG_HOST_TIMEOUT,
            fetch_catalog_transport(host, port, content_id),
        )
        .await
        {
            Ok(Ok(transport)) => return Ok(transport),
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
    hex::encode(hasher.finalize())
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

/// Whether an IP is safe to fetch a transport file from — a conservative denylist that fails
/// closed on anything private, local, or otherwise not a normal public unicast address. Used by
/// the transport-file `url=` fetch to block SSRF into internal services.
fn is_safe_ip(ip: &IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => {
            let o = v4.octets();
            !(v4.is_loopback()
                || v4.is_private()
                || v4.is_link_local()
                || v4.is_unspecified()
                || v4.is_broadcast()
                || v4.is_documentation()
                || v4.is_multicast()
                // Shared address space / CGNAT: 100.64.0.0/10.
                || (o[0] == 100 && (o[1] & 0b1100_0000) == 0b0100_0000))
        }
        IpAddr::V6(v6) => {
            // Re-check IPv4-mapped addresses against the v4 rules.
            if let Some(v4) = v6.to_ipv4_mapped() {
                return is_safe_ip(&IpAddr::V4(v4));
            }
            let seg = v6.segments();
            !(v6.is_loopback()
                || v6.is_unspecified()
                || v6.is_multicast()
                // Unique local fc00::/7.
                || (seg[0] & 0xfe00) == 0xfc00
                // Link-local unicast fe80::/10.
                || (seg[0] & 0xffc0) == 0xfe80)
        }
    }
}

/// Resolve `host` to a single safe [`SocketAddr`], or [`ResolveError::Url`] if it is unresolvable
/// or every resolved address is blocked by [`is_safe_ip`]. IP literals are validated directly;
/// hostnames are resolved and the first safe address is kept (later pinned into the client so
/// DNS rebinding cannot swap it).
async fn resolve_safe_addr(host: &str, port: u16) -> Result<SocketAddr, ResolveError> {
    // `Url::host_str` keeps the brackets on an IPv6 literal (`[::1]`); strip them so the literal
    // parses and is validated, rather than falling through to a doomed DNS lookup.
    let host = host
        .strip_prefix('[')
        .and_then(|h| h.strip_suffix(']'))
        .unwrap_or(host);
    if let Ok(ip) = host.parse::<IpAddr>() {
        return if is_safe_ip(&ip) {
            Ok(SocketAddr::new(ip, port))
        } else {
            Err(ResolveError::Url("blocked address"))
        };
    }
    let addrs = tokio::net::lookup_host((host, port))
        .await
        .map_err(|_| ResolveError::Url("dns failed"))?;
    for addr in addrs {
        if is_safe_ip(&addr.ip()) {
            return Ok(addr);
        }
    }
    Err(ResolveError::Url("blocked address"))
}

/// Upper bound on a fetched transport-file body (bytes). Matches the descriptor ceilings the
/// catalog path and `MAX_METADATA_SIZE` already impose on the same `AceStreamTransport` dict.
pub const MAX_TRANSPORT_FILE: u64 = 1_048_576;

const TRANSPORT_URL_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const TRANSPORT_URL_TOTAL_TIMEOUT: Duration = Duration::from_secs(15);

/// Fetch a transport file from `url`, connecting only to the pre-validated `addr` (pinned so DNS
/// rebinding cannot redirect the connection), and decode it into a [`StreamInfo`]. Redirects are
/// disabled (a redirect is an SSRF bypass), the body is size-capped, and a non-2xx status fails
/// closed. This is the inner seam the public entry calls after the SSRF guard runs.
/// Fetch + decode a transport URL into a [`StreamInfo`]. Retained as a test helper; the
/// production path fetches bytes via [`fetch_transport_bytes_from`] and decodes for the
/// specific stream kind (live vs VOD).
#[cfg(test)]
async fn fetch_transport_from(
    url: &str,
    host: &str,
    addr: SocketAddr,
) -> Result<StreamInfo, ResolveError> {
    stream_info_from_transport(&fetch_transport_bytes_from(url, host, addr).await?)
}

async fn fetch_transport_bytes_from(
    url: &str,
    host: &str,
    addr: SocketAddr,
) -> Result<Vec<u8>, ResolveError> {
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(TRANSPORT_URL_CONNECT_TIMEOUT)
        .timeout(TRANSPORT_URL_TOTAL_TIMEOUT)
        .resolve(host, addr)
        .build()
        .map_err(|_| ResolveError::Url("client build failed"))?;

    let mut resp = client
        .get(url)
        .send()
        .await
        .map_err(|_| ResolveError::Url("fetch failed"))?;
    if !resp.status().is_success() {
        return Err(ResolveError::Url("http status"));
    }

    let mut body = Vec::new();
    while let Some(chunk) = resp
        .chunk()
        .await
        .map_err(|_| ResolveError::Url("fetch failed"))?
    {
        if body.len() as u64 + chunk.len() as u64 > MAX_TRANSPORT_FILE {
            return Err(ResolveError::Url("transport too large"));
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

/// Resolve a transport-file `url=` input into a [`StreamInfo`]: require an http/https scheme, run
/// the SSRF guard on the host, then fetch+decode via [`fetch_transport_from`]. Every failure path
/// is fail-closed.
pub async fn stream_info_from_transport_url(url: &str) -> Result<StreamInfo, ResolveError> {
    stream_info_from_transport(&transport_bytes_from_url(url).await?)
}

/// Fetch raw `AceStreamTransport` bytes from an `http(s)` transport-file URL, applying the same
/// SSRF guard and size cap as [`stream_info_from_transport_url`]. The pure decode half is left
/// to the caller ([`vod_info_from_transport`] for VOD).
pub async fn transport_bytes_from_url(url: &str) -> Result<Vec<u8>, ResolveError> {
    let parsed = reqwest::Url::parse(url).map_err(|_| ResolveError::Url("bad url"))?;
    if !matches!(parsed.scheme(), "http" | "https") {
        return Err(ResolveError::Url("unsupported scheme"));
    }
    let host = parsed.host_str().ok_or(ResolveError::Url("no host"))?;
    let port = parsed
        .port_or_known_default()
        .ok_or(ResolveError::Url("no port"))?;
    let addr = resolve_safe_addr(host, port).await?;
    fetch_transport_bytes_from(url, host, addr).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use cbc::cipher::{block_padding::Pkcs7, BlockModeEncrypt, KeyIvInit};

    fn vod_transport(pieces_bytes: usize, length: Option<i64>, multifile: bool) -> Vec<u8> {
        use ace_wire::bencode::Bencode;
        use std::collections::BTreeMap;
        let mut d = BTreeMap::new();
        d.insert(b"name".to_vec(), Bencode::Bytes(b"movie".to_vec()));
        // Fields the infohash formula requires (name/authmethod/pubkey/piece_length/
        // chunk_length/bitrate — see infohash_of_descriptor).
        d.insert(b"authmethod".to_vec(), Bencode::Bytes(b"None".to_vec()));
        d.insert(b"pubkey".to_vec(), Bencode::Bytes(vec![]));
        d.insert(b"bitrate".to_vec(), Bencode::Int(100000));
        d.insert(b"piece_length".to_vec(), Bencode::Int(131072));
        d.insert(b"chunk_length".to_vec(), Bencode::Int(16384));
        if let Some(len) = length {
            d.insert(b"length".to_vec(), Bencode::Int(len));
        }
        if pieces_bytes > 0 {
            d.insert(b"pieces".to_vec(), Bencode::Bytes(vec![7u8; pieces_bytes]));
        }
        if multifile {
            d.insert(b"files".to_vec(), Bencode::List(vec![]));
        }
        ace_wire::transport::encode_transport(&Bencode::Dict(d))
    }

    #[test]
    fn vod_info_from_single_file_transport() {
        let tf = vod_transport(60, Some(300000), false); // 3 pieces, single-file
        let info = vod_info_from_transport(&tf).unwrap();
        assert_eq!(info.piece_hashes.len(), 3);
        assert_eq!(info.total_length, 300000);
        assert_eq!(info.piece_size(2), 300000 - 2 * 131072);
    }

    #[test]
    fn vod_info_rejects_live_transport() {
        let tf = vod_transport(0, None, false); // no pieces => live
        assert!(matches!(
            vod_info_from_transport(&tf),
            Err(ResolveError::Transport(_))
        ));
    }

    #[test]
    fn vod_info_rejects_multifile() {
        let tf = vod_transport(20, Some(1000), true);
        assert!(matches!(
            vod_info_from_transport(&tf),
            Err(ResolveError::Transport(_))
        ));
    }

    #[test]
    fn vod_info_rejects_missing_length() {
        let tf = vod_transport(20, None, false);
        assert!(matches!(
            vod_info_from_transport(&tf),
            Err(ResolveError::Transport(_))
        ));
    }

    #[test]
    fn infohash_hex_is_lowercase_and_zero_padded() {
        let infohash = [
            0x00, 0x01, 0x0a, 0x0f, 0x10, 0xab, 0xcd, 0xef, 0x20, 0x30, 0x40, 0x50, 0x60, 0x70,
            0x80, 0x90, 0xa0, 0xb0, 0xc0, 0xd0,
        ];
        let encoded = infohash_hex(&infohash);

        assert_eq!(encoded, "00010a0f10abcdef2030405060708090a0b0c0d0");
        assert_eq!(hex20(&encoded).unwrap(), infohash);
    }

    #[test]
    fn ssrf_guard_rejects_unsafe_ip_literals() {
        use std::net::Ipv4Addr;
        for ip in [
            "127.0.0.1",
            "10.1.2.3",
            "192.168.0.1",
            "172.16.0.1",
            "169.254.0.1",
            "100.64.0.1",
            "0.0.0.0",
            "255.255.255.255",
            "224.0.0.1",
        ] {
            assert!(!is_safe_ip(&ip.parse().unwrap()), "{ip} must be unsafe");
        }
        for ip in ["::1", "fc00::1", "fe80::1", "::", "ff02::1"] {
            assert!(!is_safe_ip(&ip.parse().unwrap()), "{ip} must be unsafe");
        }
        // IPv4-mapped loopback must also be rejected.
        assert!(!is_safe_ip(&"::ffff:127.0.0.1".parse().unwrap()));
        // Public addresses are allowed.
        assert!(is_safe_ip(&IpAddr::V4(Ipv4Addr::new(93, 184, 216, 34))));
        assert!(is_safe_ip(&IpAddr::V6(
            "2606:2800:220:1:248:1893:25c8:1946".parse().unwrap()
        )));
    }

    #[tokio::test]
    async fn ssrf_guard_rejects_loopback_host() {
        assert_eq!(
            resolve_safe_addr("127.0.0.1", 80).await,
            Err(ResolveError::Url("blocked address"))
        );
    }

    #[tokio::test]
    async fn ssrf_guard_handles_bracketed_ipv6_literal() {
        // `Url::host_str` returns IPv6 literals bracketed; they must be parsed+validated, not
        // treated as an (always-failing) hostname. Bracketed loopback -> blocked (not dns failed).
        assert_eq!(
            resolve_safe_addr("[::1]", 80).await,
            Err(ResolveError::Url("blocked address"))
        );
        // A bracketed public IPv6 literal resolves to a safe address.
        let ok = resolve_safe_addr("[2606:2800:220:1:248:1893:25c8:1946]", 443)
            .await
            .unwrap();
        assert_eq!(ok.port(), 443);
        assert!(ok.is_ipv6());
    }

    // Minimal one-shot HTTP/1.1 server: serves `status`+`body` to the first connection.
    async fn serve_once(status: &'static str, body: Vec<u8>) -> SocketAddr {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            if let Ok((mut sock, _)) = listener.accept().await {
                let mut buf = [0u8; 1024];
                let _ = sock.read(&mut buf).await; // drain request headers
                let header = format!(
                    "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                let _ = sock.write_all(header.as_bytes()).await;
                let _ = sock.write_all(&body).await;
                let _ = sock.shutdown().await;
            }
        });
        addr
    }

    #[tokio::test]
    async fn fetch_decodes_a_valid_transport() {
        let tf = make_transport(
            b"d10:authmethod3:RSA7:bitratei100000e12:chunk_lengthi16384e4:name4:Test12:piece_lengthi1048576e6:pubkey3:abc8:trackersl18:udp://t.example:80ee",
        );
        let addr = serve_once("200 OK", tf).await;
        let url = format!("http://127.0.0.1:{}/t.bin", addr.port());
        let si = fetch_transport_from(&url, "127.0.0.1", addr).await.unwrap();
        assert_eq!(si.piece_length, 1_048_576);
        assert_eq!(si.infohash[0], 0x92);
    }

    #[tokio::test]
    async fn fetch_rejects_oversized_body() {
        let addr = serve_once("200 OK", vec![b'x'; (MAX_TRANSPORT_FILE + 1) as usize]).await;
        let url = format!("http://127.0.0.1:{}/big.bin", addr.port());
        assert_eq!(
            fetch_transport_from(&url, "127.0.0.1", addr).await,
            Err(ResolveError::Url("transport too large"))
        );
    }

    #[tokio::test]
    async fn fetch_rejects_non_transport_body() {
        let addr = serve_once("200 OK", b"not a transport".to_vec()).await;
        let url = format!("http://127.0.0.1:{}/x", addr.port());
        assert!(matches!(
            fetch_transport_from(&url, "127.0.0.1", addr).await,
            Err(ResolveError::Transport(_))
        ));
    }

    #[tokio::test]
    async fn fetch_rejects_non_200_status() {
        let addr = serve_once("404 Not Found", b"nope".to_vec()).await;
        let url = format!("http://127.0.0.1:{}/missing", addr.port());
        assert_eq!(
            fetch_transport_from(&url, "127.0.0.1", addr).await,
            Err(ResolveError::Url("http status"))
        );
    }

    #[tokio::test]
    async fn transport_url_rejects_unsupported_scheme() {
        assert_eq!(
            stream_info_from_transport_url("file:///etc/passwd").await,
            Err(ResolveError::Url("unsupported scheme"))
        );
        assert_eq!(
            stream_info_from_transport_url("ftp://example.com/x").await,
            Err(ResolveError::Url("unsupported scheme"))
        );
    }

    #[tokio::test]
    #[ignore = "network: fetches a real public transport-file URL"]
    async fn live_transport_url_fetch() {
        // Replace with a known public/free transport-file URL when validating live.
        let url =
            std::env::var("OUTPACE_TEST_TRANSPORT_URL").expect("set OUTPACE_TEST_TRANSPORT_URL");
        let si = stream_info_from_transport_url(&url).await.unwrap();
        assert_ne!(si.infohash, [0u8; 20]);
    }

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
    fn rejects_oversized_piece_length() {
        // 100 MiB pieces would force a huge per-piece reassembly buffer at stream start;
        // reject the untrusted geometry before it becomes a StreamInfo.
        let tf = make_transport(b"d12:chunk_lengthi16384e12:piece_lengthi104857600ee");
        assert_eq!(
            stream_info_from_transport(&tf),
            Err(ResolveError::Transport("piece_length too large"))
        );
    }

    #[test]
    fn rejects_unexpected_chunk_length() {
        // The wire codec's chunk size is the fixed Acestream protocol constant (16 KiB).
        let tf = make_transport(b"d12:chunk_lengthi8192e12:piece_lengthi65536ee");
        assert_eq!(
            stream_info_from_transport(&tf),
            Err(ResolveError::Transport("unexpected chunk_length"))
        );
    }

    #[test]
    fn rejects_piece_length_not_multiple_of_chunk() {
        // A piece that isn't a whole number of chunks makes chunk request generation
        // inconsistent with the descriptor.
        let tf = make_transport(b"d12:chunk_lengthi16384e12:piece_lengthi65537ee");
        assert_eq!(
            stream_info_from_transport(&tf),
            Err(ResolveError::Transport(
                "piece_length not a multiple of chunk_length"
            ))
        );
    }

    #[test]
    fn accepts_broadcast_geometry() {
        // 64 KiB pieces / 16 KiB chunks — the source-node geometry outpace itself mints.
        let tf = make_transport(
            b"d10:authmethod3:RSA7:bitratei8375e12:chunk_lengthi16384e4:name4:Test12:piece_lengthi65536e6:pubkey3:abce",
        );
        let si = stream_info_from_transport(&tf).unwrap();
        assert_eq!(si.piece_length, 65_536);
        assert_eq!(si.chunk_length, 16_384);
        assert_eq!(si.chunks_per_piece(), 4);
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
            source_pubkey: vec![],
        };
        assert_eq!(c.get("k"), None);
        c.put("k", info.clone());
        assert_eq!(c.get("k"), Some(info));
        std::thread::sleep(Duration::from_millis(55));
        assert_eq!(c.get("k"), None, "entry expires after the TTL");
    }
}
