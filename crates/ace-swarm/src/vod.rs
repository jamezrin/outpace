//! Single-file VOD download over vanilla BitTorrent: request blocks in order, verify each
//! assembled piece against the transport's SHA-1 `pieces`, and emit verified bytes in order.
//!
//! VOD is standard BitTorrent (`Request`/`Piece`/`Bitfield`/`Have`), unlike the live path
//! which reuses those message IDs with custom `[stream]` payloads and in-band RSA signatures.
//! This module therefore shares only the low-level connect/handshake primitives with live and
//! is deterministically testable against a local mock seeder (no live swarm).

use crate::types::VodInfo;
use ace_peer::session::{connect, PeerSession};
use ace_wire::handshake::random_peer_id;
use ace_wire::message::PeerMessage;
use bytes::Bytes;
use sha1::{Digest, Sha1};
use std::net::SocketAddrV4;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc;

/// Maximum time allowed for establishing a VOD peer connection.
pub const VOD_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// Maximum time allowed for completing the BitTorrent handshake.
pub const VOD_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
/// Maximum time a VOD peer may leave us choked after interest is announced.
pub const VOD_UNCHOKE_TIMEOUT: Duration = Duration::from_secs(15);
/// Maximum time between newly covered blocks while assembling a VOD piece.
pub const VOD_BLOCK_PROGRESS_TIMEOUT: Duration = Duration::from_secs(15);
/// Maximum time allowed for an outbound VOD protocol write.
pub const VOD_WRITE_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Clone, Copy)]
struct VodTimeouts {
    connect: Duration,
    handshake: Duration,
    unchoke: Duration,
    block_progress: Duration,
    write: Duration,
}

const DEFAULT_VOD_TIMEOUTS: VodTimeouts = VodTimeouts {
    connect: VOD_CONNECT_TIMEOUT,
    handshake: VOD_HANDSHAKE_TIMEOUT,
    unchoke: VOD_UNCHOKE_TIMEOUT,
    block_progress: VOD_BLOCK_PROGRESS_TIMEOUT,
    write: VOD_WRITE_TIMEOUT,
};

/// True iff `bytes` hashes (SHA-1) to `expected` — standard BitTorrent piece integrity.
pub fn verify_piece(expected: &[u8; 20], bytes: &[u8]) -> bool {
    let digest = Sha1::digest(bytes);
    digest.as_slice() == expected
}

/// Piece-local assembly state whose memory is bounded by the validated piece geometry.
///
/// `covered` is a bitset (`Vec<bool>`) with one bit per output byte. This accepts standard
/// block-aligned responses while also handling partial overlaps without double-counting them.
/// Peer-provided offsets and lengths are validated before either buffer is indexed.
struct PieceBuffer {
    bytes: Vec<u8>,
    covered: Vec<bool>,
    covered_len: usize,
}

impl PieceBuffer {
    fn new(piece_len: usize) -> Self {
        Self {
            bytes: vec![0; piece_len],
            covered: vec![false; piece_len],
            covered_len: 0,
        }
    }

    /// Add one received block. Empty, overflowing, or out-of-range blocks are peer errors;
    /// valid duplicate/overlapping bytes are idempotent and only new coverage advances
    /// completion.
    fn add_block(&mut self, begin: u32, block: &[u8]) -> Result<bool, ()> {
        if block.is_empty() {
            return Err(());
        }
        let start = begin as usize;
        let end = start.checked_add(block.len()).ok_or(())?;
        if start >= self.bytes.len() || end > self.bytes.len() {
            return Err(());
        }

        self.bytes[start..end].copy_from_slice(block);
        let covered_before = self.covered_len;
        for covered in &mut self.covered[start..end] {
            if !*covered {
                *covered = true;
                self.covered_len += 1;
            }
        }
        Ok(self.covered_len > covered_before)
    }

    fn is_complete(&self) -> bool {
        self.covered_len == self.bytes.len()
    }
}

/// Error from a VOD download.
#[derive(Debug)]
pub enum VodError {
    /// No peer could supply a verifying copy of the piece at this index.
    Unrecoverable(u64),
    /// The consumer dropped the receiver before the download finished.
    ConsumerGone,
}

/// Download a single-file VOD described by `info` from `peers`, verifying every piece against
/// its SHA-1 hash before sending its bytes (in order) to `tx`.
///
/// Deliberately simple: pieces are pulled sequentially from one peer at a time, resuming at the
/// same cursor on the next peer if a peer fails (connect error, disconnect, or a piece that
/// fails verification). Rarest-first / multi-peer parallelism is a documented follow-up. Fails
/// with [`VodError::Unrecoverable`] if the piece list cannot be completed, and never emits
/// unverified bytes.
pub async fn download_vod(
    info: VodInfo,
    peers: Vec<SocketAddrV4>,
    tx: mpsc::Sender<Bytes>,
) -> Result<(), VodError> {
    let end_piece = info.piece_count();
    download_vod_pieces(info, peers, tx, 0, end_piece).await
}

/// Download the contiguous piece range `[first_piece, end_piece)` of `info`, verifying every
/// piece against its SHA-1 hash before sending its (whole, in-order) bytes to `tx`. This is the
/// primitive behind byte-range/seek serving: the caller picks the pieces covering the requested
/// byte range and trims the partial ends. Shares the same sequential single-peer strategy and
/// stall budget as [`download_vod`], and likewise never emits unverified bytes.
pub async fn download_vod_pieces(
    info: VodInfo,
    peers: Vec<SocketAddrV4>,
    tx: mpsc::Sender<Bytes>,
    first_piece: u64,
    end_piece: u64,
) -> Result<(), VodError> {
    download_vod_pieces_with_timeouts(
        info,
        peers,
        tx,
        first_piece,
        end_piece,
        DEFAULT_VOD_TIMEOUTS,
    )
    .await
}

async fn download_vod_pieces_with_timeouts(
    info: VodInfo,
    peers: Vec<SocketAddrV4>,
    tx: mpsc::Sender<Bytes>,
    first_piece: u64,
    end_piece: u64,
    timeouts: VodTimeouts,
) -> Result<(), VodError> {
    let end_piece = end_piece.min(info.piece_count());
    let mut next_piece = first_piece;
    let mut peer_idx = 0usize;
    // Stall budget: consecutive attempts that made no progress. Reset whenever `next_piece`
    // advances, so a long download that keeps delivering pieces across reconnecting peers is
    // never failed for "using up attempts" — only a genuine stall (no piece advanced) is.
    let mut attempts = 0usize;
    let max_attempts = peers.len().max(1) * 3;
    while next_piece < end_piece {
        if peers.is_empty() || attempts >= max_attempts {
            return Err(VodError::Unrecoverable(next_piece));
        }
        attempts += 1;
        let addr = peers[peer_idx % peers.len()];
        peer_idx += 1;
        let progress_before = next_piece;
        let addr_string = addr.to_string();

        let mut session = tokio::select! {
            _ = tx.closed() => return Err(VodError::ConsumerGone),
            result = tokio::time::timeout(timeouts.connect, connect(&addr_string)) => {
                match result {
                    Ok(Ok(session)) => session,
                    Ok(Err(_)) | Err(_) => continue,
                }
            }
        };
        let handshake = tokio::select! {
            _ = tx.closed() => return Err(VodError::ConsumerGone),
            result = tokio::time::timeout(
                timeouts.handshake,
                session.perform_handshake(info.infohash, random_peer_id()),
            ) => result,
        };
        if !matches!(handshake, Ok(Ok(_))) {
            continue;
        }
        let drain_result = tokio::select! {
            _ = tx.closed() => return Err(VodError::ConsumerGone),
            result = drain_from_peer(
                &mut session,
                &info,
                &mut next_piece,
                end_piece,
                &tx,
                timeouts,
            ) => result,
        };
        match drain_result {
            Ok(()) => {}
            Err(VodError::ConsumerGone) => return Err(VodError::ConsumerGone),
            // Peer-local failure: keep the cursor and try another peer.
            Err(VodError::Unrecoverable(_)) => {}
        }
        if next_piece > progress_before {
            // This peer delivered at least one verified piece — that's forward progress, even if
            // it then dropped mid-drain. Only consecutive no-progress attempts count as a stall.
            attempts = 0;
        }
    }
    Ok(())
}

/// Pull pieces in order from a single connected peer, verifying and emitting each. Returns when
/// all pieces are done, or with [`VodError::Unrecoverable`] when this peer stops being useful.
async fn drain_from_peer<S: AsyncRead + AsyncWrite + Unpin>(
    session: &mut PeerSession<S>,
    info: &VodInfo,
    next_piece: &mut u64,
    end_piece: u64,
    tx: &mpsc::Sender<Bytes>,
    timeouts: VodTimeouts,
) -> Result<(), VodError> {
    send_vod_message(
        session,
        &PeerMessage::Interested,
        tx,
        timeouts.write,
        *next_piece,
    )
    .await?;
    let mut unchoked = false;
    while *next_piece < end_piece {
        let idx = *next_piece;
        let piece_len = info.piece_size(idx) as usize;
        let mut assembled = PieceBuffer::new(piece_len);
        if unchoked {
            request_piece_blocks(session, info, idx, piece_len, tx, timeouts.write).await?;
        }
        let mut progress_deadline = tokio::time::Instant::now()
            + if unchoked {
                timeouts.block_progress
            } else {
                timeouts.unchoke
            };
        let mut requested_this_piece = unchoked;
        loop {
            let msg = tokio::select! {
                _ = tx.closed() => return Err(VodError::ConsumerGone),
                result = tokio::time::timeout_at(progress_deadline, session.read_message()) => {
                    match result {
                        Ok(Ok(message)) => message,
                        Ok(Err(_)) | Err(_) => return Err(VodError::Unrecoverable(idx)),
                    }
                }
            };
            match msg {
                PeerMessage::Unchoke => {
                    if !unchoked {
                        unchoked = true;
                        if !requested_this_piece {
                            request_piece_blocks(session, info, idx, piece_len, tx, timeouts.write)
                                .await?;
                            requested_this_piece = true;
                            // The first unchoke changes the wait from "permission" to "data".
                            // Later choke/unchoke chatter cannot extend this deadline.
                            progress_deadline =
                                tokio::time::Instant::now() + timeouts.block_progress;
                        }
                    }
                }
                PeerMessage::Choke => {
                    unchoked = false;
                }
                PeerMessage::Piece {
                    index,
                    begin,
                    block,
                } if index == idx as u32 => {
                    let made_progress = assembled
                        .add_block(begin, &block)
                        .map_err(|_| VodError::Unrecoverable(idx))?;
                    if assembled.is_complete() {
                        break;
                    }
                    if made_progress {
                        progress_deadline = tokio::time::Instant::now() + timeouts.block_progress;
                    }
                }
                // Ignore Have / Bitfield / unrelated-piece / other messages; keep reading.
                _ => {}
            }
        }
        if !verify_piece(&info.piece_hashes[idx as usize], &assembled.bytes) {
            // This peer served a piece that failed its hash; abandon it (keep the cursor).
            return Err(VodError::Unrecoverable(idx));
        }
        tx.send(Bytes::from(assembled.bytes))
            .await
            .map_err(|_| VodError::ConsumerGone)?;
        *next_piece += 1;
    }
    Ok(())
}

/// Send `Request` messages covering `[0, piece_len)` of piece `idx` in `chunk_length` blocks.
async fn request_piece_blocks<S: AsyncRead + AsyncWrite + Unpin>(
    session: &mut PeerSession<S>,
    info: &VodInfo,
    idx: u64,
    piece_len: usize,
    tx: &mpsc::Sender<Bytes>,
    write_timeout: Duration,
) -> Result<(), VodError> {
    let block = info.chunk_length as usize;
    let mut begin = 0usize;
    while begin < piece_len {
        let length = block.min(piece_len - begin);
        send_vod_message(
            session,
            &PeerMessage::Request {
                index: idx as u32,
                begin: begin as u32,
                length: length as u32,
            },
            tx,
            write_timeout,
            idx,
        )
        .await?;
        begin += length;
    }
    Ok(())
}

async fn send_vod_message<S: AsyncRead + AsyncWrite + Unpin>(
    session: &mut PeerSession<S>,
    message: &PeerMessage,
    tx: &mpsc::Sender<Bytes>,
    timeout: Duration,
    piece: u64,
) -> Result<(), VodError> {
    tokio::select! {
        _ = tx.closed() => Err(VodError::ConsumerGone),
        result = tokio::time::timeout(timeout, session.send(message)) => {
            match result {
                Ok(Ok(())) => Ok(()),
                Ok(Err(_)) | Err(_) => Err(VodError::Unrecoverable(piece)),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn verify_piece_accepts_matching_and_rejects_tampered() {
        let data = b"hello vod piece";
        let hash: [u8; 20] = Sha1::digest(data).into();
        assert!(verify_piece(&hash, data));
        let mut bad = data.to_vec();
        bad[0] ^= 0xff;
        assert!(!verify_piece(&hash, &bad));
    }

    #[test]
    fn duplicate_block_does_not_advance_unique_coverage() {
        let mut piece = PieceBuffer::new(6);
        piece.add_block(0, &[1, 2, 3]).unwrap();
        piece.add_block(0, &[1, 2, 3]).unwrap();
        assert_eq!(piece.covered_len, 3);
        assert!(!piece.is_complete());

        piece.add_block(3, &[4, 5, 6]).unwrap();
        assert!(piece.is_complete());
        assert_eq!(piece.bytes, vec![1, 2, 3, 4, 5, 6]);
    }

    #[test]
    fn partial_overlap_counts_only_new_bytes() {
        let mut piece = PieceBuffer::new(6);
        piece.add_block(0, &[1, 2, 3, 4]).unwrap();
        piece.add_block(2, &[3, 4, 5, 6]).unwrap();
        assert_eq!(piece.covered_len, 6);
        assert!(piece.is_complete());
        assert_eq!(piece.bytes, vec![1, 2, 3, 4, 5, 6]);
    }

    #[test]
    fn out_of_order_unique_blocks_complete() {
        let mut piece = PieceBuffer::new(6);
        piece.add_block(4, &[5, 6]).unwrap();
        piece.add_block(0, &[1, 2]).unwrap();
        assert!(!piece.is_complete());
        piece.add_block(2, &[3, 4]).unwrap();
        assert!(piece.is_complete());
        assert_eq!(piece.bytes, vec![1, 2, 3, 4, 5, 6]);
    }

    #[test]
    fn malformed_blocks_are_rejected_without_resizing() {
        let mut piece = PieceBuffer::new(4);
        assert!(piece.add_block(0, &[]).is_err());
        assert!(piece.add_block(4, &[1]).is_err());
        assert!(piece.add_block(3, &[1, 2]).is_err());
        assert!(piece.add_block(u32::MAX, &[1]).is_err());
        assert_eq!(piece.bytes.len(), 4);
        assert_eq!(piece.covered_len, 0);
    }
}

#[cfg(test)]
mod seeder_tests {
    use super::*;
    use crate::types::VodInfo;
    use ace_wire::handshake::Handshake;
    use ace_wire::message::PeerMessage;
    use std::net::SocketAddrV4;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    // Build `total_len` bytes of content and a matching VodInfo (piece hashes over that content).
    fn make_content(piece_length: u64, chunk_length: u64, total_len: u64) -> (Vec<u8>, VodInfo) {
        let content: Vec<u8> = (0..total_len).map(|i| (i % 251) as u8).collect();
        let piece_count = total_len.div_ceil(piece_length);
        let mut piece_hashes = Vec::new();
        for p in 0..piece_count {
            let start = (p * piece_length) as usize;
            let end = ((p + 1) * piece_length).min(total_len) as usize;
            let h: [u8; 20] = Sha1::digest(&content[start..end]).into();
            piece_hashes.push(h);
        }
        let info = VodInfo {
            infohash: [0x42; 20],
            piece_length,
            chunk_length,
            trackers: vec![],
            piece_hashes,
            total_length: total_len,
        };
        (content, info)
    }

    // A minimal standard-BitTorrent seeder. Accepts connections in a loop (so a client that
    // abandons a peer after a failed verification can reconnect immediately — no timeout wait).
    async fn spawn_seeder(
        content: Vec<u8>,
        info: VodInfo,
        tamper: bool,
        block_delay: Option<Duration>,
    ) -> SocketAddrV4 {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = match listener.local_addr().unwrap() {
            std::net::SocketAddr::V4(a) => a,
            _ => unreachable!(),
        };
        tokio::spawn(async move {
            loop {
                let (mut sock, _) = match listener.accept().await {
                    Ok(v) => v,
                    Err(_) => break,
                };
                let content = content.clone();
                let info = info.clone();
                tokio::spawn(async move {
                    let mut hs = [0u8; ace_wire::handshake::HANDSHAKE_LEN];
                    if sock.read_exact(&mut hs).await.is_err() {
                        return;
                    }
                    let reply =
                        Handshake::new(info.infohash, ace_wire::handshake::random_peer_id())
                            .encode();
                    if sock.write_all(&reply).await.is_err() {
                        return;
                    }
                    // Full bitfield (all pieces), MSB-first, then unchoke.
                    let nbytes = (info.piece_count() as usize).div_ceil(8);
                    let mut bits = vec![0u8; nbytes];
                    for p in 0..info.piece_count() as usize {
                        bits[p / 8] |= 0x80 >> (p % 8);
                    }
                    let _ = sock.write_all(&PeerMessage::Bitfield(bits).encode()).await;
                    let _ = sock.write_all(&PeerMessage::Unchoke.encode()).await;
                    // Answer requests until the peer disconnects.
                    let mut buf: Vec<u8> = Vec::new();
                    let mut tmp = [0u8; 4096];
                    loop {
                        loop {
                            match PeerMessage::decode(&buf) {
                                Ok(Some((msg, used))) => {
                                    buf.drain(..used);
                                    if let PeerMessage::Request {
                                        index,
                                        begin,
                                        length,
                                    } = msg
                                    {
                                        if let Some(delay) = block_delay {
                                            tokio::time::sleep(delay).await;
                                        }
                                        let start = (index as u64 * info.piece_length
                                            + begin as u64)
                                            as usize;
                                        let end = start + length as usize;
                                        let mut block = content[start..end].to_vec();
                                        if tamper && index == 0 && begin == 0 {
                                            block[0] ^= 0xff;
                                        }
                                        let piece = PeerMessage::Piece {
                                            index,
                                            begin,
                                            block,
                                        }
                                        .encode();
                                        if sock.write_all(&piece).await.is_err() {
                                            return;
                                        }
                                    }
                                }
                                Ok(None) => break,
                                Err(_) => return,
                            }
                        }
                        let n = match sock.read(&mut tmp).await {
                            Ok(0) | Err(_) => return,
                            Ok(n) => n,
                        };
                        buf.extend_from_slice(&tmp[..n]);
                    }
                });
            }
        });
        addr
    }

    #[tokio::test]
    async fn downloads_and_verifies_single_file_vod() {
        // 3 pieces, last one partial: piece_length 32 KiB, chunk 16 KiB, total 80000.
        let (content, info) = make_content(32768, 16384, 80000);
        let addr = spawn_seeder(content.clone(), info.clone(), false, None).await;
        let (tx, mut rx) = mpsc::channel::<Bytes>(64);
        let handle = tokio::spawn(async move { download_vod(info, vec![addr], tx).await });
        let mut got = Vec::new();
        while let Some(chunk) = rx.recv().await {
            got.extend_from_slice(&chunk);
        }
        handle.await.unwrap().unwrap();
        assert_eq!(got, content);
    }

    #[tokio::test]
    async fn downloads_only_the_requested_piece_range() {
        // 5 pieces of 32 KiB each (total 160000). Request pieces [1, 4): 1, 2, 3.
        let (content, info) = make_content(32768, 16384, 160000);
        assert_eq!(info.piece_count(), 5);
        let addr = spawn_seeder(content.clone(), info.clone(), false, None).await;
        let (tx, mut rx) = mpsc::channel::<Bytes>(64);
        let handle =
            tokio::spawn(async move { download_vod_pieces(info, vec![addr], tx, 1, 4).await });
        let mut got = Vec::new();
        while let Some(chunk) = rx.recv().await {
            got.extend_from_slice(&chunk);
        }
        handle.await.unwrap().unwrap();
        // Exactly the whole-piece bytes for pieces 1..=3, verified against their SHA-1 hashes.
        assert_eq!(got, content[32768..4 * 32768]);
    }

    #[tokio::test]
    async fn piece_range_download_rejects_a_tampered_covering_piece() {
        // Tamper corrupts piece 0's first block; a range that includes piece 0 must still verify
        // every covering piece it emits, so the download fails rather than leaking bad bytes.
        let (content, info) = make_content(32768, 16384, 160000);
        let addr = spawn_seeder(content, info.clone(), true, None).await;
        let (tx, mut rx) = mpsc::channel::<Bytes>(64);
        let handle =
            tokio::spawn(async move { download_vod_pieces(info, vec![addr], tx, 0, 2).await });
        while rx.recv().await.is_some() {}
        assert!(
            handle.await.unwrap().is_err(),
            "a tampered covering piece must fail the ranged download"
        );
    }

    async fn spawn_silent_peer(infohash: [u8; 20], complete_handshake: bool) -> SocketAddrV4 {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = match listener.local_addr().unwrap() {
            std::net::SocketAddr::V4(addr) => addr,
            _ => unreachable!(),
        };
        tokio::spawn(async move {
            loop {
                let (mut sock, _) = listener.accept().await.unwrap();
                tokio::spawn(async move {
                    let mut hs = [0u8; ace_wire::handshake::HANDSHAKE_LEN];
                    if sock.read_exact(&mut hs).await.is_err() {
                        return;
                    }
                    if complete_handshake {
                        let reply = Handshake::new(infohash, ace_wire::handshake::random_peer_id())
                            .encode();
                        if sock.write_all(&reply).await.is_err() {
                            return;
                        }
                    }
                    std::future::pending::<()>().await;
                });
            }
        });
        addr
    }

    fn fast_timeouts() -> VodTimeouts {
        VodTimeouts {
            connect: Duration::from_millis(100),
            handshake: Duration::from_millis(100),
            unchoke: Duration::from_millis(100),
            block_progress: Duration::from_millis(100),
            write: Duration::from_millis(100),
        }
    }

    async fn spawn_chattering_peer(infohash: [u8; 20]) -> SocketAddrV4 {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = match listener.local_addr().unwrap() {
            std::net::SocketAddr::V4(addr) => addr,
            _ => unreachable!(),
        };
        tokio::spawn(async move {
            loop {
                let (mut sock, _) = listener.accept().await.unwrap();
                tokio::spawn(async move {
                    let mut hs = [0u8; ace_wire::handshake::HANDSHAKE_LEN];
                    if sock.read_exact(&mut hs).await.is_err() {
                        return;
                    }
                    let reply =
                        Handshake::new(infohash, ace_wire::handshake::random_peer_id()).encode();
                    if sock.write_all(&reply).await.is_err() {
                        return;
                    }
                    loop {
                        for message in [PeerMessage::Unchoke, PeerMessage::Choke] {
                            if sock.write_all(&message.encode()).await.is_err() {
                                return;
                            }
                            tokio::time::sleep(Duration::from_millis(5)).await;
                        }
                    }
                });
            }
        });
        addr
    }

    #[tokio::test]
    async fn handshaking_silent_peer_is_abandoned() {
        let (_, info) = make_content(16, 8, 16);
        let silent = spawn_silent_peer(info.infohash, true).await;
        let (tx, _rx) = mpsc::channel(1);
        let result = tokio::time::timeout(
            Duration::from_millis(500),
            download_vod_pieces_with_timeouts(info, vec![silent], tx, 0, 1, fast_timeouts()),
        )
        .await
        .expect("silent peer must not hang");
        assert!(matches!(result, Err(VodError::Unrecoverable(0))));
    }

    #[tokio::test]
    async fn choke_unchoke_chatter_does_not_extend_the_data_deadline() {
        let (_, info) = make_content(16, 8, 16);
        let chatter = spawn_chattering_peer(info.infohash).await;
        let (tx, _rx) = mpsc::channel(1);
        let result = tokio::time::timeout(
            Duration::from_millis(500),
            download_vod_pieces_with_timeouts(info, vec![chatter], tx, 0, 1, fast_timeouts()),
        )
        .await
        .expect("state chatter without bytes must not hang");
        assert!(matches!(result, Err(VodError::Unrecoverable(0))));
    }

    #[tokio::test]
    async fn silent_peer_falls_back_to_healthy_peer() {
        let (content, info) = make_content(32, 8, 32);
        let silent = spawn_silent_peer(info.infohash, true).await;
        let healthy = spawn_seeder(content.clone(), info.clone(), false, None).await;
        let (tx, mut rx) = mpsc::channel(4);
        let handle = tokio::spawn(download_vod_pieces_with_timeouts(
            info,
            vec![silent, healthy],
            tx,
            0,
            1,
            fast_timeouts(),
        ));
        let got = rx.recv().await.unwrap();
        assert_eq!(got.as_ref(), content);
        assert!(rx.recv().await.is_none());
        handle.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn continuing_block_progress_extends_the_deadline() {
        // Eight blocks take about 400 ms in total, twice the 200 ms progress timeout.
        let (content, info) = make_content(64, 8, 64);
        let peer = spawn_seeder(
            content.clone(),
            info.clone(),
            false,
            Some(Duration::from_millis(50)),
        )
        .await;
        let (tx, mut rx) = mpsc::channel(4);
        let result = download_vod_pieces_with_timeouts(
            info,
            vec![peer],
            tx,
            0,
            1,
            VodTimeouts {
                block_progress: Duration::from_millis(200),
                ..fast_timeouts()
            },
        );
        let result = tokio::time::timeout(Duration::from_millis(800), result)
            .await
            .expect("progressing peer must finish");
        result.unwrap();
        assert_eq!(rx.recv().await.unwrap().as_ref(), content);
    }

    #[tokio::test]
    async fn empty_peer_set_is_unrecoverable_immediately() {
        let (_, info) = make_content(16, 8, 16);
        let (tx, _rx) = mpsc::channel(1);
        assert!(matches!(
            download_vod(info, vec![], tx).await,
            Err(VodError::Unrecoverable(0))
        ));
    }

    #[tokio::test]
    async fn dropping_consumer_cancels_a_silent_peer_promptly() {
        let (_, info) = make_content(16, 8, 16);
        let silent = spawn_silent_peer(info.infohash, true).await;
        let (tx, rx) = mpsc::channel(1);
        let handle = tokio::spawn(download_vod_pieces_with_timeouts(
            info,
            vec![silent],
            tx,
            0,
            1,
            VodTimeouts {
                unchoke: Duration::from_secs(10),
                ..fast_timeouts()
            },
        ));
        tokio::time::sleep(Duration::from_millis(10)).await;
        drop(rx);
        let result = tokio::time::timeout(Duration::from_millis(100), handle)
            .await
            .expect("consumer cancellation must be prompt")
            .unwrap();
        assert!(matches!(result, Err(VodError::ConsumerGone)));
    }

    #[tokio::test]
    async fn dropping_consumer_cancels_backpressured_request_writes() {
        let (_server, client) = tokio::io::duplex(1);
        let mut session = PeerSession::new(client);
        let (_, info) = make_content(32, 8, 32);
        let (tx, rx) = mpsc::channel(1);
        let handle = tokio::spawn(async move {
            request_piece_blocks(&mut session, &info, 0, 32, &tx, Duration::from_secs(10)).await
        });
        tokio::time::sleep(Duration::from_millis(10)).await;
        drop(rx);
        let result = tokio::time::timeout(Duration::from_millis(100), handle)
            .await
            .expect("consumer cancellation must interrupt a blocked request write")
            .unwrap();
        assert!(matches!(result, Err(VodError::ConsumerGone)));
    }

    // A seeder that serves at most `pieces_per_conn` distinct pieces per connection, then drops.
    // Forces the client to reconnect and resume — exercising progress across many short peers.
    async fn spawn_flaky_seeder(
        content: Vec<u8>,
        info: VodInfo,
        pieces_per_conn: usize,
    ) -> SocketAddrV4 {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = match listener.local_addr().unwrap() {
            std::net::SocketAddr::V4(a) => a,
            _ => unreachable!(),
        };
        tokio::spawn(async move {
            loop {
                let (mut sock, _) = match listener.accept().await {
                    Ok(v) => v,
                    Err(_) => break,
                };
                let content = content.clone();
                let info = info.clone();
                tokio::spawn(async move {
                    let mut hs = [0u8; ace_wire::handshake::HANDSHAKE_LEN];
                    if sock.read_exact(&mut hs).await.is_err() {
                        return;
                    }
                    let reply =
                        Handshake::new(info.infohash, ace_wire::handshake::random_peer_id())
                            .encode();
                    if sock.write_all(&reply).await.is_err() {
                        return;
                    }
                    let nbytes = (info.piece_count() as usize).div_ceil(8);
                    let mut bits = vec![0u8; nbytes];
                    for p in 0..info.piece_count() as usize {
                        bits[p / 8] |= 0x80 >> (p % 8);
                    }
                    let _ = sock.write_all(&PeerMessage::Bitfield(bits).encode()).await;
                    let _ = sock.write_all(&PeerMessage::Unchoke.encode()).await;
                    let mut served: std::collections::HashSet<u32> =
                        std::collections::HashSet::new();
                    let mut buf: Vec<u8> = Vec::new();
                    let mut tmp = [0u8; 4096];
                    loop {
                        loop {
                            match PeerMessage::decode(&buf) {
                                Ok(Some((msg, used))) => {
                                    buf.drain(..used);
                                    if let PeerMessage::Request {
                                        index,
                                        begin,
                                        length,
                                    } = msg
                                    {
                                        if !served.contains(&index) {
                                            if served.len() >= pieces_per_conn {
                                                return; // drop: force a reconnect
                                            }
                                            served.insert(index);
                                        }
                                        let start = (index as u64 * info.piece_length
                                            + begin as u64)
                                            as usize;
                                        let end = start + length as usize;
                                        let block = content[start..end].to_vec();
                                        let piece = PeerMessage::Piece {
                                            index,
                                            begin,
                                            block,
                                        }
                                        .encode();
                                        if sock.write_all(&piece).await.is_err() {
                                            return;
                                        }
                                    }
                                }
                                Ok(None) => break,
                                Err(_) => return,
                            }
                        }
                        let n = match sock.read(&mut tmp).await {
                            Ok(0) | Err(_) => return,
                            Ok(n) => n,
                        };
                        buf.extend_from_slice(&tmp[..n]);
                    }
                });
            }
        });
        addr
    }

    #[tokio::test]
    async fn progress_across_flaky_peer_resets_stall_budget() {
        // 5 pieces from a single peer that drops after each piece. With one peer the stall
        // budget is only 3 attempts, so this only completes because progress resets it.
        let (content, info) = make_content(16384, 16384, 5 * 16384 - 100);
        assert_eq!(info.piece_count(), 5);
        let addr = spawn_flaky_seeder(content.clone(), info.clone(), 1).await;
        let (tx, mut rx) = mpsc::channel::<Bytes>(64);
        let handle = tokio::spawn(async move { download_vod(info, vec![addr], tx).await });
        let mut got = Vec::new();
        while let Some(chunk) = rx.recv().await {
            got.extend_from_slice(&chunk);
        }
        handle.await.unwrap().unwrap();
        assert_eq!(got, content);
    }

    #[tokio::test]
    async fn tampered_piece_is_rejected() {
        let (content, info) = make_content(32768, 16384, 80000);
        let addr = spawn_seeder(content, info.clone(), true, None).await;
        let (tx, mut rx) = mpsc::channel::<Bytes>(64);
        let handle = tokio::spawn(async move { download_vod(info, vec![addr], tx).await });
        // Nothing that fails verification should ever be emitted.
        while rx.recv().await.is_some() {}
        let result = handle.await.unwrap();
        assert!(
            result.is_err(),
            "a tampered, unrecoverable piece must fail the download"
        );
    }
}
