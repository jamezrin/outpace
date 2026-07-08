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
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc;

/// True iff `bytes` hashes (SHA-1) to `expected` — standard BitTorrent piece integrity.
pub fn verify_piece(expected: &[u8; 20], bytes: &[u8]) -> bool {
    let digest = Sha1::digest(bytes);
    digest.as_slice() == expected
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
    let piece_count = info.piece_count();
    let mut next_piece: u64 = 0;
    let mut peer_idx = 0usize;
    // Stall budget: consecutive attempts that made no progress. Reset whenever `next_piece`
    // advances, so a long download that keeps delivering pieces across reconnecting peers is
    // never failed for "using up attempts" — only a genuine stall (no piece advanced) is.
    let mut attempts = 0usize;
    let max_attempts = peers.len().max(1) * 3;
    while next_piece < piece_count {
        if peers.is_empty() || attempts >= max_attempts {
            return Err(VodError::Unrecoverable(next_piece));
        }
        attempts += 1;
        let addr = peers[peer_idx % peers.len()];
        peer_idx += 1;
        let progress_before = next_piece;

        let mut session = match connect(&addr.to_string()).await {
            Ok(s) => s,
            Err(_) => continue,
        };
        if session
            .perform_handshake(info.infohash, random_peer_id())
            .await
            .is_err()
        {
            continue;
        }
        match drain_from_peer(&mut session, &info, &mut next_piece, &tx).await {
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
    tx: &mpsc::Sender<Bytes>,
) -> Result<(), VodError> {
    session
        .send(&PeerMessage::Interested)
        .await
        .map_err(|_| VodError::Unrecoverable(*next_piece))?;
    let piece_count = info.piece_count();
    let mut unchoked = false;
    while *next_piece < piece_count {
        let idx = *next_piece;
        let piece_len = info.piece_size(idx) as usize;
        let mut assembled = vec![0u8; piece_len];
        let mut have = 0usize;
        if unchoked {
            request_piece_blocks(session, info, idx, piece_len)
                .await
                .map_err(|_| VodError::Unrecoverable(idx))?;
        }
        loop {
            let msg = match session.read_message().await {
                Ok(m) => m,
                Err(_) => return Err(VodError::Unrecoverable(idx)),
            };
            match msg {
                PeerMessage::Unchoke => {
                    if !unchoked {
                        unchoked = true;
                        request_piece_blocks(session, info, idx, piece_len)
                            .await
                            .map_err(|_| VodError::Unrecoverable(idx))?;
                    }
                }
                PeerMessage::Choke => unchoked = false,
                PeerMessage::Piece { index, begin, block } if index == idx as u32 => {
                    let start = begin as usize;
                    if start < piece_len {
                        let end = (start + block.len()).min(piece_len);
                        assembled[start..end].copy_from_slice(&block[..end - start]);
                        have += end - start;
                    }
                    if have >= piece_len {
                        break;
                    }
                }
                // Ignore Have / Bitfield / unrelated-piece / other messages; keep reading.
                _ => {}
            }
        }
        if !verify_piece(&info.piece_hashes[idx as usize], &assembled) {
            // This peer served a piece that failed its hash; abandon it (keep the cursor).
            return Err(VodError::Unrecoverable(idx));
        }
        tx.send(Bytes::from(assembled))
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
) -> ace_peer::Result<()> {
    let block = info.chunk_length as usize;
    let mut begin = 0usize;
    while begin < piece_len {
        let length = block.min(piece_len - begin);
        session
            .send(&PeerMessage::Request {
                index: idx as u32,
                begin: begin as u32,
                length: length as u32,
            })
            .await?;
        begin += length;
    }
    Ok(())
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
    async fn spawn_seeder(content: Vec<u8>, info: VodInfo, tamper: bool) -> SocketAddrV4 {
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
                        Handshake::new(info.infohash, ace_wire::handshake::random_peer_id()).encode();
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
                                    if let PeerMessage::Request { index, begin, length } = msg {
                                        let start = (index as u64 * info.piece_length
                                            + begin as u64)
                                            as usize;
                                        let end = start + length as usize;
                                        let mut block = content[start..end].to_vec();
                                        if tamper && index == 0 && begin == 0 {
                                            block[0] ^= 0xff;
                                        }
                                        let piece =
                                            PeerMessage::Piece { index, begin, block }.encode();
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
        let addr = spawn_seeder(content.clone(), info.clone(), false).await;
        let (tx, mut rx) = mpsc::channel::<Bytes>(64);
        let handle = tokio::spawn(async move { download_vod(info, vec![addr], tx).await });
        let mut got = Vec::new();
        while let Some(chunk) = rx.recv().await {
            got.extend_from_slice(&chunk);
        }
        handle.await.unwrap().unwrap();
        assert_eq!(got, content);
    }

    // A seeder that serves at most `pieces_per_conn` distinct pieces per connection, then drops.
    // Forces the client to reconnect and resume — exercising progress across many short peers.
    async fn spawn_flaky_seeder(content: Vec<u8>, info: VodInfo, pieces_per_conn: usize) -> SocketAddrV4 {
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
                        Handshake::new(info.infohash, ace_wire::handshake::random_peer_id()).encode();
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
                    let mut served: std::collections::HashSet<u32> = std::collections::HashSet::new();
                    let mut buf: Vec<u8> = Vec::new();
                    let mut tmp = [0u8; 4096];
                    loop {
                        loop {
                            match PeerMessage::decode(&buf) {
                                Ok(Some((msg, used))) => {
                                    buf.drain(..used);
                                    if let PeerMessage::Request { index, begin, length } = msg {
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
                                        let piece =
                                            PeerMessage::Piece { index, begin, block }.encode();
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
        let addr = spawn_seeder(content, info.clone(), true).await;
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
