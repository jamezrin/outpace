use super::*;

/// One whole verified piece from the cache, assembled chunk by chunk. `shared_block` is the
/// peer-facing API and refuses anything over one 16 KiB BitTorrent request, so asking it for a
/// whole multi-chunk piece always missed and every overlapping read re-downloaded the piece.
/// Any absent or short chunk is a miss rather than a shifted, partial piece.
async fn cached_piece(
    store: &Arc<tokio::sync::Mutex<PieceStore>>,
    info: &VodInfo,
    piece: u64,
) -> Option<Bytes> {
    let length = info.piece_size(piece);
    let chunk_length = info.chunk_length;
    if length == 0 || length > ace_swarm::resolve::MAX_PIECE_LENGTH || chunk_length == 0 {
        return None;
    }
    let chunks = u16::try_from(length.div_ceil(chunk_length)).ok()?;
    // Check the first chunk before reserving a whole piece, so a miss allocates nothing extra.
    let (mut bytes, _) = PieceStore::shared_chunk(store, piece, 0).await?;
    if bytes.len() as u64 != length.min(chunk_length) {
        return None;
    }
    bytes.reserve_exact(length as usize - bytes.len());
    for index in 1..chunks {
        let (chunk, _) = PieceStore::shared_chunk(store, piece, index).await?;
        let expected = (length - u64::from(index) * chunk_length).min(chunk_length);
        if chunk.len() as u64 != expected {
            return None;
        }
        bytes.extend_from_slice(&chunk);
    }
    Some(Bytes::from(bytes))
}

/// Remember a discovery result for later range reads, but only a nonempty one: caching an empty
/// result would make a VOD whose first lookup found nobody unplayable for the store's lifetime.
fn remember_peers(slot: &mut Option<Vec<SocketAddrV4>>, discovered: &[SocketAddrV4]) {
    if !discovered.is_empty() {
        *slot = Some(discovered.to_vec());
    }
}

/// The leading run of `[first_piece, end_piece)` already present in `cache`. Returns those pieces'
/// bytes plus `download_from`, the first index still needing download — so `open_range` serves the
/// cached prefix from memory and only fetches `[download_from, end_piece)`. For the common
/// sequential-read pattern (each segment covers a still-hot piece plus at most one new one), the
/// shared piece is served from cache and never re-downloaded.
async fn cached_prefix(
    store: &Arc<tokio::sync::Mutex<PieceStore>>,
    info: &VodInfo,
    first_piece: u64,
    end_piece: u64,
) -> (Vec<Bytes>, u64) {
    let mut prefix = Vec::new();
    let mut p = first_piece;
    while p < end_piece {
        match cached_piece(store, info, p).await {
            Some(bytes) => {
                prefix.push(bytes);
                p += 1;
            }
            None => break,
        }
    }
    (prefix, p)
}

/// A resolved VOD ready to serve byte ranges. Holds the descriptor geometry plus the discovery
/// inputs `open_range` needs, and a shared bounded piece cache + once-discovered peer list so the
/// many range reads of one playback (HLS segments, seeks) resolve peers once and download each
/// covering piece at most once.
pub(super) struct AceVodContent {
    pub(super) peer_policy: ace_swarm::peer_policy::PeerPolicy,
    pub(super) info: VodInfo,
    pub(super) bootstrap_peers: Vec<SocketAddrV4>,
    pub(super) announce_peer_port: tokio::sync::watch::Receiver<Option<u16>>,
    /// Shared cache/reseed store and the producer lease anchoring its registry lifetime.
    pub(super) store: (Arc<tokio::sync::Mutex<PieceStore>>, SeedLease),
    /// Nonempty discovery result reused by every range read (VOD swarms are stable, unlike the
    /// live edge). `None` until a read discovers at least one peer.
    pub(super) peers: Arc<tokio::sync::Mutex<Option<Vec<SocketAddrV4>>>>,
    /// Serializes cache inspection plus download streams so overlapping requests never fetch
    /// and write the same piece concurrently.
    pub(super) range_lock: Arc<tokio::sync::Mutex<()>>,
}

impl AceVodContent {
    /// Peers for this VOD, discovered once and cached (an empty result is retried next read). Bootstrap peers (tests / explicit config)
    /// are used directly. VOD swarms are standard BitTorrent, discovered like the live path.
    async fn peers(&self) -> Vec<SocketAddrV4> {
        let mut guard = self.peers.lock().await;
        if self.peer_policy.is_private() {
            return self.peer_policy.endpoints();
        }
        if let Some(peers) = guard.as_ref() {
            return peers.clone();
        }
        let discovered = if self.bootstrap_peers.is_empty() {
            let announce_port = self.announce_peer_port.borrow().unwrap_or(0);
            discover_peers(
                &self.info.trackers,
                &self.info.infohash,
                &random_peer_id(),
                announce_port,
            )
            .await
        } else {
            self.bootstrap_peers.clone()
        };
        remember_peers(&mut guard, &discovered);
        discovered
    }
}

#[async_trait]
impl VodContent for AceVodContent {
    fn content_length(&self) -> u64 {
        self.info.total_length
    }

    async fn open_range(
        &self,
        start: u64,
        end: u64,
    ) -> Result<Box<dyn VodByteSource>, ProviderError> {
        let total = self.info.total_length;
        if total == 0 || start > end || end >= total {
            return Err(ProviderError::Backend(format!(
                "vod range [{start}, {end}] out of bounds for {total}-byte content"
            )));
        }
        let piece_length = self.info.piece_length;
        let range_guard = self.range_lock.clone().lock_owned().await;
        // The contiguous pieces covering [start, end]. `download_vod_pieces` verifies each whole
        // piece against its SHA-1 hash before emitting it, so trimming its output to the range
        // never yields an unverified byte.
        let first_piece = start / piece_length;
        let end_piece = end / piece_length + 1;

        // Serve any already-cached leading pieces from memory; only download the missing suffix.
        let (prefix, download_from) =
            cached_prefix(&self.store.0, &self.info, first_piece, end_piece).await;

        let (tx, rx) = mpsc::channel::<Bytes>(64);
        if download_from < end_piece {
            // VOD swarms are standard BitTorrent; discover the same way live does (once, cached).
            let peers = self.peers().await;
            crate::alog!(
                "[ace] open_range [{start}, {end}]: pieces [{download_from}, {end_piece}) \
                 (prefix {} cached), {} peer(s)",
                prefix.len(),
                peers.len()
            );
            if peers.is_empty() {
                return Err(ProviderError::NoPeers("no VOD peers discovered".into()));
            }
            let info = self.info.clone();
            let store = self.store.0.clone();
            let chunk_length = self.info.chunk_length;
            // Download the missing suffix, teeing every whole verified piece into the cache as it
            // streams so a later overlapping read finds it instead of re-downloading. The
            // downloader runs detached: if the consumer drops (leaving the tee loop, dropping
            // `drx`), `download_vod_pieces` sees `ConsumerGone` on its next send and stops.
            let (dtx, mut drx) = mpsc::channel::<Bytes>(64);
            let peer_policy = self.peer_policy.clone();
            let downloader = tokio::spawn(async move {
                if let Err(e) = ace_swarm::vod::download_vod_pieces_with_peer_policy(
                    info,
                    peers,
                    dtx,
                    download_from,
                    end_piece,
                    peer_policy,
                )
                .await
                {
                    crate::alog!("[ace] vod range download ended: {e:?}");
                }
            });
            tokio::spawn(async move {
                // Keep the range transaction serialized until the downloader has observed
                // cancellation and exited, not merely until the HTTP consumer drops its source.
                let _range_guard = range_guard;
                let mut idx = download_from;
                loop {
                    let piece = tokio::select! {
                        _ = tx.closed() => break,
                        piece = drx.recv() => match piece {
                            Some(piece) => piece,
                            None => break,
                        },
                    };
                    // The downloader emits only whole pieces after SHA-1 verification. Populate
                    // the shared store only here, never from partial/unverified peer blocks.
                    for (chunk, data) in piece.chunks(chunk_length as usize).enumerate() {
                        PieceStore::shared_put_chunk_with_header(
                            &store,
                            idx,
                            chunk as u16,
                            [0; 8],
                            data,
                        )
                        .await;
                    }
                    idx += 1;
                    if tx.send(piece).await.is_err() {
                        break;
                    }
                }
                drop(drx);
                let _ = downloader.await;
            });
        } else {
            drop(range_guard);
        }
        // else: the whole range is cached — `tx` drops here, closing `rx`, so the source emits
        // only the cached prefix.

        // The first covering piece begins at `first_piece * piece_length`; drop the bytes before
        // `start`, then emit exactly the range length.
        let skip = start - first_piece * piece_length;
        let emit_len = end - start + 1;
        Ok(Box::new(VodSource {
            prefix: prefix.into(),
            rx,
            skip,
            remaining: emit_len,
            emit_len,
        }))
    }
}

/// Streams verified whole-piece bytes trimmed to a byte range: it drops `skip` leading bytes (the
/// offset of `start` inside the first covering piece) and stops after `emit_len` bytes, so a caller
/// only ever sees the requested `[start, end]`. Pieces come first from `prefix` (leading pieces
/// already in the VOD's cache) and then from `rx` (the freshly downloaded suffix) — a single
/// contiguous, in-order stream of the covering pieces.
pub(super) struct VodSource {
    /// Cached leading covering pieces, emitted (and trimmed) before anything from `rx`.
    pub(super) prefix: VecDeque<Bytes>,
    pub(super) rx: mpsc::Receiver<Bytes>,
    /// Bytes still to drop from the front before emitting (offset of `start` in the first piece).
    pub(super) skip: u64,
    /// Bytes still to emit before end-of-range.
    pub(super) remaining: u64,
    /// Total bytes this source will emit (the range length); constant after construction.
    pub(super) emit_len: u64,
}

#[async_trait]
impl VodByteSource for VodSource {
    fn content_length(&self) -> u64 {
        self.emit_len
    }
    async fn next(&mut self) -> Option<Bytes> {
        while self.remaining > 0 {
            // Cached prefix pieces first, then the downloaded suffix.
            let mut chunk = match self.prefix.pop_front() {
                Some(chunk) => chunk,
                None => self.rx.recv().await?,
            };
            if self.skip > 0 {
                if self.skip >= chunk.len() as u64 {
                    self.skip -= chunk.len() as u64;
                    continue;
                }
                chunk = chunk.slice(self.skip as usize..);
                self.skip = 0;
            }
            if chunk.len() as u64 > self.remaining {
                chunk = chunk.slice(..self.remaining as usize);
            }
            self.remaining -= chunk.len() as u64;
            return Some(chunk);
        }
        None
    }
}

#[cfg(test)]
mod remember_peers_tests {
    use super::*;

    #[test]
    fn empty_discovery_is_not_cached_but_a_nonempty_one_is() {
        let mut slot = None;
        remember_peers(&mut slot, &[]);
        assert_eq!(
            slot, None,
            "an empty result must leave the next read free to rediscover"
        );

        let peer = SocketAddrV4::new(std::net::Ipv4Addr::LOCALHOST, 6881);
        remember_peers(&mut slot, &[peer]);
        assert_eq!(slot, Some(vec![peer]));
    }
}
