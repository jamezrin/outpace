use super::*;

/// First piece to request given a peer window and a configured prefetch depth.
pub(in crate::ace_provider) fn prefetch_start(
    min_piece: u64,
    max_piece: u64,
    prefetch: u64,
) -> u64 {
    max_piece.saturating_sub(prefetch).max(min_piece)
}

impl Continuity {
    /// The very first peer connection for this stream: bootstrap from its window.
    pub(in crate::ace_provider) fn fresh(
        info: &StreamInfo,
        min_piece: u64,
        max_piece: u64,
        prefetch: u64,
        live_recovery: LiveRecoveryConfig,
    ) -> (Continuity, u64) {
        let start = prefetch_start(min_piece, max_piece, prefetch);
        // Strip the per-piece signature tail from the emitted media stream, and — when the
        // resolved transport gave us the source's `pubkey` — verify each piece's in-band RSA
        // signature before its bytes can be served (issue #10). A descriptor without a
        // parseable pubkey yields `sig_len == 0` and an empty `source_pubkey`, so nothing is
        // stripped or verified.
        let reasm = PieceReassembler::new(info.piece_length, start)
            .with_piece_trailer(info.sig_len as u64)
            .with_source_pubkey(info.source_pubkey.clone())
            .with_max_pieces_ahead(live_recovery.max_reasm_pieces_ahead);
        (
            Continuity {
                live_recovery,
                reasm,
                resync: ace_media::mpegts::TsResync::new(),
                output_gate: ace_media::mpegts::KeyframeGate::new_passthrough(),
                transport_recovery: None,
                discontinuity_pending: false,
                scheduler: Scheduler::new(live_recovery.max_piece_advance as usize),
                active_peers: ActivePeers::new(),
                received_chunks: BTreeMap::new(),
                piece_producers: BTreeMap::new(),
                #[cfg(test)]
                contribution_observer: None,
                requested_at: HashMap::new(),
                next_needed_since: Instant::now(),
                head: max_piece,
                startup_deadline: None,
                request_floor: Arc::new(AtomicU64::new(start)),
                prefetch,
                emitted: 0,
                next_log: 1 << 20,
                authenticated_logged: false,
            },
            start,
        )
    }

    /// A reconnect to a new peer after losing the previous one: keep going from where we
    /// left off rather than restarting near the new peer's head (which would duplicate
    /// already-served pieces into the broadcast). Only skips forward — an unavoidable,
    /// logged gap — if the new peer's window has already evicted the piece we still needed
    /// (we were disconnected longer than the live window covers). Returns the piece index
    /// to advertise as our position in the outgoing handshake.
    pub(in crate::ace_provider) fn resume(
        &mut self,
        addr: SocketAddrV4,
        min_piece: u64,
        max_piece: u64,
    ) -> u64 {
        self.head = self.head.max(max_piece);
        self.scheduler.clear_in_flight();
        self.active_peers = ActivePeers::new();
        self.received_chunks.clear();
        // Retained partial and authenticated ahead pieces keep their producer evidence.
        self.requested_at.clear();
        self.next_needed_since = Instant::now();
        let next = self.reasm.next_needed();
        let resume = next.max(min_piece);
        if resume > next {
            crate::alog!(
                "[ace] {addr}: reconnect gap — peer's window already evicted pieces {next}..{}; skipping ahead",
                resume - 1
            );
            self.reasm.skip_to(resume);
            self.arm_output_gate();
        }
        self.piece_producers.retain(|&piece, _| piece >= resume);
        resume
    }

    /// Whether a peer advertising `window` can still serve the piece we need next.
    ///
    /// A window that already covers the piece trivially can. The subtle case is the live edge:
    /// once we are caught up, `next_needed` is the piece *after* the newest one the source has
    /// produced, so no peer in the swarm can advertise it yet. Requiring coverage there rejects
    /// every peer at once and leaves the pool reconnecting into "no usable upstreams" until some
    /// advertisement drifts forward — a multi-second output gap on a stream that was healthy.
    /// A peer that is current with the live edge is precisely the one to wait on: it delivers the
    /// piece via a live HAVE as soon as the source produces it. A peer whose window ends short of
    /// the edge is genuinely behind and still rejected.
    pub(in crate::ace_provider) fn window_can_resume(&self, window: &LivePosition) -> bool {
        let needed = i64::try_from(self.reasm.next_needed()).unwrap_or(i64::MAX);
        if window.max_piece >= needed {
            return true;
        }
        let head = i64::try_from(self.head).unwrap_or(i64::MAX);
        needed > head && window.max_piece >= head
    }

    /// Rejected bytes cannot complete a request. Forget the partial chunk count and all
    /// outstanding copies, including requests assigned before a retransmission to another peer.
    pub(in crate::ace_provider) fn release_rejected_piece(&mut self, piece: u64) {
        self.reasm.discard_partial(piece);
        self.received_chunks.remove(&piece);
        self.piece_producers.remove(&piece);
        self.scheduler.on_drop(piece);
        self.active_peers.complete_everywhere(piece);
        self.requested_at.remove(&piece);
    }

    pub(in crate::ace_provider) fn note_chunk(
        &mut self,
        piece: u64,
        chunk: u16,
        chunks_per_piece: u16,
    ) -> bool {
        let chunks_per_piece = chunks_per_piece.max(1) as usize;
        let chunks = self.received_chunks.entry(piece).or_default();
        chunks.insert(chunk);
        if chunks.len() >= chunks_per_piece {
            self.received_chunks.remove(&piece);
            self.scheduler.on_complete(piece);
            // The piece may have been re-requested from more than one peer (retransmission);
            // clear it from every peer's slot and stop its retransmit timer.
            self.active_peers.complete_everywhere(piece);
            self.requested_at.remove(&piece);
            true
        } else {
            false
        }
    }

    /// Pieces still outstanding past the configured request timeout — candidates to re-request.
    /// Also prunes timer bookkeeping for pieces the cursor has already advanced past.
    pub(in crate::ace_provider) fn timed_out_requests(&mut self, now: Instant) -> Vec<u64> {
        let next = self.reasm.next_needed();
        self.requested_at.retain(|&p, _| p >= next);
        self.requested_at
            .iter()
            .filter(|(_, &at)| now.duration_since(at) >= self.live_recovery.request_timeout())
            .map(|(&p, _)| p)
            .collect()
    }

    /// If the cursor has been stuck past the configured request timeout on a piece no unchoked
    /// upstream can still serve (evicted from every window), skip forward to the lowest piece some peer
    /// does have. Returns the skip target if it skipped. This is the mid-session analogue of
    /// [`resume`](Self::resume)'s reconnect-gap skip — recovering without a full teardown.
    pub(in crate::ace_provider) fn skip_evicted_gap(&mut self, now: Instant) -> Option<u64> {
        let next = self.reasm.next_needed();
        if now.duration_since(self.next_needed_since) < self.live_recovery.request_timeout()
            || self.active_peers.any_unchoked_covers(next)
        {
            return None;
        }
        let floor = self.active_peers.lowest_covered_piece()?;
        if floor <= next {
            return None;
        }
        self.reasm.skip_to(floor);
        self.arm_output_gate();
        self.active_peers.prune_below(floor);
        self.requested_at.retain(|&p, _| p >= floor);
        self.received_chunks.retain(|&p, _| p >= floor);
        self.piece_producers.retain(|&p, _| p >= floor);
        self.next_needed_since = now;
        Some(floor)
    }

    /// Enforce the configured live cushion immediately, before requests or publication.
    /// This also drops stale partial/completed pieces and releases their peer slots when
    /// a newly learned head outruns the cursor. Once output exists, a forward skip uses
    /// the same fresh discontinuity gate as an unknown whole-piece loss (issue #169).
    /// Provisional startup positioning has no previously published stream to interrupt.
    pub(in crate::ace_provider) fn skip_far_behind_live(&mut self, now: Instant) -> Option<u64> {
        self.request_floor
            .fetch_max(self.head.saturating_sub(self.prefetch), Ordering::Relaxed);
        let next = self.reasm.next_needed();
        if self.head.saturating_sub(next) <= self.prefetch {
            return None;
        }
        let target = self.head.saturating_sub(self.prefetch);
        self.reasm.skip_to(target);
        if self.emitted > 0 {
            self.arm_output_gate();
        } else {
            // No published stream exists to mark discontinuous at provisional startup.
            self.resync = ace_media::mpegts::TsResync::new();
        }
        self.active_peers.prune_below(target);
        self.requested_at.retain(|&p, _| p >= target);
        self.received_chunks.retain(|&p, _| p >= target);
        self.piece_producers.retain(|&p, _| p >= target);
        self.next_needed_since = now;
        Some(target)
    }

    pub(in crate::ace_provider) fn arm_output_gate(&mut self) {
        if let Some(recovery) = self.transport_recovery.take() {
            crate::alog!(
                "[mpegts] transport recovery superseded by piece skip: discarded_bytes={} withheld_bytes={} duration_ms={} loss_events={}",
                recovery.discarded_bytes,
                recovery.withheld_bytes,
                recovery.started.elapsed().as_millis(),
                recovery.loss_events
            );
        }
        self.output_gate = ace_media::mpegts::KeyframeGate::new();
        self.resync = ace_media::mpegts::TsResync::new();
        self.discontinuity_pending = true;
    }

    fn filter_output_after_discontinuity(&mut self, aligned: &[u8]) -> Option<LiveOutput> {
        let output = self.output_gate.push_report(aligned);
        if let Some(recovery) = &mut self.transport_recovery {
            recovery.withheld_bytes += output.withheld_bytes;
        }
        if let Some(reason) = output.resumed {
            if let Some(recovery) = self.transport_recovery.take() {
                crate::alog!(
                    "[mpegts] transport recovery resumed: reason={reason:?} discarded_bytes={} withheld_bytes={} duration_ms={} loss_events={}",
                    recovery.discarded_bytes,
                    recovery.withheld_bytes,
                    recovery.started.elapsed().as_millis(),
                    recovery.loss_events
                );
            }
        }
        (!output.bytes.is_empty()).then(|| LiveOutput {
            bytes: Bytes::from(output.bytes),
            discontinuity: self.take_discontinuity(),
        })
    }

    pub(in crate::ace_provider) fn resync_output(&mut self, bytes: &[u8]) -> Vec<LiveOutput> {
        let output = self.resync.push_report(bytes);
        let mut chunks = Vec::new();
        let mut start = 0;
        let mut emitted = self.emitted;
        for boundary in output.boundaries {
            if let Some(prefix) =
                self.filter_output_after_discontinuity(&output.bytes[start..boundary.output_offset])
            {
                emitted += prefix.bytes.len() as u64;
                chunks.push(prefix);
            }
            crate::alog!(
                "[mpegts] transport resync discarded boundary bytes: discarded_bytes={} batch_aligned_offset={} stream_output_offset={}",
                boundary.discarded_bytes,
                boundary.output_offset,
                emitted
            );
            let recovery = self
                .transport_recovery
                .get_or_insert_with(|| TransportRecovery {
                    started: Instant::now(),
                    discarded_bytes: 0,
                    withheld_bytes: 0,
                    loss_events: 0,
                });
            recovery.discarded_bytes += boundary.discarded_bytes;
            recovery.loss_events += 1;
            self.output_gate.rearm_for_discontinuity();
            self.discontinuity_pending = true;
            start = boundary.output_offset;
        }
        if let Some(suffix) = self.filter_output_after_discontinuity(&output.bytes[start..]) {
            chunks.push(suffix);
        }
        chunks
    }

    pub(in crate::ace_provider) fn take_discontinuity(&mut self) -> bool {
        std::mem::take(&mut self.discontinuity_pending)
    }

    #[cfg(test)]
    pub(in crate::ace_provider) fn output_gate_armed(&self) -> bool {
        !self.output_gate.is_locked()
    }

    pub(in crate::ace_provider) fn register_active_peer(
        &mut self,
        id: u64,
        addr: SocketAddrV4,
        window: LivePosition,
    ) {
        self.active_peers.insert(id, addr, window);
    }

    pub(in crate::ace_provider) fn set_peer_unchoked(&mut self, id: u64, unchoked: bool) {
        self.active_peers.set_unchoked(id, unchoked);
    }

    pub(in crate::ace_provider) fn update_peer_window(
        &mut self,
        id: u64,
        min_piece: u64,
        max_piece: u64,
    ) {
        self.active_peers.update_window(id, min_piece, max_piece);
    }
}
