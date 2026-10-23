//! Session-authorized finite/live MPEG-TS signing and publication.

use crate::broadcast::{Broadcast, BroadcastError, ProducerAuthority, CHUNK_LENGTH, PIECE_LENGTH};
use ace_wire::live_codec::piece_header_from_unix_seconds;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::watch;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IngestError {
    ActiveIngest,
    Sealed,
    Stopping,
    PersistenceFailed,
    PieceIndexExhausted,
    IngestFailed,
}
impl IngestError {
    pub fn code(self) -> &'static str {
        match self {
            Self::ActiveIngest => "producer_conflict",
            Self::Sealed => "ingest_sealed",
            Self::Stopping => "broadcast_stopping",
            Self::PersistenceFailed => "persistence_failed",
            Self::PieceIndexExhausted => "piece_index_exhausted",
            Self::IngestFailed => "ingest_failed",
        }
    }
}
impl std::fmt::Display for IngestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.code())
    }
}
impl std::error::Error for IngestError {}
impl From<BroadcastError> for IngestError {
    fn from(error: BroadcastError) -> Self {
        match error {
            BroadcastError::PersistenceFailed => Self::PersistenceFailed,
            BroadcastError::PieceIndexExhausted => Self::PieceIndexExhausted,
            BroadcastError::ProducerConflict => Self::ActiveIngest,
            BroadcastError::Stopping | BroadcastError::ServiceStopping => Self::Stopping,
            _ => Self::IngestFailed,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct IngestProgress {
    pub aligned_bytes: usize,
    pub committed_pieces: u64,
}

// The last outstanding publication owns the attempt's busy bit, even if the ingest or
// waiting future is dropped. It holds no producer/controller, so there is no owner cycle.
struct Attempt {
    live: AtomicBool,
    busy: Arc<AtomicBool>,
}
impl Drop for Attempt {
    fn drop(&mut self) {
        self.busy.store(false, Ordering::SeqCst);
    }
}
struct IngestPermit(Arc<Attempt>);
impl Drop for IngestPermit {
    fn drop(&mut self) {
        self.0.live.store(false, Ordering::SeqCst);
    }
}

pub struct BroadcastIngest {
    broadcast: Broadcast,
    authority: Arc<ProducerAuthority>,
    permit: IngestPermit,
    resync: ace_media::mpegts::TsResync,
    chunker: ace_wire::signing_chunker::SigningChunker,
    start: u64,
    signature_len: u64,
    payload_capacity: usize,
    buffered_payload: usize,
    sealed: bool,
    failure: Option<IngestError>,
}

impl BroadcastIngest {
    pub(crate) fn authorized(broadcast: Broadcast, authority: Arc<ProducerAuthority>) -> Self {
        let start = broadcast.cursor.start_piece();
        let sig_len = broadcast.auth.signature_len() as u64;
        Self {
            broadcast,
            permit: IngestPermit(Arc::new(Attempt {
                live: AtomicBool::new(true),
                busy: authority.ingest_busy.clone(),
            })),
            authority,
            resync: ace_media::mpegts::TsResync::new(),
            // Rebase the unmodified wire chunker after each complete signed piece. Keep
            // global indexes checked here, without imposing a lifetime byte-count ceiling.
            chunker: ace_wire::signing_chunker::SigningChunker::new(
                PIECE_LENGTH,
                CHUNK_LENGTH,
                0,
                sig_len,
            ),
            start,
            signature_len: sig_len,
            payload_capacity: (PIECE_LENGTH - sig_len) as usize,
            buffered_payload: 0,
            sealed: false,
            failure: None,
        }
    }

    fn check(&self) -> Result<(), IngestError> {
        if let Some(error) = self.failure {
            return Err(error);
        }
        if self.sealed {
            return Err(IngestError::Sealed);
        }
        self.broadcast.cursor.check()?;
        if *self.authority.cancel.borrow() || !self.permit.0.live.load(Ordering::SeqCst) {
            return Err(IngestError::Stopping);
        }
        Ok(())
    }
    fn fail(&mut self, error: IngestError) -> IngestError {
        self.failure = Some(error);
        self.sealed = true;
        self.permit.0.live.store(false, Ordering::SeqCst);
        // The owner observes the error and decides its lifetime. In particular a terminal
        // pull failure must retain its claim/status until explicit cancellation/detach.
        error
    }

    pub async fn push_bytes(&mut self, bytes: &[u8]) -> Result<IngestProgress, IngestError> {
        self.check()?;
        let mut progress = IngestProgress::default();
        for slice in bytes.chunks(64 * 1024) {
            self.check()?;
            let aligned = self.resync.push(slice);
            progress.aligned_bytes += aligned.len();
            let mut remaining = aligned.as_slice();
            while !remaining.is_empty() {
                self.check()?;
                let take = remaining
                    .len()
                    .min(self.payload_capacity - self.buffered_payload);
                let outputs = self.chunker.push(&remaining[..take], &self.broadcast.auth);
                self.buffered_payload += take;
                remaining = &remaining[take..];
                // A canceled waiter cannot subsequently reuse advanced signing state. The
                // owned publication job still keeps its permit/fences until it settles.
                self.failure = Some(IngestError::IngestFailed);
                match self.store_outputs(outputs, false).await {
                    Ok(pieces) => {
                        self.failure = None;
                        progress.committed_pieces += pieces;
                    }
                    Err(error) => return Err(self.fail(error)),
                }
                if self.buffered_payload == self.payload_capacity {
                    self.start = match self.start.checked_add(1) {
                        Some(next) => next,
                        None => return Err(self.fail(IngestError::PieceIndexExhausted)),
                    };
                    self.buffered_payload = 0;
                    self.chunker = ace_wire::signing_chunker::SigningChunker::new(
                        PIECE_LENGTH,
                        CHUNK_LENGTH,
                        0,
                        self.signature_len,
                    );
                }
            }
        }
        Ok(progress)
    }

    /// Ordinary finite EOF only. Cancellation/fatal errors discard the buffered tail.
    pub async fn finish(&mut self) -> Result<(), IngestError> {
        if self.sealed {
            return self.failure.map_or(Ok(()), Err);
        }
        self.check()?;
        self.sealed = true;
        self.failure = Some(IngestError::IngestFailed);
        let outputs = self.chunker.flush(&self.broadcast.auth);
        match self.store_outputs(outputs, true).await {
            Ok(_) => {
                self.failure = None;
                Ok(())
            }
            Err(error) => Err(self.fail(error)),
        }
    }

    async fn store_outputs(
        &self,
        mut outputs: Vec<ace_wire::chunker::OutChunk>,
        checkpoint: bool,
    ) -> Result<u64, IngestError> {
        if outputs.is_empty() && !checkpoint {
            return Ok(0);
        }
        for out in &mut outputs {
            out.piece = self
                .start
                .checked_add(out.piece)
                .filter(|piece| *piece < u64::MAX)
                .ok_or(IngestError::PieceIndexExhausted)?;
        }
        let publication = self.broadcast.cursor.publication.clone().lock_owned().await;
        let broadcast = self.broadcast.clone();
        let attempt = self.permit.0.clone();
        let cancel = self.authority.cancel.subscribe();
        let (tx, mut rx) = watch::channel(None);
        let job = tokio::spawn(async move {
            let result = async move {
                let mut publication = publication;
                let mut current_header = None;
                let mut pieces = 0;
                for out in outputs {
                    check_publication(&broadcast, &attempt, &cancel)?;
                    broadcast.cursor.consume(out.piece).await?;
                    let mut waiting_cancel = cancel.clone();
                    let store = tokio::select! {
                        biased;
                        () = crate::broadcast::cancelled(&mut waiting_cancel) => return Err(IngestError::Stopping),
                        store = broadcast.store.clone().lock_owned() => store,
                    };
                    check_publication(&broadcast, &attempt, &cancel)?;
                    let header = header_for_piece(&mut current_header, out.piece);
                    let completes_piece = u64::from(out.chunk) + 1 == PIECE_LENGTH / CHUNK_LENGTH
                        && out.data.len() == CHUNK_LENGTH as usize;
                    let current = broadcast.clone();
                    let current_attempt = attempt.clone();
                    let current_cancel = cancel.clone();
                    // The real store mutation runs off the executor with both guards owned
                    // by the blocking job. Check again after its queue/store-lock wait, right
                    // before exposure. Cancellation cannot detach an already-started write
                    // from its publication fence. The store's normal budget/eviction apply.
                    let (returned, result) = tokio::task::spawn_blocking(move || {
                        let mut store = store;
                        let result = check_publication(&current, &current_attempt, &current_cancel)
                            .map(|()| store.put_chunk_with_header(out.piece, out.chunk, header, &out.data));
                        (publication, result)
                    }).await.map_err(|_| IngestError::IngestFailed)?;
                    publication = returned;
                    result?;
                    if completes_piece { pieces += 1; }
                }
                let _publication = publication;
                if checkpoint {
                    check_publication(&broadcast, &attempt, &cancel)?;
                    broadcast.cursor.checkpoint().await?;
                }
                Ok(pieces)
            }.await;
            tx.send_replace(Some(result));
        });
        {
            let mut jobs = self.authority.jobs.lock().unwrap();
            jobs.retain(|job| !job.is_finished());
            jobs.push(job);
        }
        loop {
            if let Some(result) = *rx.borrow_and_update() {
                return result;
            }
            if rx.changed().await.is_err() {
                return Err(IngestError::IngestFailed);
            }
        }
    }
}

fn check_publication(
    broadcast: &Broadcast,
    attempt: &Attempt,
    cancel: &watch::Receiver<bool>,
) -> Result<(), IngestError> {
    broadcast.cursor.check()?;
    if *cancel.borrow() || !attempt.live.load(Ordering::SeqCst) {
        return Err(IngestError::Stopping);
    }
    Ok(())
}

fn current_live_piece_header() -> [u8; 8] {
    let seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64();
    piece_header_from_unix_seconds(seconds)
}
fn header_for_piece(current: &mut Option<(u64, [u8; 8])>, piece: u64) -> [u8; 8] {
    match current {
        Some((current_piece, header)) if *current_piece == piece => *header,
        _ => {
            let header = current_live_piece_header();
            *current = Some((piece, header));
            header
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::broadcast::{BroadcastRegistry, ProducerKind};
    use ace_swarm::listen::SeedRegistry;

    fn ts_packets(count: usize) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(count * 188);
        for i in 0..count {
            bytes.push(0x47);
            bytes.extend(std::iter::repeat_n((i % 251) as u8, 187));
        }
        bytes
    }

    #[tokio::test]
    async fn shared_ingest_writes_signed_ts_chunks_to_the_real_store() {
        let registry = BroadcastRegistry::new();
        let seed = SeedRegistry::new();
        let (broadcast, mut producer) = registry
            .acquire_producer(
                "ingest",
                "Ingest",
                &[],
                &seed,
                4 << 20,
                ProducerKind::RawPut,
            )
            .await
            .unwrap();
        let mut ingest = producer.open_ingest().unwrap();
        ingest.push_bytes(&ts_packets(400)).await.unwrap();
        ingest.finish().await.unwrap();
        let store = broadcast.store.lock().await;
        assert!(
            store.chunk(0, 0).is_some(),
            "real store contains the signed first chunk"
        );
        assert_ne!(store.piece_header(0).unwrap(), [0; 8]);
    }

    #[tokio::test]
    async fn a_new_real_ingest_starts_at_the_next_committed_piece() {
        let registry = BroadcastRegistry::new();
        let seed = SeedRegistry::new();
        let (broadcast, mut producer) = registry
            .acquire_producer("resume", "Resume", &[], &seed, 8 << 20, ProducerKind::Pull)
            .await
            .unwrap();
        let mut first = producer.open_ingest().unwrap();
        first.push_bytes(&ts_packets(700)).await.unwrap();
        first.finish().await.unwrap();
        drop(first);
        let next_piece = broadcast.cursor.start_piece();
        assert!(next_piece > 0);

        let mut retry = producer.open_ingest().unwrap();
        retry.push_bytes(&ts_packets(400)).await.unwrap();
        retry.finish().await.unwrap();
        let store = broadcast.store.lock().await;
        assert!(store.chunk(next_piece, 0).is_some());
        assert!(store.chunk(0, 0).is_some());
    }
}

#[cfg(test)]
mod tests_pull206;
