//! Shared MPEG-TS broadcast ingest path used by raw HTTP PUT and RTMP ingest.

use crate::broadcast::{BroadcastCursor, CHUNK_LENGTH, PIECE_LENGTH};
use ace_swarm::store::PieceStore;
use ace_wire::live_auth::LiveSourceAuth;
use ace_wire::live_codec::piece_header_from_unix_seconds;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex;

pub struct BroadcastIngest {
    store: Arc<Mutex<PieceStore>>,
    auth: Arc<LiveSourceAuth>,
    cursor: Arc<BroadcastCursor>,
    resync: ace_media::mpegts::TsResync,
    chunker: ace_wire::signing_chunker::SigningChunker,
    current_header: Option<(u64, [u8; 8])>,
}

impl BroadcastIngest {
    /// Start (or resume) ingest. Piece numbering begins at `cursor.start_piece()`, so a second
    /// ingest for the same broadcast continues the sequence instead of restarting at 0, and the
    /// cursor is advanced (and throttle-persisted) as pieces are produced.
    pub fn new(
        store: Arc<Mutex<PieceStore>>,
        auth: Arc<LiveSourceAuth>,
        cursor: Arc<BroadcastCursor>,
    ) -> Self {
        let sig_len = auth.signature_len() as u64;
        let start_piece = cursor.start_piece();
        Self {
            store,
            auth,
            cursor,
            resync: ace_media::mpegts::TsResync::new(),
            chunker: ace_wire::signing_chunker::SigningChunker::new(
                PIECE_LENGTH,
                CHUNK_LENGTH,
                start_piece,
                sig_len,
            ),
            current_header: None,
        }
    }

    pub async fn push_bytes(&mut self, bytes: &[u8]) {
        let aligned = self.resync.push(bytes);
        let outputs = self.chunker.push(&aligned, &self.auth);
        self.store_outputs(outputs).await;
    }

    pub async fn finish(&mut self) {
        let outputs = self.chunker.flush(&self.auth);
        self.store_outputs(outputs).await;
        self.cursor.flush();
    }

    async fn store_outputs(&mut self, outputs: Vec<ace_wire::chunker::OutChunk>) {
        for out in outputs {
            let header = header_for_piece(&mut self.current_header, out.piece);
            self.store
                .lock()
                .await
                .put_chunk_with_header(out.piece, out.chunk, header, &out.data);
            // Track the live edge for ingest-resume continuity (throttle-persisted).
            self.cursor.advance_to(out.piece + 1);
        }
    }
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
    use super::*;
    use crate::broadcast::{CHUNK_LENGTH, PIECE_LENGTH};
    use ace_swarm::store::PieceStore;
    use ace_wire::live_auth::LiveSourceAuth;
    use std::sync::Arc;
    use tokio::sync::Mutex;

    fn ts_body(bytes: usize) -> Vec<u8> {
        const TS_PACKET_LEN: usize = 188;
        let packets = bytes.div_ceil(TS_PACKET_LEN);
        let mut body = Vec::with_capacity(packets * TS_PACKET_LEN);
        for i in 0..packets {
            body.push(0x47);
            body.extend(std::iter::repeat_n((i % 251) as u8, TS_PACKET_LEN - 1));
        }
        body
    }

    #[tokio::test]
    async fn shared_ingest_writes_signed_ts_chunks_to_store() {
        let store = Arc::new(Mutex::new(PieceStore::new(
            PIECE_LENGTH,
            CHUNK_LENGTH,
            4 << 20,
        )));
        let auth = Arc::new(LiveSourceAuth::generate());
        let cursor = crate::broadcast::BroadcastCursor::detached(0);
        let mut ingest = BroadcastIngest::new(store.clone(), auth, cursor);

        ingest
            .push_bytes(&ts_body(CHUNK_LENGTH as usize + 188))
            .await;
        ingest.finish().await;

        let guard = store.lock().await;
        assert!(
            guard.chunk(0, 0).is_some(),
            "shared ingest must write chunk (0, 0)"
        );
        let header = guard.piece_header(0).expect("piece header is recorded");
        assert_ne!(header, [0; 8], "source ingest must generate a live header");
    }

    #[tokio::test]
    async fn ingest_resumes_piece_numbering_from_the_cursor() {
        let store = Arc::new(Mutex::new(PieceStore::new(
            PIECE_LENGTH,
            CHUNK_LENGTH,
            8 << 20,
        )));
        let auth = Arc::new(LiveSourceAuth::generate());
        // A resumed broadcast whose cursor already sits at piece 42.
        let cursor = crate::broadcast::BroadcastCursor::detached(42);
        let mut ingest = BroadcastIngest::new(store.clone(), auth, cursor.clone());

        // Push a full piece worth of TS so the first piece is emitted complete.
        ingest.push_bytes(&ts_body(PIECE_LENGTH as usize)).await;
        ingest.finish().await;

        let guard = store.lock().await;
        assert!(
            guard.chunk(42, 0).is_some(),
            "first produced piece must be the cursor's start piece (42), not 0"
        );
        assert!(
            guard.chunk(0, 0).is_none(),
            "ingest must not restart numbering at piece 0"
        );
        drop(guard);
        assert!(
            cursor.start_piece() >= 43,
            "cursor advances past the pieces produced"
        );
    }
}
