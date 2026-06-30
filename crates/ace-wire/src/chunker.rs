//! Splits a contiguous live byte stream into transport-geometry units — the inverse of
//! [`crate::reassembly::PieceReassembler`]. Pure: feed bytes with [`TsChunker::push`], drain the
//! full `chunk_length` chunks it returns; the final partial chunk is only emitted by [`TsChunker::flush`].
//! Chunk-then-reassemble is the identity.

/// One emitted chunk, addressed the way the live piece protocol expects.
#[derive(Debug, PartialEq, Eq)]
pub struct OutChunk {
    pub piece: u64,
    pub chunk: u16,
    /// Byte offset within the piece (`0..piece_length`) — the `begin` a reassembler consumes.
    pub begin: u64,
    pub data: Vec<u8>,
}

/// Splits a contiguous live byte stream into `chunk_length`-sized blocks addressed by
/// ascending `(piece, chunk, begin)` from `start_piece`. Pure logic; pairs with
/// [`crate::reassembly::PieceReassembler`] on the download side.
pub struct TsChunker {
    piece_length: u64,
    chunk_length: u64,
    start_piece: u64,
    buf: Vec<u8>,
    abs: u64, // absolute offset (from the epoch) of the NEXT byte to be emitted (i.e. of buf[0])
}

impl TsChunker {
    /// `piece_length` must be a positive multiple of `chunk_length`, and `chunk_length` > 0.
    pub fn new(piece_length: u64, chunk_length: u64, start_piece: u64) -> Self {
        TsChunker { piece_length, chunk_length, start_piece, buf: Vec::new(), abs: 0 }
    }

    /// Append `bytes`; return every full `chunk_length` chunk now available, in order.
    pub fn push(&mut self, bytes: &[u8]) -> Vec<OutChunk> {
        self.buf.extend_from_slice(bytes);
        let mut out = Vec::new();
        while self.buf.len() as u64 >= self.chunk_length {
            let data: Vec<u8> = self.buf.drain(..self.chunk_length as usize).collect();
            out.push(self.make_chunk(data));
        }
        out
    }

    /// Emit any remaining buffered bytes as a final (possibly short) chunk. Returns `None` if
    /// nothing is buffered.
    pub fn flush(&mut self) -> Option<OutChunk> {
        if self.buf.is_empty() {
            return None;
        }
        let data = std::mem::take(&mut self.buf);
        Some(self.make_chunk(data))
    }

    fn make_chunk(&mut self, data: Vec<u8>) -> OutChunk {
        let abs = self.abs;
        self.abs += data.len() as u64;
        OutChunk {
            piece: self.start_piece + abs / self.piece_length,
            chunk: ((abs % self.piece_length) / self.chunk_length) as u16,
            begin: abs % self.piece_length,
            data,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reassembly::PieceReassembler;

    #[test]
    fn chunks_are_chunk_length_sized_with_ascending_indices() {
        let mut c = TsChunker::new(8, 4, 0);
        let input: Vec<u8> = (0..12).collect();
        let chunks = c.push(&input);
        assert_eq!(chunks.len(), 3);

        assert_eq!(chunks[0].piece, 0);
        assert_eq!(chunks[0].chunk, 0);
        assert_eq!(chunks[0].begin, 0);
        assert_eq!(chunks[0].data, vec![0, 1, 2, 3]);

        assert_eq!(chunks[1].piece, 0);
        assert_eq!(chunks[1].chunk, 1);
        assert_eq!(chunks[1].begin, 4);
        assert_eq!(chunks[1].data, vec![4, 5, 6, 7]);

        assert_eq!(chunks[2].piece, 1);
        assert_eq!(chunks[2].chunk, 0);
        assert_eq!(chunks[2].begin, 0);
        assert_eq!(chunks[2].data, vec![8, 9, 10, 11]);
    }

    #[test]
    fn roundtrip_through_reassembler_is_identity() {
        // 24 bytes = exactly 3 pieces of piece_length 8 (covers >2 pieces), so every piece
        // completes and `flush()` has nothing left over — a true identity round-trip.
        let input: Vec<u8> = (0..24u8).collect();
        let mut c = TsChunker::new(8, 4, 0);
        let chunks = c.push(&input);
        assert!(c.flush().is_none(), "input is a multiple of chunk_length; nothing to flush");

        let mut r = PieceReassembler::new(8, 0);
        for chunk in &chunks {
            r.add_block(chunk.piece, chunk.begin, &chunk.data).unwrap();
        }

        assert_eq!(r.take_ready(), input);
    }

    #[test]
    fn flush_emits_trailing_partial_chunk() {
        let mut c = TsChunker::new(8, 4, 0);
        let full = c.push(&[1, 2, 3, 4, 5]);
        assert_eq!(full.len(), 1);
        assert_eq!(full[0].data, vec![1, 2, 3, 4]);

        let tail = c.flush().expect("partial chunk");
        assert_eq!(tail.data, vec![5]);
        assert_eq!(tail.piece, 0);
        assert_eq!(tail.begin, 4);
        assert_eq!(tail.chunk, 1);

        assert!(c.flush().is_none());
    }

    #[test]
    fn empty_push_emits_nothing() {
        let mut c = TsChunker::new(8, 4, 0);
        assert_eq!(c.push(&[]), Vec::new());
        assert!(c.flush().is_none());
    }

    #[test]
    fn start_piece_offsets_indices() {
        let mut c = TsChunker::new(8, 4, 100);
        let chunks = c.push(&[1, 2, 3, 4]);
        assert_eq!(chunks[0].piece, 100);
    }
}
