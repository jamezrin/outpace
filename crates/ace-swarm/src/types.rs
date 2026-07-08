//! Stream descriptors shared across resolution and download.

/// What a stream needs to be downloaded, from the transport file (or known directly).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamInfo {
    /// 20-byte BitTorrent infohash of the live stream.
    pub infohash: [u8; 20],
    /// Bytes per piece (e.g. 1_048_576).
    pub piece_length: u64,
    /// Bytes per chunk (e.g. 16_384).
    pub chunk_length: u64,
    /// Tracker URLs from the transport file (UDP `udp://host:port` entries used).
    pub trackers: Vec<String>,
    /// Per-piece live-source signature length in bytes (the RSA modulus's byte length). Each
    /// wire piece carries this many trailing signature bytes that are NOT media and must be
    /// stripped before the MPEG-TS layer (B0/note 27). `0` means unsigned pieces.
    pub sig_len: usize,
    /// The broadcast source's DER SubjectPublicKeyInfo — the transport descriptor's `pubkey`
    /// field — used to *verify* each piece's in-band RSA signature before its bytes are served
    /// (issue #10, B0/note 27). Only a resolved transport descriptor carries this; a bare
    /// infohash has no source key, so this is empty and pieces are stripped but not verified.
    pub source_pubkey: Vec<u8>,
}

/// Default per-piece live-source signature length: Acestream source nodes generate 768-bit
/// RSA keys (note 25), so each piece's signature is 96 trailing bytes. Used when we only have
/// a bare infohash and no transport `pubkey` to measure the real modulus from.
pub const DEFAULT_SIG_LEN: usize = 96;

impl StreamInfo {
    /// Number of chunks per piece (`piece_length / chunk_length`).
    pub fn chunks_per_piece(&self) -> u16 {
        (self.piece_length / self.chunk_length) as u16
    }
}

/// What a single-file VOD stream needs to be downloaded and verified, from its transport
/// descriptor. Unlike [`StreamInfo`] (live, in-band RSA signatures), VOD integrity is the
/// transport's SHA-1 `pieces` list — standard BitTorrent piece hashing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VodInfo {
    /// 20-byte BitTorrent infohash of the VOD swarm.
    pub infohash: [u8; 20],
    /// Bytes per piece (e.g. 131_072).
    pub piece_length: u64,
    /// Bytes per chunk / request block (e.g. 16_384).
    pub chunk_length: u64,
    /// Tracker URLs from the transport file.
    pub trackers: Vec<String>,
    /// Per-piece SHA-1 hashes (20 bytes each) from the transport `pieces` key.
    pub piece_hashes: Vec<[u8; 20]>,
    /// Total content length in bytes (the final piece is truncated to this).
    pub total_length: u64,
}

impl VodInfo {
    /// Chunks (request blocks) per full piece (`piece_length / chunk_length`).
    pub fn chunks_per_piece(&self) -> u16 {
        (self.piece_length / self.chunk_length) as u16
    }
    /// Number of pieces (`== piece_hashes.len()`).
    pub fn piece_count(&self) -> u64 {
        self.piece_hashes.len() as u64
    }
    /// Byte length of piece `index` (the last piece is truncated to `total_length`).
    pub fn piece_size(&self, index: u64) -> u64 {
        let start = index * self.piece_length;
        self.total_length
            .saturating_sub(start)
            .min(self.piece_length)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunks_per_piece_is_64_for_1mib_pieces() {
        let si = StreamInfo {
            infohash: [0; 20],
            piece_length: 1_048_576,
            chunk_length: 16_384,
            trackers: vec![],
            sig_len: DEFAULT_SIG_LEN,
            source_pubkey: vec![],
        };
        assert_eq!(si.chunks_per_piece(), 64);
    }
}
