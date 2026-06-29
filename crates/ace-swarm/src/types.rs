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
}

impl StreamInfo {
    /// Number of chunks per piece (`piece_length / chunk_length`).
    pub fn chunks_per_piece(&self) -> u16 {
        (self.piece_length / self.chunk_length) as u16
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
        };
        assert_eq!(si.chunks_per_piece(), 64);
    }
}
