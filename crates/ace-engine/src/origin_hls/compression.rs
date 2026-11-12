//! Bounded decoding for HTTP-encoded playlists, never media or byte ranges.

use super::OriginError;
use bytes::BytesMut;
use flate2::bufread::GzDecoder;
use std::io::Read;

const MAX_MEMBERS: usize = 16;
const DECODE_CHUNK: usize = 16 * 1024;

pub(super) fn decode_gzip(
    mut input: &[u8],
    limit: usize,
    mut check: impl FnMut() -> Result<(), OriginError>,
) -> Result<BytesMut, OriginError> {
    let mut output = BytesMut::new();
    let mut chunk = [0; DECODE_CHUNK];
    for _ in 0..MAX_MEMBERS {
        check()?;
        let previous_len = input.len();
        let mut decoder = GzDecoder::new(input);
        loop {
            check()?;
            // A nonempty probe at the exact cap validates the footer/CRC and
            // detects one extra decoded byte instead of mistaking it for EOF.
            let available = limit.saturating_sub(output.len());
            let count = chunk.len().min(available.saturating_add(1));
            let read = decoder
                .read(&mut chunk[..count])
                .map_err(|_| OriginError::InvalidPlaylist)?;
            check()?;
            if read == 0 {
                break;
            }
            if read > available {
                return Err(OriginError::TooLarge);
            }
            output.extend_from_slice(&chunk[..read]);
        }
        input = decoder.into_inner();
        if input.len() >= previous_len {
            return Err(OriginError::InvalidPlaylist);
        }
        if input.is_empty() {
            return Ok(output);
        }
        // Each member is parsed separately: MultiGzDecoder can process an
        // unbounded chain of empty members within a single read/checkpoint.
    }
    Err(OriginError::TooLarge)
}

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::{write::GzEncoder, Compression};
    use std::io::Write;

    pub(crate) fn gzip(bytes: &[u8]) -> Vec<u8> {
        let mut encoder = GzEncoder::new(Vec::new(), Compression::fast());
        encoder.write_all(bytes).unwrap();
        encoder.finish().unwrap()
    }

    #[test]
    fn exact_limit_still_validates_footer_and_rejects_extra_output() {
        let text = vec![b'x'; DECODE_CHUNK * 2];
        let encoded = gzip(&text);
        assert_eq!(decode_gzip(&encoded, text.len(), || Ok(())).unwrap(), text);
        assert_eq!(
            decode_gzip(&encoded, text.len() - 1, || Ok(())),
            Err(OriginError::TooLarge)
        );
        for offset in [8, 4] {
            let mut corrupt = encoded.clone();
            let index = corrupt.len() - offset;
            corrupt[index] ^= 1;
            assert_eq!(
                decode_gzip(&corrupt, text.len(), || Ok(())),
                Err(OriginError::InvalidPlaylist)
            );
        }
        for missing in 1..=8 {
            assert_eq!(
                decode_gzip(&encoded[..encoded.len() - missing], text.len(), || Ok(())),
                Err(OriginError::InvalidPlaylist)
            );
        }
    }

    #[test]
    fn concatenated_members_are_bounded_and_all_trailing_bytes_validated() {
        let first = gzip(b"#EXTM3U\n");
        let second = gzip(b"#EXT-X-ENDLIST\n");
        let joined = [first.clone(), second.clone()].concat();
        assert_eq!(
            &decode_gzip(&joined, 100, || Ok(())).unwrap()[..],
            b"#EXTM3U\n#EXT-X-ENDLIST\n"
        );
        assert_eq!(
            decode_gzip(&joined, 8, || Ok(())),
            Err(OriginError::TooLarge)
        );
        for tail in [&b"junk"[..], &second[..second.len() - 1]] {
            let invalid = [first.as_slice(), tail].concat();
            assert_eq!(
                decode_gzip(&invalid, 100, || Ok(())),
                Err(OriginError::InvalidPlaylist)
            );
        }
        let empty = gzip(b"");
        assert!(decode_gzip(&empty.repeat(MAX_MEMBERS), 0, || Ok(()))
            .unwrap()
            .is_empty());
        assert_eq!(
            decode_gzip(&empty.repeat(MAX_MEMBERS + 1), 0, || Ok(())),
            Err(OriginError::TooLarge)
        );
        assert_eq!(
            decode_gzip(b"", 100, || Ok(())),
            Err(OriginError::InvalidPlaylist)
        );
    }

    #[test]
    fn cancellation_and_deadline_are_checked_during_decoding() {
        let encoded = gzip(&vec![b'x'; DECODE_CHUNK * 4]);
        let empty_chain = gzip(b"").repeat(MAX_MEMBERS);
        for encoded in [encoded, empty_chain] {
            for error in [OriginError::Cancelled, OriginError::Timeout] {
                let mut calls = 0;
                assert_eq!(
                    decode_gzip(&encoded, DECODE_CHUNK * 4, || {
                        calls += 1;
                        if calls == 5 {
                            Err(error)
                        } else {
                            Ok(())
                        }
                    }),
                    Err(error)
                );
                assert_eq!(calls, 5);
            }
        }
    }
}
