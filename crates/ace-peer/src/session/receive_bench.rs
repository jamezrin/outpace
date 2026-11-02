//! Explicit, bounded synthetic benchmark; not a network or production latency test.
use super::*;
use std::hint::black_box;
use std::time::Instant;

struct Input {
    bytes: Vec<u8>,
    offset: usize,
    chunk: usize,
    reads: usize,
    max_requested: usize,
}

impl AsyncRead for Input {
    fn poll_read(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        target: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        this.reads += 1;
        this.max_requested = this.max_requested.max(target.remaining());
        let n = (this.bytes.len() - this.offset)
            .min(this.chunk)
            .min(target.remaining());
        target.put_slice(&this.bytes[this.offset..this.offset + n]);
        this.offset += n;
        Poll::Ready(Ok(()))
    }
}

impl AsyncWrite for Input {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        b: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Poll::Ready(Ok(b.len()))
    }
    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

#[tokio::test]
#[ignore = "bounded synthetic CPU benchmark; run explicitly in release mode"]
async fn receive_buffer_measurement() {
    for (case, frames, payload, chunk) in [
        ("coalesced", 100_000, 20, 16 * 1024),
        ("fragmented", 20_000, 20, 3),
        // Unknown messages with piece-sized payloads isolate owned-payload copying;
        // this does not benchmark the Piece codec or media processing.
        ("pieces", 2_000, 16 * 1024, 16 * 1024),
    ] {
        let frame = PeerMessage::Unknown {
            id: 250,
            payload: vec![17; payload],
        }
        .encode();
        let bytes = frame.repeat(frames);
        for trial in 0..=3 {
            let mut session = PeerSession::new(Input {
                bytes: bytes.clone(),
                offset: 0,
                chunk,
                reads: 0,
                max_requested: 0,
            });
            let start = Instant::now();
            for _ in 0..frames {
                black_box(session.read_message().await.unwrap());
            }
            let seconds = start.elapsed().as_secs_f64();
            assert_eq!(session.stream.offset, bytes.len());
            assert!(session.stream.max_requested <= 16 * 1024);
            if trial != 0 {
                println!(
                    "{{\"case\":\"{case}\",\"trial\":{trial},\"frames\":{frames},\"bytes\":{},\"seconds\":{seconds:.9},\"reads\":{},\"max_requested\":{},\"peak_visible_capacity\":{}}}",
                    bytes.len(), session.stream.reads, session.stream.max_requested,
                    session.receive_peak_capacity
                );
            }
        }
    }
}
