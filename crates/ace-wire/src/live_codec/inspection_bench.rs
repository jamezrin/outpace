//! Bounded synthetic inspect-and-borrow caller benchmark, not network throughput.
use super::*;
use std::hint::black_box;
use std::time::Instant;

type Inspection = fn(&PeerMessage);

fn owned(message: &PeerMessage) {
    for _ in 0..100_000 {
        let chunk = LiveChunk::from_message(black_box(message)).unwrap();
        black_box((
            chunk.piece,
            chunk.chunk,
            chunk.piece_header,
            chunk.data.as_slice(),
        ));
    }
}

fn borrowed(message: &PeerMessage) {
    for _ in 0..100_000 {
        let chunk = LiveChunk::borrow_from_message(black_box(message)).unwrap();
        black_box((chunk.piece, chunk.chunk, chunk.piece_header, chunk.data));
    }
}

#[test]
#[ignore = "bounded synthetic live chunk inspection benchmark; run explicitly in release mode"]
fn live_chunk_inspection_measurement() {
    for size in [64, 16 * 1024] {
        let message = build_piece(5, 7, 3, [4; 8], &vec![17; size]);
        for trial in 0..=3 {
            let modes: [(&str, Inspection); 2] = if trial % 2 == 0 {
                [("owned", owned), ("borrowed", borrowed)]
            } else {
                [("borrowed", borrowed), ("owned", owned)]
            };
            for (mode, run) in modes {
                let start = Instant::now();
                run(&message);
                let seconds = start.elapsed().as_secs_f64();
                if trial != 0 {
                    println!("{{\"mode\":\"{mode}\",\"payload_bytes\":{size},\"iterations\":100000,\"trial\":{trial},\"seconds\":{seconds:.9}}}");
                }
            }
        }
    }
}
