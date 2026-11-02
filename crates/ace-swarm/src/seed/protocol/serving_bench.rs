//! Explicit synthetic prepare_reply benchmark, excluding wire encoding/network I/O.
use super::*;
use std::hint::black_box;
use std::time::Instant;

const CHUNK: usize = 16 * 1024;

async fn fixture() -> Arc<Mutex<PieceStore>> {
    let store = Arc::new(Mutex::new(PieceStore::new(
        (CHUNK * 2) as u64,
        CHUNK as u64,
        (CHUNK * 2) as u64,
    )));
    PieceStore::shared_put_complete_piece(&store, 7, [9; 8], &vec![17; CHUNK * 2])
        .await
        .unwrap();
    store
}

#[tokio::test]
#[ignore = "bounded synthetic serving benchmark; run explicitly in release mode"]
async fn shared_serving_measurement() {
    let request = PeerMessage::Unknown {
        id: 6,
        payload: [
            3_u32.to_be_bytes().as_slice(),
            7_u32.to_be_bytes().as_slice(),
            &[0, 0],
        ]
        .concat(),
    };
    for (profile_name, profile) in [
        (
            "inbound",
            ServeProfile::Inbound {
                fallback_header: [8; 8],
            },
        ),
        ("reciprocal", ServeProfile::Reciprocal),
    ] {
        for cold in [false, true] {
            let count = if cold { 1_000 } else { 10_000 };
            for trial in 0..=3 {
                // Construction/publication is outside the timer. A cold store is
                // served once; a warm store is promoted by one untimed reply.
                let mut stores = Vec::with_capacity(if cold { count } else { 1 });
                for _ in 0..if cold { count } else { 1 } {
                    stores.push(fixture().await);
                }
                if !cold {
                    black_box(
                        prepare_reply(&request, Some(&stores[0]), profile)
                            .await
                            .unwrap(),
                    );
                }
                let start = Instant::now();
                for i in 0..count {
                    let store = &stores[if cold { i } else { 0 }];
                    let reply = prepare_reply(black_box(&request), Some(store), profile)
                        .await
                        .unwrap();
                    assert_eq!(reply.media_bytes, CHUNK);
                    black_box(reply);
                }
                let seconds = start.elapsed().as_secs_f64();
                if trial != 0 {
                    println!("{{\"profile\":\"{profile_name}\",\"cold\":{cold},\"trial\":{trial},\"requests\":{count},\"payload_bytes\":{CHUNK},\"seconds\":{seconds:.9}}}");
                }
            }
        }
    }
}
