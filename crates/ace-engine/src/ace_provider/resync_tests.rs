use super::*;
use ace_media::mpegts::{ts_pid, TS_PACKET_LEN};
const PMT: u16 = 0x100;
const VIDEO: u16 = 0x101;
fn packet(pid: u16, marker: u8, rai: bool) -> Vec<u8> {
    let mut p = vec![marker; TS_PACKET_LEN];
    p[..4].copy_from_slice(&[0x47, ((pid >> 8) as u8 & 0x1f) | 0x40, pid as u8, 0x10]);
    if rai {
        p[3] = 0x30;
        p[4] = 1;
        p[5] = 0x40;
    }
    p
}

fn psi(pid: u16, section: &[u8]) -> Vec<u8> {
    let mut p = packet(pid, 0xff, false);
    p[4] = 0;
    p[5..5 + section.len()].copy_from_slice(section);
    p
}

fn pat(pmt: u16) -> Vec<u8> {
    psi(
        0,
        &[
            0,
            0xb0,
            13,
            0,
            1,
            0xc1,
            0,
            0,
            0,
            1,
            0xe0 | (pmt >> 8) as u8,
            pmt as u8,
            0,
            0,
            0,
            0,
        ],
    )
}

fn pmt(pmt: u16, video: u16, codec: u8) -> Vec<u8> {
    psi(
        pmt,
        &[
            2,
            0xb0,
            18,
            0,
            1,
            0xc1,
            0,
            0,
            0xe0 | (video >> 8) as u8,
            video as u8,
            0xf0,
            0,
            codec,
            0xe0 | (video >> 8) as u8,
            video as u8,
            0xf0,
            0,
            0,
            0,
            0,
            0,
        ],
    )
}

fn markers(bytes: &[u8]) -> Vec<u8> {
    bytes
        .as_chunks::<TS_PACKET_LEN>()
        .0
        .iter()
        .filter(|p| ts_pid(p.as_slice()) == VIDEO && p[3] & 0x10 != 0)
        .map(|p| p[20])
        .collect()
}

async fn buffered_provider(config: StartupBufferConfig) -> Box<dyn TsSource> {
    let (tx, rx) = mpsc::channel(8);
    // Feed the engine adapter the authenticated output covered by swarm resync tests.
    for chunk in [
        LiveOutput {
            bytes: Bytes::from(
                [pat(PMT), pmt(PMT, VIDEO, 0x1b), packet(VIDEO, b'a', true)].concat(),
            ),
            discontinuity: false,
        },
        LiveOutput {
            bytes: Bytes::from(
                [
                    packet(VIDEO, b'b', false),
                    packet(VIDEO, b'c', false),
                    packet(VIDEO, b'd', false),
                ]
                .concat(),
            ),
            discontinuity: false,
        },
        LiveOutput {
            bytes: Bytes::from(
                [pat(PMT), pmt(PMT, VIDEO, 0x1b), packet(VIDEO, b'r', true)].concat(),
            ),
            discontinuity: true,
        },
    ] {
        tx.send(chunk).await.unwrap();
    }
    drop(tx);
    let source = Box::new(AceSource {
        metrics: None,
        _sampler: None,
        rx,
        discontinuity: false,
        peers: Arc::new(AtomicU32::new(0)),
        downloaded: Arc::new(AtomicU64::new(0)),
        uploaded: Arc::new(AtomicU64::new(0)),
        peers_served: Arc::new(AtomicU32::new(0)),
        metadata: StreamMetadata::default(),
    });
    StartupBufferedSource::new(source, config, Some(188))
}

#[tokio::test]
async fn released_startup_source_emits_provider_prefix_before_gap() {
    let mut source = buffered_provider(StartupBufferConfig {
        target_ms: 1,
        max_bytes: 4096,
        timeout_ms: 10_000,
    })
    .await;
    assert_eq!(markers(&source.next().await.unwrap()), b"a");
    assert!(!source.take_discontinuity());
    assert_eq!(markers(&source.next().await.unwrap()), b"bcd");
    assert!(!source.take_discontinuity());
    assert_eq!(markers(&source.next().await.unwrap()), b"r");
    assert!(source.take_discontinuity());
    assert!(source.next().await.is_none());
}

#[tokio::test]
async fn collecting_startup_source_retains_existing_gap_discard_policy() {
    let mut source = buffered_provider(StartupBufferConfig {
        target_ms: 10_000,
        max_bytes: 4096,
        timeout_ms: 10_000,
    })
    .await;
    assert_eq!(
        markers(&source.next().await.unwrap()),
        b"r",
        "collecting startup still discards its pre-gap reservoir"
    );
    assert!(source.take_discontinuity());
    assert!(source.next().await.is_none());
}
