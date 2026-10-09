//! Synthetic transport regressions use the real provider output path.
use super::*;
use ace_media::mpegts::{ts_pid, TS_PACKET_LEN};

const PMT: u16 = 0x100;
const VIDEO: u16 = 0x101;

fn continuity() -> Continuity {
    let info = StreamInfo {
        infohash: [0; 20],
        piece_length: 4,
        chunk_length: 2,
        trackers: vec![],
        metadata: Default::default(),
        sig_len: 0,
        source_pubkey: vec![],
    };
    Continuity::fresh(
        &info,
        100,
        149,
        PREFETCH_PIECES,
        LiveRecoveryConfig::default(),
    )
    .0
}

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

fn output_bytes(output: Vec<LiveOutput>) -> Vec<u8> {
    output
        .into_iter()
        .flat_map(|chunk| chunk.bytes.to_vec())
        .collect()
}

fn prime(c: &mut Continuity) {
    let opening = [
        pat(PMT),
        pmt(PMT, VIDEO, 0x1b),
        packet(VIDEO, b'a', true),
        packet(VIDEO, b'b', false),
    ]
    .concat();
    assert_eq!(markers(&output_bytes(c.resync_output(&opening))), b"a");
}

#[test]
fn resync_handles_multiple_losses_in_one_batch_in_order() {
    let mut c = continuity();
    prime(&mut c);
    let mixed = [
        packet(VIDEO, b'c', false),
        vec![0; 8],
        packet(VIDEO, b'x', false),
        packet(VIDEO, b'r', true),
        packet(VIDEO, b'e', false),
        vec![0; 12],
        packet(VIDEO, b'z', false),
        packet(VIDEO, b't', true),
        packet(VIDEO, b'u', false),
    ]
    .concat();
    let chunks = c.resync_output(&mixed);
    assert_eq!(chunks.len(), 3);
    assert_eq!(markers(&chunks[0].bytes), b"bc");
    assert!(!chunks[0].discontinuity);
    assert_eq!(markers(&chunks[1].bytes), b"re");
    assert!(chunks[1].discontinuity);
    assert_eq!(markers(&chunks[2].bytes), b"t");
    assert!(chunks[2].discontinuity);
}

#[test]
fn resync_pending_losses_keep_cache_and_accumulate_withholding() {
    let mut c = continuity();
    prime(&mut c);
    let first = [
        vec![0; 8],
        packet(VIDEO, b'x', false),
        packet(VIDEO, b'y', false),
        packet(VIDEO, b'w', false),
    ]
    .concat();
    let prefix = c.resync_output(&first);
    assert_eq!(markers(&prefix[0].bytes), b"b");
    assert!(!prefix[0].discontinuity);
    let pending = c.transport_recovery.as_ref().unwrap();
    assert_eq!((pending.discarded_bytes, pending.withheld_bytes), (8, 376));
    let second = [
        vec![0; 12],
        packet(VIDEO, b'z', false),
        packet(VIDEO, b't', true),
        packet(VIDEO, b'u', false),
    ]
    .concat();
    let resumed = c.resync_output(&second);
    assert_eq!(resumed.len(), 1);
    assert_eq!(markers(&resumed[0].bytes), b"t");
    assert!(resumed[0].discontinuity);
    assert!(c.transport_recovery.is_none());
    // The operational resume event must report 20 discarded and 752 withheld bytes,
    // loss_events=2. This focused test's --nocapture output also verifies that accounting.
}

#[test]
fn resync_split_pushes_preserve_prefix_and_first_access_point() {
    let mixed = [
        packet(VIDEO, b'c', false),
        packet(VIDEO, b'd', false),
        vec![0; 8],
        packet(VIDEO, b'x', false),
        packet(VIDEO, b'r', true),
        packet(VIDEO, b's', false),
        packet(VIDEO, b't', false),
    ]
    .concat();
    for split in 0..=mixed.len() {
        let mut c = continuity();
        prime(&mut c);
        let mut chunks = c.resync_output(&mixed[..split]);
        chunks.extend(c.resync_output(&mixed[split..]));
        assert_eq!(
            chunks.iter().filter(|o| o.discontinuity).count(),
            1,
            "split={split}"
        );
        for chunk in &chunks {
            if chunk.discontinuity {
                assert_eq!(markers(&chunk.bytes)[0], b'r', "split={split}");
            }
        }
        assert_eq!(markers(&output_bytes(chunks)), b"bcdrs", "split={split}");
    }
}

#[test]
fn resync_tail_loss_arms_only_after_its_valid_prefix() {
    let mut c = continuity();
    prime(&mut c);
    let mixed = [packet(VIDEO, b'c', false), vec![0; 1024]].concat();
    let prefix = c.resync_output(&mixed);
    assert_eq!(markers(&output_bytes(prefix)), b"bc");
    assert!(c.output_gate_armed());
    let resumed = c.resync_output(
        &[
            packet(VIDEO, b'x', false),
            packet(VIDEO, b'r', true),
            packet(VIDEO, b's', false),
        ]
        .concat(),
    );
    assert_eq!(resumed.len(), 1);
    assert!(resumed[0].discontinuity);
    assert_eq!(markers(&resumed[0].bytes), b"r");
}

async fn buffered_provider(config: StartupBufferConfig) -> Box<dyn TsSource> {
    let mut c = continuity();
    let opening = [
        pat(PMT),
        pmt(PMT, VIDEO, 0x1b),
        packet(VIDEO, b'a', true),
        packet(VIDEO, b'b', false),
    ]
    .concat();
    let mixed = [
        packet(VIDEO, b'c', false),
        packet(VIDEO, b'd', false),
        vec![0; 8],
        packet(VIDEO, b'x', false),
        packet(VIDEO, b'r', true),
        packet(VIDEO, b's', false),
    ]
    .concat();
    let (tx, rx) = mpsc::channel(8);
    for chunk in c
        .resync_output(&opening)
        .into_iter()
        .chain(c.resync_output(&mixed))
    {
        tx.send(chunk).await.unwrap();
    }
    drop(tx);
    let source = Box::new(AceSource {
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

#[test]
fn resync_preserves_every_pre_loss_packet_in_mixed_batch() {
    let mut c = continuity();
    prime(&mut c);
    let mixed = [
        packet(VIDEO, b'c', false),
        packet(VIDEO, b'd', false),
        vec![0; 8],
        pat(PMT),
        pmt(PMT, VIDEO, 0x1b),
        packet(VIDEO, b'x', false),
        packet(VIDEO, b'r', true),
        packet(VIDEO, b's', false),
    ]
    .concat();
    assert_eq!(
        markers(&output_bytes(c.resync_output(&mixed))),
        b"bcdr",
        "complete pre-loss packets must precede first resumed RAI"
    );
}

#[test]
fn resync_uses_cached_psi_at_first_post_loss_rai() {
    let mut c = continuity();
    prime(&mut c);
    let mixed = [
        vec![0; 8],
        packet(VIDEO, b'x', false),
        packet(VIDEO, b'r', true),
        packet(VIDEO, b's', false),
    ]
    .concat();
    assert_eq!(
        markers(&output_bytes(c.resync_output(&mixed))),
        b"br",
        "no new PAT/PMT is needed to resume"
    );
}

#[test]
fn resync_synthetic_vector_keeps_prefix_and_resumes_without_new_psi() {
    let vector = include_bytes!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../tests/vectors/media/transport-resync.ts"
    ));
    let mut c = continuity();
    let output = c.resync_output(vector);
    assert_eq!(output.len(), 2);
    assert_eq!(markers(&output[0].bytes), b"abcd");
    assert!(!output[0].discontinuity);
    assert_eq!(markers(&output[1].bytes), b"rs");
    assert!(output[1].discontinuity);
}
