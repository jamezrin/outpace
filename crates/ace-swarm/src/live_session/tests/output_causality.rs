use super::*;
const TS_PACKET_LEN: usize = 188;
const TS_SYNC: u8 = 0x47;
fn ts(pid: u16, pusi: bool, rai: bool, payload: &[u8]) -> Vec<u8> {
    let mut p = vec![0xFFu8; TS_PACKET_LEN];
    p[0] = TS_SYNC;
    p[1] = ((pid >> 8) as u8 & 0x1F) | if pusi { 0x40 } else { 0 };
    p[2] = (pid & 0xFF) as u8;
    let payload_start = if rai {
        // adaptation field (1 byte length + 1 flags byte) + payload, AFC = 11
        p[3] = 0x30;
        p[4] = 1; // adaptation_field_length = just the flags byte
        p[5] = 0x40; // random_access_indicator
        6
    } else {
        p[3] = 0x10; // AFC = 01, payload only
        4
    };
    let n = payload.len().min(TS_PACKET_LEN - payload_start);
    p[payload_start..payload_start + n].copy_from_slice(&payload[..n]);
    p
}
fn psi(pid: u16, section: &[u8]) -> Vec<u8> {
    let mut payload = vec![0u8]; // pointer_field = 0
    payload.extend_from_slice(section);
    ts(pid, true, false, &payload)
}
fn pat(pmt_pid: u16) -> Vec<u8> {
    // section after table_id: tsid(2) + flags(1) + sec#(1) + last#(1) + 1 program(4) + crc(4)
    let section_length = 5 + 4 + 4;
    let mut s = vec![0x00u8]; // table_id = PAT
    s.push(0xB0 | ((section_length >> 8) as u8 & 0x0F));
    s.push((section_length & 0xFF) as u8);
    s.extend_from_slice(&[0x00, 0x01]); // transport_stream_id
    s.push(0xC1); // version 0, current
    s.push(0x00); // section_number
    s.push(0x00); // last_section_number
    s.extend_from_slice(&[0x00, 0x01]); // program_number = 1
    s.extend_from_slice(&[0xE0 | ((pmt_pid >> 8) as u8 & 0x1F), (pmt_pid & 0xFF) as u8]);
    s.extend_from_slice(&[0, 0, 0, 0]); // CRC32 placeholder
    psi(0, &s)
}
fn pmt(pmt_pid: u16, video_pid: u16) -> Vec<u8> {
    pmt_codec(pmt_pid, video_pid, 0x1B) // H.264
}
fn pmt_codec(pmt_pid: u16, video_pid: u16, stream_type: u8) -> Vec<u8> {
    // after table_id: prog#(2)+flags(1)+sec#(1)+last#(1)+pcrpid(2)+pinfolen(2)+stream(5)+crc(4)
    let section_length = 5 + 4 + 5 + 4;
    let mut s = vec![0x02u8]; // table_id = PMT
    s.push(0xB0 | ((section_length >> 8) as u8 & 0x0F));
    s.push((section_length & 0xFF) as u8);
    s.extend_from_slice(&[0x00, 0x01]); // program_number
    s.push(0xC1); // version 0, current
    s.push(0x00); // section_number
    s.push(0x00); // last_section_number
    s.extend_from_slice(&[
        0xE0 | ((video_pid >> 8) as u8 & 0x1F),
        (video_pid & 0xFF) as u8,
    ]); // PCR_PID = video
    s.extend_from_slice(&[0xF0, 0x00]); // program_info_length = 0
    s.push(stream_type);
    s.extend_from_slice(&[
        0xE0 | ((video_pid >> 8) as u8 & 0x1F),
        (video_pid & 0xFF) as u8,
    ]);
    s.extend_from_slice(&[0xF0, 0x00]); // ES_info_length = 0
    s.extend_from_slice(&[0, 0, 0, 0]); // CRC32 placeholder
    psi(pmt_pid, &s)
}
fn pes_nal(nal: &[u8]) -> Vec<u8> {
    let mut pes = vec![0x00, 0x00, 0x01, 0xE0, 0x00, 0x00, 0x80, 0x00, 0x00];
    pes.extend_from_slice(&[0x00, 0x00, 0x01]);
    pes.extend_from_slice(nal);
    pes
}

fn fixture() -> Vec<u8> {
    [
        pat(0x100),
        pmt(0x100, 0x101),
        ts(0x11, true, false, &[0; 30]),
        ts(0x101, true, false, &pes_nal(&[0x67, 1])),
        ts(0x101, true, false, &pes_nal(&[0x65, 2])),
        ts(0x101, false, false, &[8; 30]),
    ]
    .concat()
}

fn continuity() -> Continuity {
    let mut c = Continuity::fresh(&info(), 0, 0, 0, default_live_recovery()).0;
    c.output_gate = ace_media::mpegts::KeyframeGate::new();
    c
}

fn output(parts: &[&[u8]]) -> (Vec<u8>, Vec<usize>) {
    let mut c = continuity();
    c.output_gate =
        ace_media::mpegts::KeyframeGate::with_service_name(Some("Synthetic proof".into()));
    let mut bytes = Vec::new();
    let mut markers = Vec::new();
    for part in parts {
        for segment in c.resync_output(part) {
            if segment.discontinuity {
                markers.push(bytes.len());
            }
            bytes.extend_from_slice(&segment.bytes);
        }
    }
    (bytes, markers)
}

#[test]
fn titled_media_multiple_loss_partition_bytes_and_marker_offsets() {
    let run = fixture();
    let input = [
        run.clone(),
        vec![0x23; 801],
        run.clone(),
        vec![0x24; 397],
        run.clone(),
        vec![0x25; 401],
    ]
    .concat();
    let baseline = output(&[&input, &run]);
    assert!(!baseline.0.is_empty());
    assert!(!baseline.1.is_empty());
    let mut cuts = Vec::new();
    for base in (0..input.len()).step_by(188) {
        for delta in [0, 1, 3, 4, 5, 6, 9, 13, 14, 15, 16, 187] {
            if base + delta <= input.len() {
                cuts.push(base + delta);
            }
        }
    }
    for split in cuts {
        assert_eq!(
            output(&[&input[..split], &input[split..], &run]),
            baseline,
            "partition at {split}"
        );
    }
    // Repeated tiny junk fragments, with the same suffix and no terminal flush.
    let parts: Vec<_> = input
        .chunks(17)
        .chain(std::iter::once(run.as_slice()))
        .collect();
    assert_eq!(output(&parts), baseline);
}

fn token(port: u16) -> Arc<OutputCredit> {
    Arc::new(OutputCredit {
        producer: SocketAddrV4::new([127, 0, 0, 1].into(), port),
        kind: CandidateKind::Discovered,
        credited: std::sync::atomic::AtomicBool::new(false),
    })
}

#[test]
fn boundary_packet_provenance_tracks_both_sources_without_crediting_preparation() {
    let run = fixture();
    let mut c = continuity();
    let a = token(10001);
    let b = token(10002);
    let split = 4 * 188 + 15; // Inside access packet PES/NAL.
    let mut segments = c.resync_output_credited(&run[..split], Some(a.clone()));
    segments.extend(c.resync_output_credited(&run[split..], Some(b.clone())));
    assert!(!segments.is_empty());
    let credits: Vec<_> = c.pending_credit.iter().flatten().collect();
    assert!(credits.iter().any(|t| Arc::ptr_eq(t, &a)));
    assert!(credits.iter().any(|t| Arc::ptr_eq(t, &b)));
    assert!(!a.credited.load(Ordering::Relaxed));
    assert!(!b.credited.load(Ordering::Relaxed));
}

#[tokio::test]
async fn causal_enqueue_credit_survives_cancel_between_segments_and_repeated_tokens() {
    let info = info();
    let seed = SeedConfig {
        metrics: None,
        registry: SeedRegistry::new(),
        store_factory: test_store_factory(4096, None),
        enabled: true,
        prefetch_pieces: 0,
        live_recovery: default_live_recovery(),
        warm_peers: WarmPeerCache::memory(),
    };
    let store = Arc::new(tokio::sync::Mutex::new(PieceStore::new(4, 2, 4096)));
    let mut c = continuity();
    let mut candidates = SessionCandidates::default();
    let downloaded = Arc::new(AtomicU64::new(0));
    let a = token(10001);
    let b = token(10002);
    candidates.learn(a.producer, a.kind);
    candidates.learn(b.producer, b.kind);
    candidates.explored(a.producer);
    candidates.explored(b.producer);
    let run = fixture();
    let split = 4 * 188 + 15;
    let first = c.resync_output_credited(&run[..split], Some(a.clone()));
    assert!(
        first.is_empty(),
        "parameter-only prefix must remain withheld"
    );
    assert!(c.pending_credit.is_empty());
    assert_eq!(candidates.explorations(a.producer), 1);
    assert_eq!(candidates.explorations(b.producer), 1);
    let segments = c.resync_output_credited(&run[split..], Some(b.clone()));
    assert_eq!(segments.len(), 1);
    c.pending_output.extend(segments);
    // A later packet from the same token remains causal, but cannot earn new credit.
    let later = c.resync_output_credited(&run, Some(b.clone()));
    c.pending_output.extend(later);
    assert!(c.pending_output.len() >= 2);
    let expected: u64 = c.pending_output.iter().map(|s| s.bytes.len() as u64).sum();
    let (tx, mut rx) = mpsc::channel(1);
    {
        let future = crate::live_session::pool::actor::drain_retained(
            &mut c,
            &mut candidates,
            &info,
            &seed,
            &store,
            &downloaded,
            &tx,
        );
        tokio::pin!(future);
        std::future::poll_fn(|cx| {
            assert!(future.as_mut().poll(cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
    }
    assert!(a.credited.load(Ordering::Relaxed));
    assert!(b.credited.load(Ordering::Relaxed));
    assert!(!c.pending_output.is_empty());
    assert_eq!(candidates.explorations(a.producer), 0);
    assert_eq!(candidates.explorations(b.producer), 0);
    // Observe the real candidate side effect, not just the retained credit bit.
    // Repeated references must not erase exploration accrued after the first commit.
    candidates.explored(a.producer);
    candidates.explored(b.producer);
    let first = rx.recv().await.unwrap().bytes.len() as u64;
    assert_eq!(downloaded.load(Ordering::Relaxed), first);
    assert!(
        crate::live_session::pool::actor::drain_retained(
            &mut c,
            &mut candidates,
            &info,
            &seed,
            &store,
            &downloaded,
            &tx,
        )
        .await
    );
    assert_eq!(downloaded.load(Ordering::Relaxed), expected);
    assert_eq!(c.emitted, expected);
    assert!(a.credited.load(Ordering::Relaxed));
    assert!(b.credited.load(Ordering::Relaxed));
    assert!(c.pending_credit.is_empty());
}

#[test]
fn withheld_and_no_producer_inputs_cannot_borrow_triggering_producer() {
    let mut c = continuity();
    let withheld = token(10003);
    let prefix = [
        pat(0x100),
        pmt(0x100, 0x101),
        ts(0x101, true, false, &pes_nal(&[0x67, 1])),
    ]
    .concat();
    assert!(c
        .resync_output_credited(&prefix, Some(withheld.clone()))
        .is_empty());
    let access = [
        ts(0x101, true, false, &pes_nal(&[0x65, 2])),
        ts(0x101, false, false, &[8; 30]),
    ]
    .concat();
    let segments = c.resync_output_credited(&access, None);
    assert!(!segments.is_empty()); // Includes generated cached tables.
    assert!(c.pending_credit.iter().all(Vec::is_empty));
    assert!(!withheld.credited.load(Ordering::Relaxed));
}
