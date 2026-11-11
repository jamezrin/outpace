use super::*;
use std::hint::black_box;

fn packet(pid: u16, pcr: u64, discontinuity: bool) -> [u8; TS_PACKET_LEN] {
    let mut bytes = [17; TS_PACKET_LEN];
    bytes[0] = 0x47;
    bytes[1] = (pid >> 8) as u8 & 0x1f;
    bytes[2] = pid as u8;
    bytes[3] = 0x30;
    bytes[4] = 7;
    bytes[5] = 0x10 | if discontinuity { 0x80 } else { 0 };
    bytes[6] = (pcr >> 25) as u8;
    bytes[7] = (pcr >> 17) as u8;
    bytes[8] = (pcr >> 9) as u8;
    bytes[9] = (pcr >> 1) as u8;
    bytes[10] = ((pcr & 1) << 7) as u8 | 0x7e;
    bytes
}

// Frozen pre-refactor byte-loop oracle; intentionally independent of the new scanner.
fn legacy(reservoir: &mut StartupReservoir, bytes: &[u8]) -> Option<usize> {
    let prior_bytes = reservoir.queued_bytes;
    let mut retain_from = None;
    for (index, &byte) in bytes.iter().enumerate() {
        reservoir.partial.push(byte);
        if reservoir.partial.len() != TS_PACKET_LEN {
            continue;
        }
        let mut packet = [0_u8; TS_PACKET_LEN];
        packet.copy_from_slice(&reservoir.partial);
        reservoir.partial.clear();
        let timing = ts_timing(&packet);
        if reservoir.clock.observe(timing) == ClockObservation::Reset {
            retain_from = Some(prior_bytes + index + 1 - TS_PACKET_LEN);
            // A clock reset discards media, never the absolute startup origin.
            reservoir.release_clock_duration = None;
            reservoir.release_bytes = 0;
            if !timing.discontinuity {
                let _ = reservoir.clock.observe(timing);
            }
        }
    }
    retain_from
}

fn reservoir() -> StartupReservoir {
    let mut value = StartupReservoir::new(
        StartupBufferConfig {
            target_ms: 1_000_000,
            max_bytes: 1024 * 1024,
            timeout_ms: 1_000_000,
        },
        None,
    );
    value.first_byte_at = Some(Instant::now());
    value.release_clock_duration = Some(Duration::from_secs(1));
    value.release_bytes = 73;
    value
}

fn apply(
    value: &mut StartupReservoir,
    bytes: &[u8],
    observe: fn(&mut StartupReservoir, &[u8]) -> Option<usize>,
) -> Option<usize> {
    let retained = observe(value, bytes);
    let mut chunk = Bytes::copy_from_slice(bytes);
    if let Some(offset) = retained {
        value.discard_prefix(offset, &mut chunk);
    }
    value.queued_bytes += chunk.len();
    value.queue.push_back(chunk);
    retained
}

#[test]
fn all_packet_partitions_preserve_intermediate_reset_state_and_retained_bytes() {
    let sequence = [
        packet(0x100, (1_u64 << 33) - 45_000, false),
        packet(0x100, 0, false),
        packet(0x200, 1, false),
        [0xff; TS_PACKET_LEN],
        packet(0x100, 45_000, true),
        packet(0x100, 90_000, false),
        packet(0x100, 1, false),
        packet(0x100, 45_001, false),
    ]
    .concat();
    let bytes = [sequence, packet(0x100, 90_001, false)[..73].to_vec()].concat();
    for split in 0..=bytes.len() {
        let mut expected = reservoir();
        let mut actual = reservoir();
        for chunk in [&bytes[..split], &[][..], &bytes[split..]] {
            assert_eq!(
                apply(&mut actual, chunk, StartupReservoir::observe_bytes),
                apply(&mut expected, chunk, legacy),
                "split {split}"
            );
            assert_eq!(actual.partial, expected.partial, "split {split}");
            assert!(actual.partial.len() < TS_PACKET_LEN);
            assert_eq!(actual.clock.elapsed(), expected.clock.elapsed());
            assert_eq!(actual.queued_bytes, expected.queued_bytes);
            assert_eq!(actual.queue, expected.queue);
            assert_eq!(
                actual.first_byte_at.is_none(),
                expected.first_byte_at.is_none()
            );
            assert_eq!(
                actual.release_clock_duration,
                expected.release_clock_duration
            );
            assert_eq!(actual.release_bytes, expected.release_bytes);
        }
    }
}

#[test]
#[ignore = "bounded synthetic startup scanner benchmark; run explicitly in release mode"]
fn startup_scan_measurement() {
    let bytes = packet(0x100, 90_000, false).repeat(50_000);
    for (name, width) in [("aligned", 188 * 64), ("split", 189)] {
        for trial in 0..=3 {
            let mut state = reservoir();
            let start = std::time::Instant::now();
            for chunk in bytes.chunks(width) {
                black_box(state.observe_bytes(black_box(chunk)));
                state.queued_bytes += chunk.len();
            }
            let seconds = start.elapsed().as_secs_f64();
            assert!(state.partial.is_empty());
            if trial != 0 {
                println!("{{\"case\":\"{name}\",\"trial\":{trial},\"bytes\":{},\"seconds\":{seconds:.9}}}",bytes.len());
            }
        }
    }
}
