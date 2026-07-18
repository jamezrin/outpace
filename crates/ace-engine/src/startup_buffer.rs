use crate::config::StartupBufferConfig;
use crate::provider::{SourceStats, TsSource};
use ace_media::mpegts::{ts_timing, ClockObservation, SelectedPcrClock, TS_PACKET_LEN};
use ace_swarm::types::StreamMetadata;
use async_trait::async_trait;
use bytes::Bytes;
use std::collections::VecDeque;
use std::time::Duration;
use tokio::time::Instant;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReleaseReason {
    TargetDuration,
    ByteLimit,
    Deadline,
    EndOfStream,
    Disabled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    Collecting,
    Draining(ReleaseReason),
    Released,
}

fn release_event(reason: ReleaseReason, duration: Duration, queued_bytes: usize) -> String {
    format!(
        "[prebuffer] startup buffer released: reason={reason:?} duration_ms={} queued_bytes={queued_bytes}",
        duration.as_millis()
    )
}

struct StartupReservoir {
    config: StartupBufferConfig,
    bitrate: Option<u64>,
    queue: VecDeque<Bytes>,
    queued_bytes: usize,
    clock: SelectedPcrClock,
    partial: Vec<u8>,
    first_clean_byte: Option<Instant>,
    release_clock_duration: Option<Duration>,
    release_bytes: usize,
    phase: Phase,
}

impl StartupReservoir {
    fn new(config: StartupBufferConfig, bitrate: Option<u64>) -> Self {
        Self {
            config,
            bitrate: bitrate.filter(|rate| *rate > 0),
            queue: VecDeque::new(),
            queued_bytes: 0,
            clock: SelectedPcrClock::new(),
            partial: Vec::with_capacity(TS_PACKET_LEN),
            first_clean_byte: None,
            release_clock_duration: None,
            release_bytes: 0,
            phase: if config.target_ms == 0 {
                Phase::Draining(ReleaseReason::Disabled)
            } else {
                Phase::Collecting
            },
        }
    }

    fn push(&mut self, mut chunk: Bytes) -> Option<ReleaseReason> {
        if self.phase != Phase::Collecting {
            return match self.phase {
                Phase::Draining(reason) => Some(reason),
                _ => None,
            };
        }
        if self.first_clean_byte.is_none() {
            self.first_clean_byte = Some(Instant::now());
        }

        if let Some(retain_from) = self.observe_bytes(&chunk) {
            self.discard_prefix(retain_from, &mut chunk);
        }
        if self.first_clean_byte.is_none() {
            self.first_clean_byte = Some(Instant::now());
        }
        let crosses_cap = self.queued_bytes.saturating_add(chunk.len()) > self.config.max_bytes;
        self.queued_bytes += chunk.len();
        self.queue.push_back(chunk);

        let reason = if crosses_cap {
            Some(ReleaseReason::ByteLimit)
        } else {
            self.ready_reason(Instant::now())
        };
        if let Some(reason) = reason {
            self.release(reason);
        }
        reason
    }

    /// Observe complete packets and return the combined queued/current byte offset of the last
    /// reset packet. The caller uses that boundary to retain the reset packet and its suffix.
    fn observe_bytes(&mut self, bytes: &[u8]) -> Option<usize> {
        let prior_bytes = self.queued_bytes;
        let mut retain_from = None;
        for (index, &byte) in bytes.iter().enumerate() {
            self.partial.push(byte);
            if self.partial.len() != TS_PACKET_LEN {
                continue;
            }
            let mut packet = [0_u8; TS_PACKET_LEN];
            packet.copy_from_slice(&self.partial);
            self.partial.clear();
            let timing = ts_timing(&packet);
            if self.clock.observe(timing) == ClockObservation::Reset {
                retain_from = Some(prior_bytes + index + 1 - TS_PACKET_LEN);
                self.first_clean_byte = None;
                self.release_clock_duration = None;
                self.release_bytes = 0;
                if !timing.discontinuity {
                    let _ = self.clock.observe(timing);
                }
            }
        }
        retain_from
    }

    fn discard_prefix(&mut self, retain_from: usize, chunk: &mut Bytes) {
        let prior_bytes = self.queued_bytes;
        if retain_from >= prior_bytes {
            self.queue.clear();
            self.queued_bytes = 0;
            *chunk = chunk.slice(retain_from - prior_bytes..);
            return;
        }

        let mut discard = retain_from;
        while discard > 0 {
            let front_len = self.queue.front().map_or(0, Bytes::len);
            if discard < front_len {
                let retained = self
                    .queue
                    .pop_front()
                    .expect("front length came from queue");
                self.queue.push_front(retained.slice(discard..));
                self.queued_bytes -= discard;
                break;
            }
            self.queue.pop_front();
            self.queued_bytes -= front_len;
            discard -= front_len;
        }
    }

    fn discontinuity(&mut self) {
        self.queue.clear();
        self.queued_bytes = 0;
        self.partial.clear();
        self.clock.reset();
        self.first_clean_byte = None;
    }

    fn duration(&self) -> Duration {
        let clock_duration = match (self.release_clock_duration, self.release_bytes) {
            (Some(duration), bytes) if bytes > 0 => {
                duration.mul_f64(self.queued_bytes as f64 / bytes as f64)
            }
            _ => self.clock.elapsed().unwrap_or_default(),
        };
        (clock_duration > Duration::ZERO)
            .then_some(clock_duration)
            .or_else(|| {
                self.bitrate.map(|rate| {
                    Duration::from_secs_f64(self.queued_bytes as f64 * 8.0 / rate as f64)
                })
            })
            .unwrap_or_default()
    }

    fn ready_reason(&self, now: Instant) -> Option<ReleaseReason> {
        match self.phase {
            Phase::Draining(reason) => Some(reason),
            Phase::Collecting if self.duration() >= self.config.target() => {
                Some(ReleaseReason::TargetDuration)
            }
            Phase::Collecting
                if self
                    .first_clean_byte
                    .is_some_and(|start| now.duration_since(start) >= self.config.timeout()) =>
            {
                Some(ReleaseReason::Deadline)
            }
            _ => None,
        }
    }

    fn deadline(&self) -> Option<Instant> {
        (self.phase == Phase::Collecting)
            .then(|| {
                self.first_clean_byte
                    .map(|start| start + self.config.timeout())
            })
            .flatten()
    }

    fn release(&mut self, reason: ReleaseReason) {
        if self.phase != Phase::Collecting {
            return;
        }
        if matches!(
            reason,
            ReleaseReason::ByteLimit | ReleaseReason::Deadline | ReleaseReason::EndOfStream
        ) {
            crate::alog!("[prebuffer] startup buffer released early: {:?}", reason);
        }
        crate::alog!(
            "{}",
            release_event(reason, self.duration(), self.queued_bytes)
        );
        self.release_clock_duration = self.clock.elapsed();
        self.release_bytes = self.queued_bytes;
        self.phase = Phase::Draining(reason);
    }

    fn pop_front(&mut self) -> Option<Bytes> {
        let chunk = self.queue.pop_front()?;
        self.queued_bytes -= chunk.len();
        Some(chunk)
    }

    fn finish_drain(&mut self) {
        if matches!(self.phase, Phase::Draining(_)) && self.queue.is_empty() {
            self.phase = Phase::Released;
        }
    }

    fn queued_bytes(&self) -> usize {
        self.queued_bytes
    }
}

pub struct StartupBufferedSource {
    inner: Box<dyn TsSource>,
    reservoir: StartupReservoir,
    pending_discontinuity: bool,
    released_chunk_discontinuity: bool,
}

impl StartupBufferedSource {
    #[allow(clippy::new_ret_no_self)] // The public decorator interface intentionally returns a trait object.
    pub fn new(
        inner: Box<dyn TsSource>,
        config: StartupBufferConfig,
        bitrate: Option<u64>,
    ) -> Box<dyn TsSource> {
        Box::new(Self {
            inner,
            reservoir: StartupReservoir::new(config, bitrate),
            pending_discontinuity: false,
            released_chunk_discontinuity: false,
        })
    }

    async fn collect_next(&mut self) {
        let next = if let Some(deadline) = self.reservoir.deadline() {
            tokio::select! { chunk = self.inner.next() => chunk, _ = tokio::time::sleep_until(deadline) => { self.reservoir.release(ReleaseReason::Deadline); return; } }
        } else {
            self.inner.next().await
        };

        match next {
            Some(chunk) => {
                if self.inner.take_discontinuity() {
                    self.reservoir.discontinuity();
                    self.pending_discontinuity = true;
                }
                self.reservoir.push(chunk);
            }
            None => self.reservoir.release(ReleaseReason::EndOfStream),
        }
    }
}

#[async_trait]
impl TsSource for StartupBufferedSource {
    async fn next(&mut self) -> Option<Bytes> {
        loop {
            match self.reservoir.phase {
                Phase::Collecting => self.collect_next().await,
                Phase::Draining(_) => {
                    if let Some(chunk) = self.reservoir.pop_front() {
                        self.released_chunk_discontinuity =
                            std::mem::take(&mut self.pending_discontinuity);
                        return Some(chunk);
                    }
                    self.reservoir.finish_drain();
                }
                Phase::Released => return self.inner.next().await,
            }
        }
    }

    fn take_discontinuity(&mut self) -> bool {
        if self.reservoir.phase == Phase::Released {
            self.inner.take_discontinuity()
        } else {
            std::mem::take(&mut self.released_chunk_discontinuity)
        }
    }

    fn stats(&self) -> SourceStats {
        let mut stats = self.inner.stats();
        if self.reservoir.queued_bytes() > 0 {
            stats.buffer_ms = self.reservoir.duration().as_millis().min(u64::MAX as u128) as u64;
        }
        stats
    }

    fn metadata(&self) -> StreamMetadata {
        self.inner.metadata()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::StartupBufferConfig;
    use crate::provider::{SourceStats, TsSource};
    use ace_swarm::types::StreamMetadata;
    use async_trait::async_trait;
    use bytes::Bytes;
    use std::collections::VecDeque;
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    };

    const PID: u16 = 0x100;

    fn config(target_ms: u64, max_bytes: usize, timeout_ms: u64) -> StartupBufferConfig {
        StartupBufferConfig {
            target_ms,
            max_bytes,
            timeout_ms,
        }
    }

    fn packet(pid: u16, pcr: Option<u64>, discontinuity: bool, marker: u8) -> Bytes {
        let mut p = [marker; 188];
        p[0] = 0x47;
        p[1] = (pid >> 8) as u8 & 0x1f;
        p[2] = pid as u8;
        p[3] = if pcr.is_some() || discontinuity {
            0x30
        } else {
            0x10
        };
        if let Some(pcr) = pcr {
            p[4] = 7;
            p[5] = 0x10 | if discontinuity { 0x80 } else { 0 };
            p[6] = (pcr >> 25) as u8;
            p[7] = (pcr >> 17) as u8;
            p[8] = (pcr >> 9) as u8;
            p[9] = (pcr >> 1) as u8;
            p[10] = ((pcr & 1) << 7) as u8 | 0x7e;
        } else if discontinuity {
            p[4] = 1;
            p[5] = 0x80;
        }
        Bytes::copy_from_slice(&p)
    }

    struct FixtureSource {
        chunks: VecDeque<(Bytes, bool)>,
        last_gap: bool,
        stats: SourceStats,
        metadata: StreamMetadata,
        delay_after: Option<usize>,
        reads: usize,
        dropped: Option<Arc<AtomicBool>>,
    }

    impl FixtureSource {
        fn new(chunks: Vec<(Bytes, bool)>) -> Self {
            Self {
                chunks: chunks.into(),
                last_gap: false,
                stats: SourceStats::default(),
                metadata: StreamMetadata::default(),
                delay_after: None,
                reads: 0,
                dropped: None,
            }
        }
    }

    impl Drop for FixtureSource {
        fn drop(&mut self) {
            if let Some(flag) = &self.dropped {
                flag.store(true, Ordering::SeqCst);
            }
        }
    }

    #[async_trait]
    impl TsSource for FixtureSource {
        async fn next(&mut self) -> Option<Bytes> {
            if self.delay_after == Some(self.reads) {
                std::future::pending().await
            };
            let (bytes, gap) = self.chunks.pop_front()?;
            self.reads += 1;
            self.last_gap = gap;
            Some(bytes)
        }
        fn take_discontinuity(&mut self) -> bool {
            std::mem::take(&mut self.last_gap)
        }
        fn stats(&self) -> SourceStats {
            self.stats.clone()
        }
        fn metadata(&self) -> StreamMetadata {
            self.metadata.clone()
        }
    }

    fn boxed(chunks: Vec<(Bytes, bool)>) -> Box<dyn TsSource> {
        Box::new(FixtureSource::new(chunks))
    }

    #[tokio::test]
    async fn withholds_until_target_then_releases_exact_bytes_in_order() {
        let source = boxed(vec![
            (packet(PID, Some(0), false, b'a'), false),
            (packet(PID, Some(90_000), false, b'b'), false),
            (packet(PID, Some(180_000), false, b'c'), false),
        ]);
        let mut buffered = StartupBufferedSource::new(source, config(2_000, 4096, 10_000), None);
        assert_eq!(buffered.next().await.unwrap()[20], b'a');
        assert_eq!(buffered.next().await.unwrap()[20], b'b');
        assert_eq!(buffered.next().await.unwrap()[20], b'c');
    }

    #[tokio::test]
    async fn startup_discontinuity_discards_pre_gap_but_runtime_gap_never_regates() {
        let source = boxed(vec![
            (packet(PID, Some(0), false, b'a'), false),
            (packet(PID, Some(45_000), false, b'b'), false),
            (packet(PID, Some(0), false, b'c'), true),
            (packet(PID, Some(90_000), false, b'd'), false),
            (packet(PID, Some(180_000), false, b'e'), false),
            (packet(PID, Some(270_000), false, b'f'), true),
        ]);
        let mut buffered = StartupBufferedSource::new(source, config(1_000, 4096, 10_000), None);
        assert_eq!(buffered.next().await.unwrap()[20], b'c');
        assert!(buffered.take_discontinuity());
        assert_eq!(buffered.next().await.unwrap()[20], b'd');
        assert_eq!(buffered.next().await.unwrap()[20], b'e');
        assert_eq!(buffered.next().await.unwrap()[20], b'f');
        assert!(buffered.take_discontinuity());
    }

    #[tokio::test]
    async fn in_chunk_clock_reset_discards_every_byte_before_reset_packet() {
        let pre_gap_a = packet(PID, Some(0), false, b'a');
        let pre_gap_b = packet(PID, Some(45_000), false, b'b');
        let reset = packet(PID, Some(90_000), true, b'c');
        let post_gap_a = packet(PID, Some(0), false, b'd');
        let post_gap_b = packet(PID, Some(90_000), false, b'e');
        let chunk = [
            pre_gap_a.as_ref(),
            pre_gap_b.as_ref(),
            reset.as_ref(),
            post_gap_a.as_ref(),
            post_gap_b.as_ref(),
        ]
        .concat();
        let expected = [reset.as_ref(), post_gap_a.as_ref(), post_gap_b.as_ref()].concat();
        let mut buffered = StartupBufferedSource::new(
            boxed(vec![(Bytes::from(chunk), false)]),
            config(1_000, 4096, 10_000),
            None,
        );

        assert_eq!(buffered.next().await.unwrap().as_ref(), expected);
    }

    #[tokio::test]
    async fn reset_packet_split_across_chunks_retains_its_prefix_not_pre_gap_bytes() {
        let pre_gap = packet(PID, Some(0), false, b'a');
        let reset = packet(PID, Some(45_000), true, b'b');
        let post_gap_a = packet(PID, Some(0), false, b'c');
        let post_gap_b = packet(PID, Some(90_000), false, b'd');
        let first = [pre_gap.as_ref(), &reset[..73]].concat();
        let second = [&reset[73..], post_gap_a.as_ref(), post_gap_b.as_ref()].concat();
        let mut buffered = StartupBufferedSource::new(
            boxed(vec![
                (Bytes::from(first), false),
                (Bytes::from(second), false),
            ]),
            config(1_000, 4096, 10_000),
            None,
        );

        assert_eq!(buffered.next().await.unwrap().as_ref(), &reset[..73]);
        assert_eq!(
            buffered.next().await.unwrap().as_ref(),
            [&reset[73..], post_gap_a.as_ref(), post_gap_b.as_ref()].concat()
        );
    }

    #[tokio::test]
    async fn zero_target_is_disabled_and_reads_only_one_chunk() {
        let mut buffered = StartupBufferedSource::new(
            boxed(vec![
                (packet(PID, Some(0), false, b'a'), false),
                (packet(PID, Some(90_000), false, b'b'), false),
            ]),
            config(0, 188, 1),
            None,
        );
        assert_eq!(buffered.next().await.unwrap()[20], b'a');
    }

    #[tokio::test]
    async fn pcr_wrap_and_other_pid_still_reach_target() {
        let modulus = 1_u64 << 33;
        let source = boxed(vec![
            (packet(PID, Some(modulus - 45_000), false, b'a'), false),
            (packet(PID + 1, Some(5_000_000), false, b'x'), false),
            (packet(PID, Some(45_000), false, b'b'), false),
        ]);
        let mut buffered = StartupBufferedSource::new(source, config(1_000, 4096, 10_000), None);
        assert_eq!(buffered.next().await.unwrap()[20], b'a');
        assert_eq!(buffered.next().await.unwrap()[20], b'x');
    }

    #[tokio::test]
    async fn backward_pcr_discards_prior_startup_data() {
        let source = boxed(vec![
            (packet(PID, Some(180_000), false, b'a'), false),
            (packet(PID, Some(90_000), false, b'b'), false),
            (packet(PID, Some(180_000), false, b'c'), false),
            (packet(PID, Some(270_000), false, b'd'), false),
        ]);
        let mut buffered = StartupBufferedSource::new(source, config(1_000, 4096, 10_000), None);
        assert_eq!(buffered.next().await.unwrap()[20], b'b');
    }

    #[tokio::test]
    async fn late_pcr_and_split_packets_are_timed_without_losing_chunks() {
        let p0 = packet(PID, Some(0), false, b'b');
        let p1 = packet(PID, Some(90_000), false, b'c');
        let source = boxed(vec![
            (packet(PID, None, false, b'a'), false),
            (p0.slice(..91), false),
            (p0.slice(91..), false),
            (p1, false),
        ]);
        let mut buffered = StartupBufferedSource::new(source, config(1_000, 4096, 10_000), None);
        assert_eq!(buffered.next().await.unwrap()[20], b'a');
        assert_eq!(buffered.next().await.unwrap().len(), 91);
        assert_eq!(buffered.next().await.unwrap().len(), 97);
    }

    #[tokio::test]
    async fn bitrate_fallback_sets_queued_duration_and_reaches_target() {
        let source = boxed(vec![
            (Bytes::from(vec![b'a'; 500]), false),
            (Bytes::from(vec![b'b'; 500]), false),
        ]);
        let mut buffered =
            StartupBufferedSource::new(source, config(1_000, 4096, 10_000), Some(8_000));
        let first = buffered.next().await.unwrap();
        assert_eq!(first[0], b'a');
        assert_eq!(buffered.stats().buffer_ms, 500);
    }

    #[tokio::test]
    async fn byte_cap_keeps_one_chunk_overhead_then_releases() {
        let source = boxed(vec![
            (Bytes::from(vec![b'a'; 150]), false),
            (Bytes::from(vec![b'b'; 100]), false),
        ]);
        let mut buffered = StartupBufferedSource::new(source, config(10_000, 200, 10_000), None);
        assert_eq!(buffered.next().await.unwrap().len(), 150);
        assert_eq!(buffered.next().await.unwrap().len(), 100);
    }

    #[tokio::test]
    async fn deadline_releases_while_inner_is_stalled() {
        let mut source = FixtureSource::new(vec![(packet(PID, None, false, b'a'), false)]);
        source.delay_after = Some(1);
        let mut buffered =
            StartupBufferedSource::new(Box::new(source), config(10_000, 4096, 10), None);
        assert_eq!(buffered.next().await.unwrap()[20], b'a');
    }

    #[tokio::test]
    async fn eof_drains_collected_chunks() {
        let mut buffered = StartupBufferedSource::new(
            boxed(vec![(packet(PID, None, false, b'a'), false)]),
            config(10_000, 4096, 10_000),
            None,
        );
        assert_eq!(buffered.next().await.unwrap()[20], b'a');
        assert!(buffered.next().await.is_none());
    }

    #[test]
    fn reservoir_never_exceeds_cap_plus_one_chunk() {
        let mut reservoir = StartupReservoir::new(config(10_000, 200, 10_000), None);
        assert!(reservoir.push(Bytes::from(vec![0; 150])).is_none());
        assert_eq!(
            reservoir.push(Bytes::from(vec![0; 100])),
            Some(ReleaseReason::ByteLimit)
        );
        assert_eq!(reservoir.queued_bytes(), 250);
        assert!(reservoir.push(Bytes::from(vec![0; 100])).is_some());
        assert_eq!(reservoir.queued_bytes(), 250);
    }

    #[test]
    fn release_event_reports_reason_duration_and_queued_bytes() {
        assert_eq!(
            release_event(ReleaseReason::TargetDuration, Duration::from_millis(20_125), 42_000),
            "[prebuffer] startup buffer released: reason=TargetDuration duration_ms=20125 queued_bytes=42000"
        );
    }

    #[tokio::test]
    async fn delegates_metadata_stats_and_drop() {
        let dropped = Arc::new(AtomicBool::new(false));
        let mut source = FixtureSource::new(vec![(Bytes::from_static(b"x"), false)]);
        source.stats = SourceStats {
            peers: 3,
            bitrate: 7,
            downloaded: 11,
            ..SourceStats::default()
        };
        source.dropped = Some(dropped.clone());
        let mut buffered =
            StartupBufferedSource::new(Box::new(source), config(10_000, 4096, 10_000), Some(8));
        assert_eq!(buffered.stats().peers, 3);
        assert_eq!(buffered.metadata(), StreamMetadata::default());
        assert_eq!(buffered.next().await.unwrap(), Bytes::from_static(b"x"));
        drop(buffered);
        assert!(dropped.load(Ordering::SeqCst));
    }
}
