//! Shared live-TS source adapter for the existing normalization worker.

use super::*;
use crate::audio_normalize::output::{TsControl, TsOutput, TsReceiver};
use crate::audio_normalize::PlaybackStateHandle;
use crate::provider::{SourceStats, StatsHandle, StatsReader, TsSource};
use async_trait::async_trait;
use bytes::Bytes;
use std::sync::Mutex;

struct TsTarget(TsControl);
impl WorkerTarget for TsTarget {
    type Output = TsOutput;
    fn status(&self) -> HlsStatus {
        process::ProcessOutput::state(&self.0.output(1))
    }
    fn begin_generation(&self, generation: GenerationId, _deadline: Instant, _recovery: bool) {
        self.0.begin(generation.0);
    }
    fn fail_generation(&self, generation: GenerationId, reason: HlsFailure) {
        process::ProcessOutput::fail(&self.0.output(generation.0), reason);
    }
    fn child_output(&self, generation: GenerationId) -> Self::Output {
        self.0.output(generation.0)
    }
    fn flush_pending(&self, generation: GenerationId) {
        process::ProcessOutput::flush_pending(&self.0.output(generation.0));
    }
    async fn feed_initial(
        &self,
        generation: GenerationId,
        packets: &[[u8; 188]],
        cancel: &mut watch::Receiver<bool>,
    ) -> Result<(), HlsFailure> {
        let mut batch = process::AcceptedBatch {
            packets: [[0; 188]; 3],
            len: packets.len(),
        };
        if packets.len() != 3 {
            return Err(HlsFailure::OutputInvalid);
        }
        batch.packets.copy_from_slice(packets);
        self.publish(generation, batch, true, cancel).await
    }
    async fn feed_packet(
        &self,
        generation: GenerationId,
        packet: &[u8; 188],
        cancel: &mut watch::Receiver<bool>,
    ) -> Result<(), HlsFailure> {
        let mut batch = process::AcceptedBatch {
            packets: [[0; 188]; 3],
            len: 1,
        };
        batch.packets[0] = *packet;
        self.publish(generation, batch, false, cancel).await
    }
}
impl TsTarget {
    async fn publish(
        &self,
        generation: GenerationId,
        batch: process::AcceptedBatch,
        flush: bool,
        cancel: &mut watch::Receiver<bool>,
    ) -> Result<(), HlsFailure> {
        use process::ProcessOutput;
        let output = self.0.output(generation.0);
        let permit = tokio::select! { biased;
            _ = process::cancelled(cancel) => return Ok(()),
            permit = output.reserve_batch(&batch) => permit?,
        };
        if *cancel.borrow() {
            return Ok(());
        }
        output.publish(permit, batch);
        if flush {
            output.flush_pending();
        }
        Ok(())
    }
}
struct CachedStats(Arc<Mutex<SourceStats>>);
impl StatsReader for CachedStats {
    fn snapshot(&self) -> SourceStats {
        self.0.lock().unwrap().clone()
    }
}
struct LiveSource {
    control: Option<WorkerControl>,
    completed: watch::Receiver<bool>,
    receiver: TsReceiver,
    state: PlaybackStateHandle,
    stats: StatsHandle,
    metadata: ace_swarm::types::StreamMetadata,
    gap: bool,
}
#[async_trait]
impl TsSource for LiveSource {
    async fn next(&mut self) -> Option<Bytes> {
        let (bytes, gap) = self.receiver.next().await?;
        self.gap = gap;
        Some(bytes)
    }
    async fn shutdown(&mut self) {
        drop(self.control.take());
        assert!(
            crate::manager::wait_cleanup(&mut self.completed).await,
            "normalization source cleanup unconfirmed"
        );
    }
    fn take_discontinuity(&mut self) -> bool {
        std::mem::take(&mut self.gap)
    }
    fn stats(&self) -> SourceStats {
        self.stats.snapshot()
    }
    fn stats_handle(&self) -> Option<StatsHandle> {
        Some(self.stats.clone())
    }
    fn metadata(&self) -> ace_swarm::types::StreamMetadata {
        self.metadata.clone()
    }
    fn chunk_generation(&self) -> Option<u64> {
        self.receiver.delivered_generation()
    }
    fn playback_state(&self) -> Option<PlaybackStateHandle> {
        Some(self.state.clone())
    }
}

/// Called only after fixed capability preflight has succeeded. The worker owns
/// upstream admission until child reap AND source shutdown have both completed.
pub(crate) fn spawn(
    source: Box<dyn TsSource>,
    prepared: PreparedNormalizer,
    timeout: Duration,
    tracker: &CleanupTracker,
) -> Box<dyn TsSource> {
    let metadata = source.metadata();
    let cached = Arc::new(Mutex::new(source.stats()));
    let stats = source
        .stats_handle()
        .unwrap_or_else(|| Arc::new(CachedStats(cached.clone())));
    let (output, receiver) = TsControl::new();
    let state = output.state();
    let failed_sources = tracker.failed_sources();
    let packet_source = PacketSource::from_reader(PacketReader::Direct {
        source: Some(source),
        stats: cached,
    });
    let deadline = Instant::now() + timeout;
    let mut control = tracker.spawn(move |cancel| async move {
        let mut worker = EnabledWorker {
            media: TsTarget(output),
            prepared,
            source: packet_source,
            recovery: RecoveryWindow::new(),
            config: HlsConfig {
                startup_timeout_ms: timeout.as_millis().min(u128::from(u64::MAX)) as u64,
                ..HlsConfig::default()
            },
            deadline,
            seed: None,
            budget: InputBudget::new(),
        };
        let result = AssertUnwindSafe(worker.run(cancel))
            .catch_unwind()
            .await
            .unwrap_or(Err(HlsFailure::Pump));
        if let Err(reason) = result {
            worker
                .media
                .fail_generation(worker.recovery.generation, reason);
        }
        let PacketReader::Direct { source, .. } = &mut worker.source.rx else {
            unreachable!()
        };
        let source = source.take().unwrap();
        if let Err(panic) = crate::manager::shutdown_source(source, &failed_sources).await {
            worker
                .media
                .fail_generation(worker.recovery.generation, HlsFailure::Pump);
            std::panic::resume_unwind(panic);
        }
        // Dropping the target publishes stopped, but cannot overwrite failure.
    });
    let completed = control.completion_receiver();
    control.transfer();
    Box::new(LiveSource {
        control: Some(control),
        completed,
        receiver,
        state,
        stats,
        metadata,
        gap: false,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::audio_normalize::{Failure, PlaybackPhase};
    use crate::provider::{SourceStats, TsSource};
    use async_trait::async_trait;
    use bytes::Bytes;
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    };
    use tokio::time::{timeout, Duration};

    struct Fixture {
        bytes: Option<Bytes>,
        dropped: Arc<AtomicBool>,
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::SeqCst);
        }
    }
    #[async_trait]
    impl TsSource for Fixture {
        async fn next(&mut self) -> Option<Bytes> {
            if let Some(bytes) = self.bytes.take() {
                Some(bytes)
            } else {
                std::future::pending().await
            }
        }
        fn stats(&self) -> SourceStats {
            SourceStats {
                peers: 7,
                downloaded: 1234,
                ..Default::default()
            }
        }
        fn metadata(&self) -> ace_swarm::types::StreamMetadata {
            ace_swarm::types::StreamMetadata {
                title: Some("Synthetic audio fixture".into()),
                ..Default::default()
            }
        }
    }
    struct BackingOwner {
        bytes: Vec<u8>,
        dropped: Arc<AtomicBool>,
    }
    impl AsRef<[u8]> for BackingOwner {
        fn as_ref(&self) -> &[u8] {
            &self.bytes
        }
    }
    impl Drop for BackingOwner {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::SeqCst);
        }
    }
    struct DropWitness {
        inner: Fixture,
        counters: process::test_support::Counters,
        observed: Arc<Mutex<Option<process::test_support::Snapshot>>>,
    }
    impl Drop for DropWitness {
        fn drop(&mut self) {
            *self.observed.lock().unwrap() = Some(self.counters.snapshot());
        }
    }
    #[async_trait]
    impl TsSource for DropWitness {
        async fn next(&mut self) -> Option<Bytes> {
            self.inner.next().await
        }
        fn stats(&self) -> SourceStats {
            self.inner.stats()
        }
    }
    struct Channel {
        rx: mpsc::Receiver<(Bytes, bool)>,
        gap: bool,
    }
    #[async_trait]
    impl TsSource for Channel {
        async fn next(&mut self) -> Option<Bytes> {
            let (bytes, gap) = self.rx.recv().await?;
            self.gap = gap;
            Some(bytes)
        }
        fn take_discontinuity(&mut self) -> bool {
            std::mem::take(&mut self.gap)
        }
        fn stats(&self) -> SourceStats {
            SourceStats::default()
        }
    }
    fn ac3_input() -> Vec<u8> {
        use crate::hls_audio::process::test_fixtures::{
            pat_section, pmt_section, psi_packet, video_access_packet,
        };
        let mut pmt = pmt_section(9, 513, 512, 0x1b, 513, 0x06);
        pmt.truncate(pmt.len() - 4);
        pmt[2] += 3;
        pmt[21] = 3;
        pmt.extend_from_slice(&[0x6a, 1, 0]);
        let crc = ace_media::mpegts::mpeg_crc32(&pmt);
        pmt.extend_from_slice(&crc.to_be_bytes());
        [
            psi_packet(0, 0, &pat_section(7, 9, 100)),
            psi_packet(100, 2, &pmt),
            video_access_packet(512),
        ]
        .concat()
    }

    #[tokio::test]
    async fn continuously_ready_native_packet_chunks_do_not_force_per_packet_output_messages() {
        struct Packets {
            data: Vec<u8>,
            offset: usize,
        }
        #[async_trait]
        impl TsSource for Packets {
            async fn next(&mut self) -> Option<Bytes> {
                if self.offset == self.data.len() {
                    return None;
                }
                let end = self.offset + 188;
                let bytes = Bytes::copy_from_slice(&self.data[self.offset..end]);
                self.offset = end;
                Some(bytes)
            }
            fn stats(&self) -> SourceStats {
                SourceStats::default()
            }
        }
        let mut expected = process::test_fixtures::transport_prefix();
        for _ in 0..199 {
            expected.extend_from_slice(&process::test_fixtures::video_access_packet(256));
        }
        let tracker = crate::manager::CleanupTracker::default();
        let mut output = spawn(
            Box::new(Packets {
                data: expected.clone(),
                offset: 0,
            }),
            PreparedNormalizer {
                executable: "/not/an/executable".into(),
            },
            Duration::from_secs(5),
            &tracker,
        );
        let collected = timeout(Duration::from_secs(1), async {
            let mut bytes = Vec::new();
            let mut chunks = 0;
            while let Some(chunk) = output.next().await {
                bytes.extend_from_slice(&chunk);
                chunks += 1;
            }
            (bytes, chunks)
        })
        .await;
        output.shutdown().await;
        assert!(tracker.drain().await);
        let (bytes, chunks) = collected.expect("bounded native output");
        assert!(
            bytes == expected,
            "native output differs: observed {} bytes, expected {}, first mismatch {:?}",
            bytes.len(),
            expected.len(),
            bytes.iter().zip(&expected).position(|(a, b)| a != b)
        );
        assert_eq!(
            chunks, 3,
            "opening plus coalesced continuation, independent of provider fragmentation"
        );
    }

    #[tokio::test]
    async fn transformed_replacement_reaps_the_old_generation_and_emits_one_new_boundary() {
        use crate::hls_audio::process::test_support::{
            fake_prepared_child, process_test_lock, ChildMode,
        };
        let _serial = process_test_lock().await;
        let tracker = crate::manager::CleanupTracker::default();
        let admission = crate::admission::Admission::new(1);
        let (tx, rx) = mpsc::channel(2);
        let (prepared, counters) = fake_prepared_child(ChildMode::ReadyThenStall);
        let source = Box::new(crate::admission::Source {
            inner: Box::new(Channel { rx, gap: false }),
            _reservation: admission.reserve().unwrap(),
        });
        let mut output = spawn(source, prepared, Duration::from_secs(5), &tracker);
        let mut state = output.playback_state().unwrap();
        tx.send((ac3_input().into(), false)).await.unwrap();
        let first = timeout(Duration::from_secs(3), output.next()).await;
        let first_gap = output.take_discontinuity();
        tx.send((ac3_input().into(), true)).await.unwrap();
        let recovered = timeout(Duration::from_secs(3), async {
            loop {
                let snapshot = state.snapshot();
                if (snapshot.generation == 2 && snapshot.phase == PlaybackPhase::Ready)
                    || matches!(
                        snapshot.phase,
                        PlaybackPhase::Failed(_) | PlaybackPhase::Stopped
                    )
                {
                    break snapshot;
                }
                if state.changed().await.is_err() {
                    break state.snapshot();
                }
            }
        })
        .await;
        let replacement = timeout(Duration::from_secs(1), output.next()).await;
        let replacement_gap = output.take_discontinuity();
        let before_cleanup = counters.snapshot();
        drop(output);
        let drained = timeout(Duration::from_secs(3), tracker.drain()).await;
        assert_eq!(drained, Ok(true));
        assert!(matches!(first, Ok(Some(_))));
        assert!(!first_gap);
        let snapshot = recovered.expect("replacement readiness bound");
        assert_eq!(snapshot.generation, 2);
        assert_eq!(snapshot.phase, PlaybackPhase::Ready);
        assert!(matches!(replacement, Ok(Some(_))));
        assert!(replacement_gap);
        assert_eq!(
            (
                before_cleanup.children_started,
                before_cleanup.children_reaped
            ),
            (2, 1)
        );
        assert!(
            before_cleanup.max_task_count <= 4,
            "generations must not overlap child or pump ownership"
        );
        let cleanup = counters.snapshot();
        assert_eq!((cleanup.children_started, cleanup.children_reaped), (2, 2));
        assert_eq!(cleanup.pumps_started, cleanup.pumps_joined);
        assert_eq!(admission.snapshot().0, 0);
    }

    #[tokio::test]
    async fn dropping_source_during_observed_output_capacity_wait_reaps_before_admission_release() {
        use crate::hls_audio::process::test_support::{
            fake_prepared_child, process_test_lock, ChildMode,
        };
        let _serial = process_test_lock().await;
        let tracker = crate::manager::CleanupTracker::default();
        let admission = crate::admission::Admission::new(1);
        let dropped = Arc::new(AtomicBool::new(false));
        let (prepared, counters) = fake_prepared_child(ChildMode::ValidOutputFlood);
        let observed = Arc::new(Mutex::new(None));
        let backing_dropped = Arc::new(AtomicBool::new(false));
        let input = ac3_input();
        let mut backing = vec![0; 16 * 1024 * 1024];
        backing[..input.len()].copy_from_slice(&input);
        let view = Bytes::from_owner(BackingOwner {
            bytes: backing,
            dropped: backing_dropped.clone(),
        })
        .slice(..input.len());
        let source = Box::new(crate::admission::Source {
            inner: Box::new(DropWitness {
                inner: Fixture {
                    bytes: Some(view),
                    dropped: dropped.clone(),
                },
                counters: counters.clone(),
                observed: observed.clone(),
            }),
            _reservation: admission.reserve().unwrap(),
        });
        let output = spawn(source, prepared, Duration::from_secs(5), &tracker);
        let blocked = timeout(Duration::from_secs(3), async {
            while counters.snapshot().capacity_waits == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await;
        let held = admission.snapshot().0;
        let backing_released_before_wait = backing_dropped.load(Ordering::SeqCst);
        drop(output);
        let cleanup = timeout(Duration::from_secs(3), tracker.drain()).await;
        assert!(
            blocked.is_ok(),
            "actual stdout reservation must have returned Pending"
        );
        assert_eq!(held, 1);
        assert!(backing_released_before_wait,"tiny source view must detach and release its oversized owner before output backpressure");
        assert_eq!(cleanup, Ok(true));
        let counts = counters.snapshot();
        assert_eq!((counts.children_started, counts.children_reaped), (1, 1));
        assert_eq!(counts.pumps_started, counts.pumps_joined);
        assert_eq!(counts.active_tasks, 0);
        let at_drop = observed
            .lock()
            .unwrap()
            .take()
            .expect("source drop observed");
        assert_eq!(
            (at_drop.children_started, at_drop.children_reaped),
            (1, 1),
            "upstream resource dropped only after child reap"
        );
        assert_eq!(
            at_drop.pumps_started, at_drop.pumps_joined,
            "upstream resource dropped only after pump joins"
        );
        assert!(dropped.load(Ordering::SeqCst));
        assert_eq!(admission.snapshot().0, 0);
    }

    #[tokio::test]
    async fn direct_source_gap_discards_partial_packet_and_publishes_one_replacement_boundary() {
        let tracker = crate::manager::CleanupTracker::default();
        let (tx, rx) = mpsc::channel(2);
        let mut output = spawn(
            Box::new(Channel { rx, gap: false }),
            PreparedNormalizer {
                executable: "/not/an/executable".into(),
            },
            Duration::from_secs(5),
            &tracker,
        );
        let bytes = crate::hls_audio::process::test_fixtures::transport_prefix();
        tx.send((bytes.clone().into(), false)).await.unwrap();
        let first = timeout(Duration::from_secs(1), output.next())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(first.as_ref(), bytes.as_slice());
        assert!(!output.take_discontinuity());
        tx.send((vec![0x47; 17].into(), false)).await.unwrap();
        tx.send((bytes.clone().into(), true)).await.unwrap();
        let replacement = timeout(Duration::from_secs(1), output.next()).await;
        let gap = output.take_discontinuity();
        let snapshot = output.playback_state().unwrap().snapshot();
        tx.send((bytes.clone().into(), false)).await.unwrap();
        let continuation = timeout(Duration::from_secs(1), output.next()).await;
        let continuation_gap = output.take_discontinuity();
        output.shutdown().await;
        assert!(tracker.drain().await);
        assert_eq!(replacement.unwrap().unwrap().as_ref(), bytes.as_slice());
        assert!(gap);
        assert_eq!(snapshot.generation, 2);
        assert_eq!(snapshot.phase, PlaybackPhase::Ready);
        assert_eq!(continuation.unwrap().unwrap().as_ref(), bytes.as_slice());
        assert!(!continuation_gap);
    }

    #[tokio::test]
    async fn panicking_upstream_shutdown_keeps_admission_owned_and_cleanup_unconfirmed() {
        struct PanicShutdown(Fixture);
        #[async_trait]
        impl TsSource for PanicShutdown {
            async fn next(&mut self) -> Option<Bytes> {
                self.0.next().await
            }
            async fn shutdown(&mut self) {
                panic!("injected source cleanup fault");
            }
            fn stats(&self) -> SourceStats {
                self.0.stats()
            }
        }
        let tracker = crate::manager::CleanupTracker::default();
        let admission = crate::admission::Admission::new(1);
        let dropped = Arc::new(AtomicBool::new(false));
        let source = Box::new(crate::admission::Source {
            inner: Box::new(PanicShutdown(Fixture {
                bytes: Some(vec![0xff; 188].into()),
                dropped: dropped.clone(),
            })),
            _reservation: admission.reserve().unwrap(),
        });
        let mut output = spawn(
            source,
            PreparedNormalizer {
                executable: "/not/an/executable".into(),
            },
            Duration::from_secs(5),
            &tracker,
        );
        assert!(timeout(Duration::from_secs(1), output.next())
            .await
            .unwrap()
            .is_none());
        assert!(AssertUnwindSafe(output.shutdown())
            .catch_unwind()
            .await
            .is_err());
        assert!(!tracker.drain().await);
        assert_eq!(admission.snapshot().0, 1);
        assert!(!dropped.load(Ordering::SeqCst));
        // The original terminal failure remains the first cause.
        assert_eq!(
            output.playback_state().unwrap().snapshot().phase,
            PlaybackPhase::Failed(Failure::Unsupported)
        );
        drop(output);
        drop(tracker);
        assert!(dropped.load(Ordering::SeqCst));
        assert_eq!(admission.snapshot().0, 0);
    }

    #[tokio::test]
    async fn finite_native_source_keeps_its_accepted_opening_drainable_after_cleanup() {
        struct Finite(Option<Bytes>);
        #[async_trait]
        impl TsSource for Finite {
            async fn next(&mut self) -> Option<Bytes> {
                self.0.take()
            }
            fn stats(&self) -> SourceStats {
                SourceStats::default()
            }
        }
        let tracker = crate::manager::CleanupTracker::default();
        let expected = crate::hls_audio::process::test_fixtures::transport_prefix();
        let mut output = spawn(
            Box::new(Finite(Some(expected.clone().into()))),
            PreparedNormalizer {
                executable: "/not/an/executable".into(),
            },
            Duration::from_secs(5),
            &tracker,
        );
        assert!(tracker.drain().await);
        assert_eq!(output.next().await.unwrap().as_ref(), expected.as_slice());
        assert!(output.next().await.is_none());
        output.shutdown().await;
    }

    #[tokio::test]
    async fn transformed_source_reaps_child_and_pumps_before_releasing_source_admission() {
        use crate::hls_audio::process::test_support::{
            fake_prepared_child, process_test_lock, ChildMode,
        };
        let _serial = process_test_lock().await;
        let tracker = crate::manager::CleanupTracker::default();
        let admission = crate::admission::Admission::new(1);
        let dropped = Arc::new(AtomicBool::new(false));
        let (prepared, counters) = fake_prepared_child(ChildMode::ReadyThenStall);
        let input = ac3_input();
        let source = Box::new(crate::admission::Source {
            inner: Box::new(Fixture {
                bytes: Some(input.into()),
                dropped: dropped.clone(),
            }),
            _reservation: admission.reserve().unwrap(),
        });
        let mut output = spawn(source, prepared, Duration::from_secs(5), &tracker);
        let delivery = timeout(Duration::from_secs(3), output.next()).await;
        let state_before_cleanup = output.playback_state().unwrap().snapshot();
        let starts = counters.snapshot().children_started;
        let admission_before_cleanup = admission.snapshot().0;
        let dropped_before_cleanup = dropped.load(Ordering::SeqCst);
        let shutdown = timeout(Duration::from_secs(3), output.shutdown()).await;
        let drained = timeout(Duration::from_secs(3), tracker.drain()).await;
        assert!(shutdown.is_ok());
        assert_eq!(drained, Ok(true));
        let bytes = delivery
            .unwrap()
            .unwrap_or_else(|| panic!("no media: {state_before_cleanup:?}"));
        assert!(bytes.len() >= 564);
        assert_eq!(state_before_cleanup.phase, PlaybackPhase::Ready);
        assert_eq!(starts, 1);
        assert_eq!(admission_before_cleanup, 1);
        assert!(!dropped_before_cleanup);
        let cleanup = counters.snapshot();
        assert_eq!((cleanup.children_started, cleanup.children_reaped), (1, 1));
        assert_eq!(cleanup.pumps_started, cleanup.pumps_joined);
        assert_eq!(cleanup.active_tasks, 0);
        assert!(dropped.load(Ordering::SeqCst));
        assert_eq!(admission.snapshot().0, 0);
    }

    #[tokio::test]
    async fn native_aac_uses_no_child_and_retains_source_ownership_until_shutdown() {
        let tracker = crate::manager::CleanupTracker::default();
        let dropped = Arc::new(AtomicBool::new(false));
        let bytes = crate::hls_audio::process::test_fixtures::transport_prefix();
        let expected = bytes.clone();
        let source = Box::new(Fixture {
            bytes: Some(bytes.into()),
            dropped: dropped.clone(),
        });
        let mut output = spawn(
            source,
            crate::hls_audio::PreparedNormalizer {
                executable: "/not/an/executable".into(),
            },
            Duration::from_secs(5),
            &tracker,
        );
        let state = output.playback_state().unwrap();
        let actual = timeout(Duration::from_secs(1), output.next())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(actual.as_ref(), expected.as_slice());
        assert_eq!(state.snapshot().phase, PlaybackPhase::Ready);
        assert_eq!(output.stats().peers, 7);
        assert_eq!(output.stats_handle().unwrap().snapshot().downloaded, 1234);
        assert_eq!(
            output.metadata().title.as_deref(),
            Some("Synthetic audio fixture")
        );
        assert!(!dropped.load(Ordering::SeqCst));
        output.shutdown().await;
        assert!(tracker.drain().await);
        assert!(dropped.load(Ordering::SeqCst));
        assert_eq!(state.snapshot().phase, PlaybackPhase::Stopped);
    }
    #[tokio::test]
    async fn unsupported_input_fails_before_eof_and_releases_admission_after_cleanup() {
        let tracker = crate::manager::CleanupTracker::default();
        let admission = crate::admission::Admission::new(1);
        let dropped = Arc::new(AtomicBool::new(false));
        let source = Box::new(crate::admission::Source {
            inner: Box::new(Fixture {
                bytes: Some(vec![0xff; 188].into()),
                dropped: dropped.clone(),
            }),
            _reservation: admission.reserve().unwrap(),
        });
        let mut output = spawn(
            source,
            crate::hls_audio::PreparedNormalizer {
                executable: "/not/an/executable".into(),
            },
            Duration::from_secs(5),
            &tracker,
        );
        let state = output.playback_state().unwrap();
        assert!(timeout(Duration::from_secs(1), output.next())
            .await
            .unwrap()
            .is_none());
        output.shutdown().await;
        assert!(tracker.drain().await);
        assert!(dropped.load(Ordering::SeqCst));
        assert_eq!(admission.snapshot().0, 0);
        assert_eq!(
            state.snapshot().phase,
            PlaybackPhase::Failed(Failure::Unsupported)
        );
    }
}
