//! Bounded, epoch-fenced transport output for one shared normalization source.

use crate::audio_normalize::{
    Failure, PlaybackPhase, PlaybackPublisher, PlaybackStateHandle, State,
};
use crate::hls_audio::process::{AcceptedBatch, ProcessOutput};
use bytes::Bytes;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;

const CHUNK_BYTES: usize = 32_712; // 174 whole packets, below 32 KiB.
const QUEUE_CHUNKS: usize = 4;
type Permit = mpsc::OwnedPermit<Frame>;
pub(crate) struct Frame {
    generation: u64,
    bytes: Bytes,
}
struct Pending {
    bytes: Vec<u8>,
    permit: Permit,
}
struct Inner {
    publisher: PlaybackPublisher,
    state: PlaybackStateHandle,
    pending: Option<Pending>,
    opening: bool,
}
pub(crate) struct TsControl {
    inner: Arc<Mutex<Inner>>,
    tx: mpsc::Sender<Frame>,
}
#[derive(Clone)]
pub(crate) struct TsOutput {
    inner: Arc<Mutex<Inner>>,
    tx: mpsc::Sender<Frame>,
    generation: u64,
}
pub(crate) struct TsReceiver {
    rx: mpsc::Receiver<Frame>,
    state: PlaybackStateHandle,
    delivered_generation: Option<u64>,
}
impl Drop for TsControl {
    fn drop(&mut self) {
        let mut inner = self.inner.lock().unwrap();
        inner.publisher.stop();
        inner.pending = None;
    }
}
impl TsControl {
    pub(crate) fn new() -> (Self, TsReceiver) {
        let (publisher, state) = PlaybackPublisher::new();
        let (tx, rx) = mpsc::channel(QUEUE_CHUNKS);
        let inner = Arc::new(Mutex::new(Inner {
            publisher,
            state: state.clone(),
            pending: None,
            opening: true,
        }));
        (
            Self { inner, tx },
            TsReceiver {
                rx,
                state,
                delivered_generation: None,
            },
        )
    }
    pub(crate) fn state(&self) -> PlaybackStateHandle {
        self.inner.lock().unwrap().state.clone()
    }
    pub(crate) fn output(&self, generation: u64) -> TsOutput {
        TsOutput {
            inner: self.inner.clone(),
            tx: self.tx.clone(),
            generation,
        }
    }
    pub(crate) fn begin(&self, generation: u64) -> Option<TsOutput> {
        let mut inner = self.inner.lock().unwrap();
        if !inner.publisher.begin_generation(generation) {
            return None;
        }
        inner.pending = None;
        inner.opening = true;
        Some(TsOutput {
            inner: self.inner.clone(),
            tx: self.tx.clone(),
            generation,
        })
    }
}
impl TsReceiver {
    pub(crate) fn delivered_generation(&self) -> Option<u64> {
        self.delivered_generation
    }
    pub(crate) async fn next(&mut self) -> Option<(Bytes, bool)> {
        loop {
            let frame = match self.state.snapshot().phase {
                PlaybackPhase::Failed(_) => return None,
                PlaybackPhase::Stopped => self.rx.try_recv().ok()?,
                _ => tokio::select! {
                    biased;
                    changed = self.state.changed() => {
                        if changed.is_err() && !matches!(self.state.snapshot().phase, PlaybackPhase::Failed(_) | PlaybackPhase::Stopped) { return None; }
                        continue;
                    }
                    frame = self.rx.recv() => frame?,
                },
            };
            let state = self.state.snapshot();
            if matches!(state.phase, PlaybackPhase::Failed(_)) {
                return None;
            }
            if frame.generation != state.generation {
                continue;
            }
            let gap = frame.generation > 1 && self.delivered_generation != Some(frame.generation);
            self.delivered_generation = Some(frame.generation);
            return Some((frame.bytes, gap));
        }
    }
}
impl TsOutput {
    fn current(&self, inner: &Inner) -> bool {
        let s = inner.state.snapshot();
        s.generation == self.generation
            && !matches!(s.phase, PlaybackPhase::Failed(_) | PlaybackPhase::Stopped)
    }
    fn flush(&self, inner: &mut Inner) {
        if let Some(pending) = inner.pending.take() {
            pending.permit.send(Frame {
                generation: self.generation,
                bytes: Bytes::from(pending.bytes),
            });
            if inner.opening {
                inner.opening = false;
                inner.publisher.mark_ready(self.generation);
            }
        }
    }
}
impl ProcessOutput for TsOutput {
    type Reservation = Option<Permit>;
    fn state(&self) -> State {
        match self.inner.lock().unwrap().state.snapshot().phase {
            PlaybackPhase::Classifying | PlaybackPhase::Starting => State::Starting,
            PlaybackPhase::Ready => State::Ready,
            PlaybackPhase::Recovering => State::Recovering,
            PlaybackPhase::Failed(reason) => State::Failed(reason),
            PlaybackPhase::Stopped => State::Stopped,
        }
    }
    fn fail(&self, reason: Failure) {
        let mut inner = self.inner.lock().unwrap();
        if inner.publisher.fail(self.generation, reason) {
            inner.pending = None;
        }
    }
    async fn reserve(&self) -> Result<Self::Reservation, Failure> {
        self.tx
            .clone()
            .reserve_owned()
            .await
            .map(Some)
            .map_err(|_| Failure::Pipe)
    }
    async fn reserve_batch(&self, batch: &AcceptedBatch) -> Result<Self::Reservation, Failure> {
        let fits = self
            .inner
            .lock()
            .unwrap()
            .pending
            .as_ref()
            .is_some_and(|p| p.bytes.len() + batch.len * 188 <= CHUNK_BYTES);
        if fits {
            Ok(None)
        } else {
            self.reserve().await
        }
    }
    fn publish(&self, reservation: Self::Reservation, batch: AcceptedBatch) {
        let mut inner = self.inner.lock().unwrap();
        if !self.current(&inner) {
            return;
        }
        if let Some(permit) = reservation {
            self.flush(&mut inner);
            inner.pending = Some(Pending {
                bytes: Vec::with_capacity(CHUNK_BYTES),
                permit,
            });
        }
        let pending = inner.pending.as_mut().expect("batch capacity reserved");
        for packet in batch.iter() {
            pending.bytes.extend_from_slice(packet);
        }
        if inner.opening {
            self.flush(&mut inner);
        }
    }
    fn flush_pending(&self) {
        let mut inner = self.inner.lock().unwrap();
        if self.current(&inner) {
            self.flush(&mut inner);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::audio_normalize::{Failure, PlaybackPhase};
    use crate::hls_audio::process::{AcceptedBatch, ProcessOutput};
    use std::task::Poll;

    fn batch(value: u8, count: usize) -> AcceptedBatch {
        AcceptedBatch {
            packets: [[value; 188]; 3],
            len: count,
        }
    }
    async fn send(sink: &TsOutput, value: u8, count: usize) {
        let b = batch(value, count);
        let permit = sink.reserve_batch(&b).await.unwrap();
        sink.publish(permit, b);
    }

    struct PacketReads {
        bytes: Vec<u8>,
        offset: usize,
        hold_open: bool,
    }
    impl tokio::io::AsyncRead for PacketReads {
        fn poll_read(
            mut self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            buffer: &mut tokio::io::ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            if self.hold_open && self.offset == self.bytes.len() {
                return Poll::Pending;
            }
            let n = (self.bytes.len() - self.offset)
                .min(188)
                .min(buffer.remaining());
            buffer.put_slice(&self.bytes[self.offset..self.offset + n]);
            self.offset += n;
            Poll::Ready(Ok(()))
        }
    }
    #[tokio::test]
    async fn pending_input_flushes_a_low_volume_tail_without_waiting_for_more_bytes_or_eof() {
        use crate::hls_audio::process::{test_fixtures, test_output_pump};
        use ace_media::audio::{AudioCodec, SelectedAudio, TransportLayout};
        let bytes = test_fixtures::ready_output_fixture(2);
        let expected = bytes.clone();
        let layout = TransportLayout {
            transport_stream_id: 7,
            program_id: 9,
            pmt_pid: 100,
            pcr_pid: 513,
            video_pid: 512,
            video_type: 0x1b,
            audio: Some(SelectedAudio {
                pid: 513,
                codec: AudioCodec::Ac3,
            }),
            semantic_identity: vec![],
        };
        let (control, mut receiver) = TsControl::new();
        let sink = control.begin(1).unwrap();
        let mut pumping = Box::pin(test_output_pump(
            PacketReads {
                bytes,
                offset: 0,
                hold_open: true,
            },
            layout,
            sink,
        ));
        let observed = tokio::time::timeout(std::time::Duration::from_secs(1), async {
            let read = async {
                let mut data = Vec::new();
                let mut chunks = 0;
                while data.len() < expected.len() {
                    let (bytes, _) = receiver.next().await.expect("valid pending tail");
                    data.extend_from_slice(&bytes);
                    chunks += 1;
                }
                (data, chunks)
            };
            tokio::select! { biased;
                result = &mut pumping => panic!("read fixture must remain pending, got {result:?}"),
                collected = read => collected,
            }
        })
        .await;
        drop(pumping);
        drop(control);
        let (data, chunks) = observed.expect("pending read flushes without EOF");
        assert_eq!(data, expected);
        assert_eq!(
            chunks, 2,
            "immediate opening and one low-volume continuation"
        );
    }

    #[tokio::test]
    async fn continuously_ready_packet_sized_reads_are_coalesced_across_read_boundaries() {
        use crate::hls_audio::process::{test_fixtures, test_output_pump};
        use ace_media::audio::{AudioCodec, SelectedAudio, TransportLayout};
        let bytes = test_fixtures::ready_output_fixture(200);
        let expected = bytes.clone();
        let layout = TransportLayout {
            transport_stream_id: 7,
            program_id: 9,
            pmt_pid: 100,
            pcr_pid: 513,
            video_pid: 512,
            video_type: 0x1b,
            audio: Some(SelectedAudio {
                pid: 513,
                codec: AudioCodec::Ac3,
            }),
            semantic_identity: vec![],
        };
        let (control, mut receiver) = TsControl::new();
        let sink = control.begin(1).unwrap();
        let observed = tokio::time::timeout(std::time::Duration::from_secs(1), async {
            let read = async {
                let mut data = Vec::new();
                let mut chunks = 0;
                while data.len() < expected.len() {
                    let (bytes, _) = receiver.next().await.expect("accepted media");
                    data.extend_from_slice(&bytes);
                    chunks += 1;
                }
                (data, chunks)
            };
            tokio::join!(
                test_output_pump(
                    PacketReads {
                        bytes,
                        offset: 0,
                        hold_open: false
                    },
                    layout,
                    sink
                ),
                read
            )
        })
        .await;
        drop(control);
        let (result, (data, chunks)) = observed.expect("bounded fragmented stream");
        assert_eq!(result, Err(Failure::Pipe));
        assert_eq!(data, expected);
        assert_eq!(
            chunks, 3,
            "opening plus bounded full and trailing chunks, not one message/allocation per packet"
        );
    }

    #[tokio::test]
    async fn real_validator_and_read_boundary_flush_low_volume_output_in_exact_order() {
        use crate::hls_audio::process::{test_fixtures, test_output_pump};
        use ace_media::audio::{AudioCodec, SelectedAudio, TransportLayout};
        let mut bytes = test_fixtures::ready_output_fixture(0);
        let opening_len = bytes.len() + 188; // Tables plus the first clean video packet.
        bytes.extend_from_slice(&test_fixtures::video_access_packet(256));
        bytes.extend_from_slice(&test_fixtures::video_access_packet(256));
        let layout = TransportLayout {
            transport_stream_id: 7,
            program_id: 9,
            pmt_pid: 100,
            pcr_pid: 513,
            video_pid: 512,
            video_type: 0x1b,
            audio: Some(SelectedAudio {
                pid: 513,
                codec: AudioCodec::Ac3,
            }),
            semantic_identity: vec![],
        };
        let (control, mut receiver) = TsControl::new();
        let sink = control.begin(1).unwrap();
        assert_eq!(
            test_output_pump(bytes.as_slice(), layout, sink).await,
            Err(Failure::Pipe)
        );
        let (first, _) = receiver.next().await.unwrap();
        assert_eq!(first.as_ref(), &bytes[..opening_len]);
        let (last, _) =
            tokio::time::timeout(std::time::Duration::from_millis(100), receiver.next())
                .await
                .expect("read boundary flushes partial chunk")
                .unwrap();
        assert_eq!(last.as_ref(), &bytes[opening_len..]);
    }

    #[tokio::test]
    async fn retired_held_bytes_and_acquired_capacity_do_not_cross_epoch_or_owner_drop() {
        let (control, mut receiver) = TsControl::new();
        let old = control.begin(1).unwrap();
        send(&old, 1, 3).await;
        receiver.next().await.unwrap();
        send(&old, 2, 1).await; // Held, not flushed yet.
        let b = batch(3, 1);
        let acquired = old.reserve().await.unwrap();
        let replacement = control.begin(2).unwrap();
        old.publish(acquired, b);
        old.flush_pending();
        send(&replacement, 4, 3).await;
        assert_eq!(
            receiver.next().await.unwrap(),
            (bytes::Bytes::from(vec![4; 564]), true)
        );
        let mut next = Box::pin(receiver.next());
        assert!(matches!(futures::poll!(&mut next), Poll::Pending));
        drop(control);
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(100), next)
                .await
                .expect("owner drop wakes receiver")
                .is_none()
        );
        assert_eq!(replacement.state(), crate::audio_normalize::State::Stopped);
    }

    #[tokio::test]
    async fn terminal_failure_wakes_a_receiver_with_no_media() {
        let (control, mut receiver) = TsControl::new();
        let sink = control.begin(1).unwrap();
        let mut next = Box::pin(receiver.next());
        assert!(matches!(futures::poll!(&mut next), Poll::Pending));
        sink.fail(Failure::ChildExit);
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(100), next)
                .await
                .expect("failure must wake reader")
                .is_none()
        );
    }

    #[tokio::test]
    async fn opening_is_immediate_and_continuations_coalesce_without_losing_bytes() {
        let (control, mut receiver) = TsControl::new();
        let sink = control.begin(1).unwrap();
        assert_eq!(control.state().snapshot().phase, PlaybackPhase::Starting);
        send(&sink, 1, 3).await;
        let (opening, gap) = receiver.next().await.unwrap();
        assert!(!gap);
        assert_eq!(opening.as_ref(), &[1; 564]);
        assert_eq!(control.state().snapshot().phase, PlaybackPhase::Ready);
        for _ in 0..200 {
            send(&sink, 2, 1).await;
        }
        sink.flush_pending();
        let (first, gap) = receiver.next().await.unwrap();
        assert!(!gap);
        assert_eq!(first.as_ref(), &[2; 32712]);
        let (last, _) = receiver.next().await.unwrap();
        assert_eq!(last.as_ref(), &[2; 4888]);
    }

    #[tokio::test]
    async fn full_queue_wait_is_cancel_safe_and_old_reservations_cannot_publish() {
        let (control, mut receiver) = TsControl::new();
        let old = control.begin(1).unwrap();
        for n in 0..4 {
            send(&old, n, 1).await;
            old.flush_pending();
        }
        let b = batch(7, 1);
        let mut blocked = Box::pin(old.reserve_batch(&b));
        assert!(matches!(futures::poll!(&mut blocked), Poll::Pending));
        drop(blocked);
        let replacement = control.begin(2).unwrap();
        // Draining stale frames releases capacity, but never returns old media.
        let mut next = Box::pin(receiver.next());
        assert!(matches!(futures::poll!(&mut next), Poll::Pending));
        drop(next);
        let stale_permit = old.reserve_batch(&b).await.unwrap();
        old.publish(stale_permit, b);
        old.flush_pending();
        send(&replacement, 9, 3).await;
        let (bytes, gap) = receiver.next().await.unwrap();
        assert_eq!(bytes.as_ref(), &[9; 564]);
        assert!(gap);
        assert!(!control.begin(2).is_some());
        old.fail(Failure::Pipe);
        assert_eq!(control.state().snapshot().phase, PlaybackPhase::Ready);
        replacement.fail(Failure::ChildExit);
        assert!(receiver.next().await.is_none());
        assert_eq!(
            control.state().snapshot().phase,
            PlaybackPhase::Failed(Failure::ChildExit)
        );
    }
}
