//! Latest-value playback state; the handle never exposes a held watch borrow.

use super::Failure;
use tokio::sync::watch;
use tokio::time::Instant;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PlaybackPhase {
    Classifying,
    Starting,
    Ready,
    Recovering,
    Failed(Failure),
    Stopped,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PlaybackSnapshot {
    pub generation: u64,
    pub phase: PlaybackPhase,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PlaybackWaitError {
    Failed(Failure),
    Stopped,
    Closed,
    Deadline,
    Cancelled,
}

/// Read-only latest state. Snapshot copies do not expose a watch lock across I/O.
#[derive(Clone, Debug)]
pub struct PlaybackStateHandle {
    receiver: watch::Receiver<PlaybackSnapshot>,
}
impl PlaybackStateHandle {
    pub fn snapshot(&self) -> PlaybackSnapshot {
        *self.receiver.borrow()
    }
    pub(crate) async fn changed(&mut self) -> Result<(), watch::error::RecvError> {
        self.receiver.changed().await
    }
    pub fn is_closed(&self) -> bool {
        self.receiver.has_changed().is_err()
    }
    /// A ready snapshot is only an observation, not a guarantee against later
    /// failure. Body/status consumers must continue observing terminal state.
    pub async fn wait_ready(
        &self,
        deadline: Instant,
        cancel: &mut watch::Receiver<bool>,
    ) -> Result<PlaybackSnapshot, PlaybackWaitError> {
        let mut receiver = self.receiver.clone();
        loop {
            if *cancel.borrow() || cancel.has_changed().is_err() {
                return Err(PlaybackWaitError::Cancelled);
            }
            let current = *receiver.borrow_and_update();
            match current.phase {
                PlaybackPhase::Failed(reason) => return Err(PlaybackWaitError::Failed(reason)),
                PlaybackPhase::Stopped => return Err(PlaybackWaitError::Stopped),
                _ => {}
            }
            if receiver.has_changed().is_err() {
                return Err(PlaybackWaitError::Closed);
            }
            if Instant::now() >= deadline {
                return Err(PlaybackWaitError::Deadline);
            }
            if current.phase == PlaybackPhase::Ready {
                return Ok(current);
            }
            tokio::select! {
                biased;
                changed = cancel.changed() => {
                    if changed.is_err() || *cancel.borrow() {
                        return Err(PlaybackWaitError::Cancelled);
                    }
                }
                changed = receiver.changed() => {
                    if changed.is_err() {
                        // A final failure stays inspectable after sender closure.
                        let final_state = *receiver.borrow();
                        return match final_state.phase {
                            PlaybackPhase::Failed(reason) => Err(PlaybackWaitError::Failed(reason)),
                            PlaybackPhase::Stopped => Err(PlaybackWaitError::Stopped),
                            _ => Err(PlaybackWaitError::Closed),
                        };
                    }
                }
                _ = tokio::time::sleep_until(deadline) => return Err(PlaybackWaitError::Deadline),
            }
        }
    }
}

/// Only normalization owners write state. Failed/stopped owners cannot resurrect
/// themselves: a new source needs a new publisher, not an error-history map.
pub(crate) struct PlaybackPublisher {
    sender: watch::Sender<PlaybackSnapshot>,
}
impl PlaybackPublisher {
    pub(crate) fn new() -> (Self, PlaybackStateHandle) {
        let (sender, receiver) = watch::channel(PlaybackSnapshot {
            generation: 0,
            phase: PlaybackPhase::Classifying,
        });
        (Self { sender }, PlaybackStateHandle { receiver })
    }
    pub(crate) fn begin_generation(&self, generation: u64) -> bool {
        self.sender.send_if_modified(|current| {
            if generation <= current.generation || terminal(current.phase) {
                return false;
            }
            current.phase = if current.generation == 0 {
                PlaybackPhase::Starting
            } else {
                PlaybackPhase::Recovering
            };
            current.generation = generation;
            true
        })
    }
    pub(crate) fn mark_ready(&self, generation: u64) -> bool {
        self.sender.send_if_modified(|current| {
            if current.generation != generation
                || !matches!(
                    current.phase,
                    PlaybackPhase::Starting | PlaybackPhase::Recovering
                )
            {
                return false;
            }
            current.phase = PlaybackPhase::Ready;
            true
        })
    }
    pub(crate) fn fail(&self, generation: u64, reason: Failure) -> bool {
        self.sender.send_if_modified(|current| {
            if current.generation != generation || terminal(current.phase) {
                return false;
            }
            current.phase = PlaybackPhase::Failed(reason);
            true
        })
    }
    pub(crate) fn stop(&self) -> bool {
        self.sender.send_if_modified(|current| {
            if terminal(current.phase) {
                return false;
            }
            current.phase = PlaybackPhase::Stopped;
            true
        })
    }
}
fn terminal(phase: PlaybackPhase) -> bool {
    matches!(phase, PlaybackPhase::Failed(_) | PlaybackPhase::Stopped)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::audio_normalize::Failure;
    use tokio::sync::watch;
    use tokio::time::{Duration, Instant};

    #[test]
    fn failed_state_is_retained_and_cannot_be_stopped_or_restarted() {
        let (publisher, handle) = PlaybackPublisher::new();
        assert_eq!(handle.snapshot().phase, PlaybackPhase::Classifying);
        assert!(publisher.begin_generation(1));
        assert!(publisher.mark_ready(1));
        assert!(publisher.fail(1, Failure::ChildExit));
        assert!(!publisher.stop());
        assert!(!publisher.begin_generation(2));
        assert!(!publisher.fail(1, Failure::Pipe));
        assert_eq!(
            handle.snapshot(),
            PlaybackSnapshot {
                generation: 1,
                phase: PlaybackPhase::Failed(Failure::ChildExit)
            }
        );
        drop(publisher);
        assert!(handle.is_closed());
        assert_eq!(
            handle.snapshot().phase,
            PlaybackPhase::Failed(Failure::ChildExit)
        );
    }

    #[test]
    fn retired_generation_cannot_mark_ready_or_fail_its_replacement() {
        let (publisher, handle) = PlaybackPublisher::new();
        assert!(publisher.begin_generation(1));
        assert!(publisher.mark_ready(1));
        assert!(publisher.begin_generation(2));
        assert_eq!(handle.snapshot().phase, PlaybackPhase::Recovering);
        assert!(!publisher.mark_ready(1));
        assert!(!publisher.fail(1, Failure::OutputInvalid));
        assert!(!publisher.begin_generation(2));
        assert!(!publisher.begin_generation(0));
        assert_eq!(handle.snapshot().generation, 2);
        assert!(publisher.mark_ready(2));
        assert!(publisher.stop());
        assert!(!publisher.mark_ready(2));
        assert!(!publisher.begin_generation(3));
    }

    #[tokio::test(start_paused = true)]
    async fn readiness_never_turns_a_deadline_cancel_or_closed_writer_into_success() {
        let (publisher, handle) = PlaybackPublisher::new();
        publisher.begin_generation(1);
        publisher.mark_ready(1);
        let (_owner, mut cancel) = watch::channel(false);
        assert_eq!(
            handle.wait_ready(Instant::now(), &mut cancel).await,
            Err(PlaybackWaitError::Deadline)
        );
        let (owner, mut cancel) = watch::channel(true);
        assert_eq!(
            handle
                .wait_ready(Instant::now() + Duration::from_secs(1), &mut cancel)
                .await,
            Err(PlaybackWaitError::Cancelled)
        );
        owner.send_replace(false);
        drop(publisher);
        assert_eq!(
            handle
                .wait_ready(Instant::now() + Duration::from_secs(1), &mut cancel)
                .await,
            Err(PlaybackWaitError::Closed)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn readiness_wait_observes_published_failure_and_cancel_without_media() {
        let (publisher, handle) = PlaybackPublisher::new();
        let (_owner, mut cancel) = watch::channel(false);
        let waiting = tokio::spawn(async move {
            handle
                .wait_ready(Instant::now() + Duration::from_secs(10), &mut cancel)
                .await
        });
        tokio::task::yield_now().await;
        publisher.fail(0, Failure::Unsupported);
        assert_eq!(
            waiting.await.unwrap(),
            Err(PlaybackWaitError::Failed(Failure::Unsupported))
        );
        let (_publisher, handle) = PlaybackPublisher::new();
        let (owner, mut cancel) = watch::channel(false);
        let waiting = tokio::spawn(async move {
            handle
                .wait_ready(Instant::now() + Duration::from_secs(10), &mut cancel)
                .await
        });
        tokio::task::yield_now().await;
        owner.send_replace(true);
        assert_eq!(waiting.await.unwrap(), Err(PlaybackWaitError::Cancelled));
    }

    #[tokio::test(start_paused = true)]
    async fn readiness_returns_the_current_ready_generation_and_retains_terminal_reason() {
        let (publisher, handle) = PlaybackPublisher::new();
        let (_owner, mut cancel) = watch::channel(false);
        publisher.begin_generation(1);
        publisher.begin_generation(2);
        publisher.mark_ready(2);
        assert_eq!(
            handle
                .wait_ready(Instant::now() + Duration::from_secs(1), &mut cancel)
                .await
                .unwrap()
                .generation,
            2
        );
        publisher.fail(2, Failure::OutputStall);
        drop(publisher);
        assert_eq!(
            handle
                .wait_ready(Instant::now() + Duration::from_secs(1), &mut cancel)
                .await,
            Err(PlaybackWaitError::Failed(Failure::OutputStall))
        );
    }
}
