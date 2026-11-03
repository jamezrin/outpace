//! Research adapters. Only qualified process/network owners may execute these.
use crate::{remaining_budget, Accounting, WorkerInput};
use futures_lite::{Stream, StreamExt};
use std::{
    net::SocketAddrV4,
    time::{Duration, Instant},
};

#[derive(Clone, Copy, Debug)]
pub enum Candidate {
    Mainline,
    N0,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Terminal {
    Target,
    Exhausted,
    Deadline,
    AdapterError,
}
#[derive(Debug)]
pub struct Record {
    pub peer_count: usize,
    pub first_peer: Option<Duration>,
    pub elapsed: Duration,
    pub terminal: Terminal,
    pub batch_sizes: Vec<usize>,
    pub batches_observed: usize,
    pub batch_history_truncated: bool,
    pub target_overshoot: usize,
}
fn empty(start: Instant, terminal: Terminal) -> Record {
    Record {
        peer_count: 0,
        first_peer: None,
        elapsed: start.elapsed(),
        terminal,
        batch_sizes: Vec::new(),
        batches_observed: 0,
        batch_history_truncated: false,
        target_overshoot: 0,
    }
}
async fn harvest<S: Stream<Item = Vec<SocketAddrV4>> + Unpin>(
    mut stream: S,
    input: &WorkerInput,
    start: Instant,
) -> Record {
    let mut counts = Accounting::new(input.target, input.result_cap);
    let terminal = loop {
        let Some(left) = remaining_budget(start.elapsed()) else {
            break Terminal::Deadline;
        };
        match tokio::time::timeout(left, stream.next()).await {
            Err(_) => break Terminal::Deadline,
            Ok(None) if remaining_budget(start.elapsed()).is_none() => break Terminal::Deadline,
            Ok(None) => break Terminal::Exhausted,
            Ok(Some(batch)) => {
                let elapsed = start.elapsed();
                if remaining_budget(elapsed).is_none() {
                    break Terminal::Deadline;
                }
                if counts.observe(&batch, elapsed) {
                    break Terminal::Target;
                }
            }
        }
    };
    Record {
        peer_count: counts.count(),
        first_peer: counts.first_peer(),
        elapsed: start.elapsed(),
        terminal,
        batch_sizes: counts.batch_sizes().to_vec(),
        batches_observed: counts.batches_observed(),
        batch_history_truncated: counts.batch_history_truncated(),
        target_overshoot: counts.target_overshoot(),
    }
}

/// Includes construction in the lookup clock; does not claim actor/task join.
/// A supervisor must contain blocking constructors and enforce process teardown.
pub async fn run(candidate: Candidate, input: &WorkerInput) -> Record {
    let start = Instant::now();
    match candidate {
        Candidate::Mainline => {
            let Ok(id) = mainline::Id::from_bytes(input.infohash()) else {
                return empty(start, Terminal::AdapterError);
            };
            let mut builder = mainline::Dht::builder();
            builder.bootstrap(&input.seeds).port(0);
            let Ok(node) = builder.build() else {
                return empty(start, Terminal::AdapterError);
            };
            let node = node.as_async();
            if remaining_budget(start.elapsed()).is_none() {
                return empty(start, Terminal::Deadline);
            }
            harvest(node.get_peers(id), input, start).await
        }
        Candidate::N0 => {
            let Ok(id) = n0_mainline::Id::from_bytes(input.infohash()) else {
                return empty(start, Terminal::AdapterError);
            };
            let seeds: Vec<_> = input.seeds.iter().map(ToString::to_string).collect();
            let mut builder = n0_mainline::Dht::builder();
            builder.bootstrap(&seeds).port(0);
            let Ok(node) = builder.build() else {
                return empty(start, Terminal::AdapterError);
            };
            let Some(left) = remaining_budget(start.elapsed()) else {
                return empty(start, Terminal::Deadline);
            };
            match tokio::time::timeout(left, node.get_peers(id)).await {
                Err(_) => empty(start, Terminal::Deadline),
                Ok(Err(_)) => empty(start, Terminal::AdapterError),
                Ok(Ok(stream)) => harvest(stream, input, start).await,
            }
        }
    }
}

#[cfg(test)]
mod clock_tests {
    use super::*;
    use std::{
        pin::Pin,
        sync::{
            atomic::{AtomicBool, AtomicUsize, Ordering},
            Arc,
        },
        task::{Context, Poll},
    };

    fn input() -> WorkerInput {
        WorkerInput::parse("0123456789abcdef0123456789abcdef01234567", "127.0.0.1:9001").unwrap()
    }

    #[tokio::test]
    async fn record_keeps_batch_sizes_and_target_overshoot() {
        let peers = (10001..=10010)
            .map(|port| SocketAddrV4::new(std::net::Ipv4Addr::LOCALHOST, port))
            .collect();
        let record = harvest(
            futures_lite::stream::iter([peers]),
            &input(),
            Instant::now(),
        )
        .await;
        assert_eq!(record.batch_sizes, vec![10]);
        assert_eq!(record.batches_observed, 1);
        assert_eq!(record.target_overshoot, 2);
        assert!(!record.batch_history_truncated);
    }
    #[tokio::test]
    async fn consumed_constructor_budget_does_not_poll_results() {
        let stream = futures_lite::stream::poll_fn(|_| -> Poll<Option<Vec<SocketAddrV4>>> {
            panic!("results polled after whole lookup budget expired")
        });
        let record = harvest(stream, &input(), Instant::now() - Duration::from_secs(16)).await;
        assert_eq!(record.terminal, Terminal::Deadline);
        assert_eq!(record.peer_count, 0);
        assert!(record.first_peer.is_none());
    }

    #[tokio::test]
    async fn deadline_wins_over_late_end_of_stream() {
        let start = Instant::now() - Duration::from_secs(14);
        let deadline = start + Duration::from_secs(15);
        let stream = futures_lite::stream::poll_fn(move |_| -> Poll<Option<Vec<SocketAddrV4>>> {
            // A stream poll can finish after the timeout's deadline. Tokio polls
            // the inner future first; an elapsed deadline must still win.
            std::thread::sleep(
                deadline.saturating_duration_since(Instant::now()) + Duration::from_millis(5),
            );
            Poll::Ready(None)
        });
        let record = harvest(stream, &input(), start).await;
        assert_eq!(record.terminal, Terminal::Deadline);
        assert_eq!(record.peer_count, 0);
        assert!(record.first_peer.is_none());
    }

    struct PendingStream {
        dropped: Arc<AtomicBool>,
        polls: Arc<AtomicUsize>,
    }
    impl Stream for PendingStream {
        type Item = Vec<SocketAddrV4>;
        fn poll_next(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            self.polls.fetch_add(1, Ordering::SeqCst);
            Poll::Pending
        }
    }
    impl Drop for PendingStream {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::SeqCst);
        }
    }
    #[tokio::test]
    async fn cancelling_harvest_releases_its_stream_without_claiming_actor_join() {
        let dropped = Arc::new(AtomicBool::new(false));
        let polls = Arc::new(AtomicUsize::new(0));
        let stream = PendingStream {
            dropped: dropped.clone(),
            polls: polls.clone(),
        };
        let input = input();
        let mut future = Box::pin(harvest(stream, &input, Instant::now()));
        assert!(futures_lite::future::poll_once(&mut future).await.is_none());
        assert!(polls.load(Ordering::SeqCst) > 0);
        assert!(!dropped.load(Ordering::SeqCst));
        drop(future);
        assert!(dropped.load(Ordering::SeqCst));
    }
}
