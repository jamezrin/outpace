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
}
fn empty(start: Instant, terminal: Terminal) -> Record {
    Record {
        peer_count: 0,
        first_peer: None,
        elapsed: start.elapsed(),
        terminal,
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
