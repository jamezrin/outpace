use super::*;

pub type PeerDiscovery = Arc<
    dyn Fn(DiscoveryOptions, mpsc::Sender<SocketAddrV4>) -> Pin<Box<dyn Future<Output = ()> + Send>>
        + Send
        + Sync,
>;

pub struct DiscoveryRun {
    pub(super) receiver: mpsc::Receiver<SocketAddrV4>,
    _worker: Option<OwnedTask>,
}
impl DiscoveryRun {
    /// Cancel discovery and wait for its worker resources to be destroyed.
    /// Canceling this waiter retains the join handle for a subsequent call.
    pub async fn shutdown(&mut self) {
        self.receiver.close();
        if let Some(worker) = self._worker.as_mut() {
            worker.0.abort();
            let _ = (&mut worker.0).await;
        }
        self._worker.take();
    }
    pub async fn next_candidate(&mut self) -> Option<SocketAddrV4> {
        let peer = self.receiver.recv().await;
        if peer.is_none() {
            self.shutdown().await;
        }
        peer
    }
    pub fn try_next_candidate(&mut self) -> Result<SocketAddrV4, mpsc::error::TryRecvError> {
        self.receiver.try_recv()
    }
    pub fn start(discovery: &PeerDiscovery, options: DiscoveryOptions) -> Self {
        let (sender, receiver) = mpsc::channel(MAX_DISCOVERY_PEERS);
        Self {
            receiver,
            _worker: Some(OwnedTask(tokio::spawn(discovery(options, sender)))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn discovery_shutdown_joins_worker_before_returning() {
        check_shutdown(false).await;
    }

    #[tokio::test]
    async fn discovery_shutdown_retains_join_after_waiter_cancellation() {
        check_shutdown(true).await;
    }

    #[tokio::test]
    async fn drained_discovery_retains_owner_until_terminal_poll() {
        let discovery: PeerDiscovery = Arc::new(|_, _| Box::pin(std::future::pending()));
        let run = DiscoveryRun::start(&discovery, DiscoveryOptions::default());
        let task = run._worker.as_ref().unwrap().0.abort_handle();
        let mut run = Some(run);
        let mut candidates = SessionCandidates::default();
        drain_discovery(&mut run, &mut candidates);
        let retained = run.is_some();
        if retained {
            assert!(next_discovery_peer(&mut run).await.is_none());
            assert!(
                run.is_none(),
                "terminal polling must release the joined run"
            );
        } else {
            // The old abort-only fallback still needs to finish before this test exits.
            tokio::time::timeout(Duration::from_secs(1), async {
                while !task.is_finished() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
        }
        assert!(
            retained,
            "a closed candidate stream discarded its worker join owner"
        );
        assert!(task.is_finished());
    }

    async fn check_shutdown(cancel_waiter: bool) {
        struct Exit(Arc<AtomicU32>);
        impl Drop for Exit {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }
        let exits = Arc::new(AtomicU32::new(0));
        let entered = Arc::new(tokio::sync::Notify::new());
        let discovery: PeerDiscovery = {
            let exits = exits.clone();
            let entered = entered.clone();
            Arc::new(move |_, _sender| {
                let exit = Exit(exits.clone());
                let entered = entered.clone();
                Box::pin(async move {
                    let _exit = exit;
                    entered.notify_one();
                    std::future::pending::<()>().await;
                })
            })
        };
        let mut run = DiscoveryRun::start(&discovery, DiscoveryOptions::default());
        entered.notified().await;
        if cancel_waiter {
            let mut closing = Box::pin(run.shutdown());
            let pending = std::future::poll_fn(|cx| {
                std::task::Poll::Ready(closing.as_mut().poll(cx).is_pending())
            })
            .await;
            drop(closing);
            assert!(
                pending,
                "current-thread worker destruction must require a yield"
            );
            assert!(run._worker.is_some(), "canceled waiter lost the join owner");
        }
        run.shutdown().await;
        run.shutdown().await;
        let exits_after_shutdown = exits.load(Ordering::SeqCst);
        // Cleanup precedes the assertion even against an implementation that returns early.
        if exits_after_shutdown == 0 {
            if let Some(worker) = run._worker.as_mut() {
                worker.0.abort();
                let _ = (&mut worker.0).await;
            }
        }
        assert_eq!(
            exits_after_shutdown, 1,
            "discovery shutdown abandoned worker destruction"
        );
    }
}
