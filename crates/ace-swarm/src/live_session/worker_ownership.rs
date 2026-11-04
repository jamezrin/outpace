//! Worker admission counts active and retired task ownership, not just peer-map entries.
use super::*;

#[cfg(test)]
tokio::task_local! {
    pub(super) static WAITING_WORKER_SLOT: Arc<tokio::sync::Notify>;
}

pub(super) struct WorkerPermit(tokio::sync::OwnedSemaphorePermit);

#[derive(Clone)]
pub(super) struct WorkerAdmission(Arc<tokio::sync::Semaphore>);
impl WorkerAdmission {
    pub(super) async fn acquire(self) -> WorkerPermit {
        #[cfg(test)]
        if self.0.available_permits() == 0 {
            let _ = WAITING_WORKER_SLOT.try_with(|probe| probe.notify_one());
        }
        WorkerPermit(
            self.0
                .acquire_owned()
                .await
                .expect("worker admission is never closed"),
        )
    }
}

pub(super) struct WorkerOwnership {
    slots: Arc<tokio::sync::Semaphore>,
    active: HashMap<u64, WorkerPermit>,
    retired: Vec<(PeerRuntime, Option<WorkerPermit>)>,
}
impl WorkerOwnership {
    pub(super) fn new(capacity: usize) -> Self {
        Self {
            slots: Arc::new(tokio::sync::Semaphore::new(capacity)),
            active: HashMap::new(),
            retired: Vec::new(),
        }
    }
    pub(super) fn admission(&self) -> WorkerAdmission {
        WorkerAdmission(self.slots.clone())
    }
    pub(super) fn register(&mut self, id: u64, permit: WorkerPermit) {
        assert!(
            Arc::ptr_eq(&self.slots, permit.0.semaphore()),
            "worker permit belongs to another owner"
        );
        assert!(
            self.active.insert(id, permit).is_none(),
            "worker generation already registered"
        );
    }
    pub(super) fn retire(&mut self, id: u64, peer: PeerRuntime) {
        let _ = peer.commands.try_send(PeerCommand::Stop);
        peer.worker.abort();
        let permit = self.active.remove(&id);
        // Unregistered fake runtimes exist only in the legacy component fixtures.
        #[cfg(not(test))]
        assert!(
            permit.is_some(),
            "production worker has no ownership permit"
        );
        self.retired.push((peer, permit));
    }
    pub(super) fn has_retired(&self) -> bool {
        !self.retired.is_empty()
    }
    pub(super) fn poll_retired(&mut self, cx: &mut std::task::Context<'_>) -> std::task::Poll<()> {
        for index in 0..self.retired.len() {
            if Pin::new(&mut self.retired[index].0.worker)
                .poll(cx)
                .is_ready()
            {
                self.retired.swap_remove(index);
                return std::task::Poll::Ready(());
            }
        }
        std::task::Poll::Pending
    }
    pub(super) async fn finish_retired(&mut self) {
        while let Some((mut peer, permit)) = self.retired.pop() {
            let _ = (&mut peer.worker).await;
            drop(peer);
            drop(permit);
        }
    }
    pub(super) fn release_joined_current(&mut self) {
        self.active.clear();
    }
}

// Captured before spawn so task-local observation follows the actual worker lifetime.
// This probe adds no production state or synchronization.
#[cfg(test)]
pub(in crate::live_session) mod lifetime_probe {
    use super::*;
    #[derive(Default)]
    pub(in crate::live_session) struct Probe {
        pub active: std::sync::atomic::AtomicUsize,
        pub spawned: tokio::sync::Notify,
    }
    tokio::task_local! {
        pub(in crate::live_session) static CURRENT: Arc<Probe>;
        pub(in crate::live_session) static CONNECT: Arc<Probe>;
        pub(in crate::live_session) static PANIC_POOL: ();
    }
    pub(in crate::live_session) struct Guard(Arc<Probe>);
    pub(in crate::live_session) fn capture() -> Option<Guard> {
        CURRENT
            .try_with(|probe| {
                probe.active.fetch_add(1, Ordering::SeqCst);
                probe.spawned.notify_one();
                Guard(probe.clone())
            })
            .ok()
    }
    pub(in crate::live_session) fn capture_connect() -> Option<Guard> {
        CONNECT
            .try_with(|probe| {
                probe.active.fetch_add(1, Ordering::SeqCst);
                probe.spawned.notify_one();
                Guard(probe.clone())
            })
            .ok()
    }
    impl Drop for Guard {
        fn drop(&mut self) {
            self.0.active.fetch_sub(1, Ordering::SeqCst);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn joined_current_workers_release_capacity_before_pool_return() {
        let info = StreamInfo {
            protection: None,
            infohash: [0; 20],
            piece_length: 4,
            chunk_length: 2,
            trackers: vec![],
            metadata: Default::default(),
            sig_len: 0,
            source_pubkey: vec![],
        };
        let policy = LiveRecoveryConfig {
            max_active_upstreams: 1,
            max_parallel_connect: 1,
            ..Default::default()
        };
        let (mut continuity, _) = Continuity::fresh(&info, 1, 2, 0, policy);
        let admission = continuity.workers.admission();
        let permit = admission.clone().acquire().await;
        let held = admission.clone().acquire().await;
        continuity.workers.register(1, permit);
        let worker = tokio::spawn(std::future::pending::<()>());
        let stopped = worker.abort_handle();
        let (commands, _) = mpsc::channel(1);
        let mut peers = BTreeMap::from([(
            1,
            PeerRuntime {
                addr: "127.0.0.1:1234".parse().unwrap(),
                min_piece: 1,
                max_piece: 2,
                unchoked_peer: false,
                produced_output: false,
                seen_ids: HashSet::new(),
                commands,
                worker,
            },
        )]);
        pool_support::shutdown_current_workers(&mut peers, &mut continuity).await;
        let next = tokio::time::timeout(Duration::from_millis(100), admission.acquire()).await;
        let physically_done = stopped.is_finished();
        continuity.workers.release_joined_current();
        drop(held);
        assert!(physically_done && peers.is_empty());
        assert!(
            next.is_ok(),
            "joined current workers strand capacity while the pool continues"
        );
    }

    #[tokio::test]
    async fn retired_worker_keeps_slot_until_joined() {
        struct Dropped(Arc<std::sync::atomic::AtomicBool>);
        impl Drop for Dropped {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }
        let mut owner = WorkerOwnership::new(1);
        let permit = owner.admission().acquire().await;
        let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let started = Arc::new(tokio::sync::Notify::new());
        let worker = tokio::spawn({
            let dropped = dropped.clone();
            let started = started.clone();
            async move {
                let _guard = Dropped(dropped);
                started.notify_one();
                std::future::pending::<()>().await;
            }
        });
        started.notified().await;
        let (commands, _) = mpsc::channel(1);
        owner.register(1, permit);
        owner.retire(
            1,
            PeerRuntime {
                addr: "127.0.0.1:1234".parse().unwrap(),
                min_piece: 1,
                max_piece: 2,
                unchoked_peer: false,
                produced_output: false,
                seen_ids: HashSet::new(),
                commands,
                worker,
            },
        );
        let mut next = Box::pin(owner.admission().acquire());
        let blocked =
            std::future::poll_fn(|cx| std::task::Poll::Ready(next.as_mut().poll(cx).is_pending()))
                .await;
        let still_owned = !dropped.load(Ordering::SeqCst) && owner.retired.len() == 1;
        owner.finish_retired().await;
        let next = tokio::time::timeout(Duration::from_secs(1), next).await;
        assert!(
            blocked && still_owned,
            "retired task freed its slot before destruction/join"
        );
        assert!(dropped.load(Ordering::SeqCst));
        assert!(owner.retired.is_empty());
        drop(next.expect("joined worker releases its slot"));
    }

    #[tokio::test]
    async fn worker_budget_waits_for_owned_permit_release() {
        let owner = WorkerOwnership::new(2);
        let first = owner.admission().acquire().await;
        let second = owner.admission().acquire().await;
        let mut third = Box::pin(owner.admission().acquire());
        let pending =
            std::future::poll_fn(|cx| std::task::Poll::Ready(third.as_mut().poll(cx).is_pending()))
                .await;
        drop(first);
        let third = tokio::time::timeout(Duration::from_secs(1), third).await;
        drop(second);
        assert!(
            pending,
            "worker budget admitted a third owner before cleanup"
        );
        assert!(third.is_ok());
    }
}
