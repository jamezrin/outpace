//! Process-wide bounded native hostname resolution for tracker and DHT discovery.
use std::collections::VecDeque;
use std::io;
use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::thread::JoinHandle;

const WORKERS: usize = 4;
const QUEUED: usize = 8;
const MAX_HOST: usize = 256;
const MAX_ADDRESSES: usize = 16;
type Operation = dyn Fn(&str) -> io::Result<Vec<SocketAddr>> + Send + Sync;
type Reply = tokio::sync::oneshot::Sender<io::Result<Vec<SocketAddr>>>;
struct Request {
    id: u64,
    host: String,
    reply: Reply,
}
#[derive(Default)]
struct State {
    queue: VecDeque<Request>,
    next_id: u64,
    running: usize,
    workers: usize,
    stop: bool,
    #[cfg(test)]
    peak_running: usize,
    #[cfg(test)]
    peak_queued: usize,
}
struct Shared {
    state: Mutex<State>,
    wake: Condvar,
    operation: Arc<Operation>,
}
struct Service {
    shared: Arc<Shared>,
    handles: Mutex<Vec<JoinHandle<()>>>,
}
#[derive(Clone)]
pub(crate) struct Resolver {
    service: Arc<Service>,
}

// Dropping the caller removes only still-queued work. A running syscall belongs to its
// physical worker until it returns, regardless of the async deadline or receiver lifetime.
struct QueuedCancellation {
    shared: Arc<Shared>,
    id: u64,
}
impl Drop for QueuedCancellation {
    fn drop(&mut self) {
        let mut state = self.shared.state.lock().unwrap();
        state.queue.retain(|request| request.id != self.id);
    }
}
impl Resolver {
    pub(crate) fn global() -> Self {
        static RESOLVER: OnceLock<Resolver> = OnceLock::new();
        RESOLVER
            .get_or_init(|| {
                Self::new(Arc::new(|host| {
                    host.to_socket_addrs()
                        .map(|addresses| addresses.take(MAX_ADDRESSES).collect())
                }))
            })
            .clone()
    }
    fn new(operation: Arc<Operation>) -> Self {
        Self::with_spawner(operation, |task| {
            std::thread::Builder::new()
                .name("discovery-resolver".into())
                .spawn(task)
        })
    }
    fn with_spawner(
        operation: Arc<Operation>,
        spawn: impl Fn(Box<dyn FnOnce() + Send>) -> io::Result<JoinHandle<()>>,
    ) -> Self {
        let shared = Arc::new(Shared {
            state: Mutex::new(State::default()),
            wake: Condvar::new(),
            operation,
        });
        let mut handles = Vec::with_capacity(WORKERS);
        for _ in 0..WORKERS {
            let work = shared.clone();
            if let Ok(handle) = spawn(Box::new(move || worker(work))) {
                handles.push(handle);
            }
        }
        shared.state.lock().unwrap().workers = handles.len();
        Self {
            service: Arc::new(Service {
                shared,
                handles: Mutex::new(handles),
            }),
        }
    }
    pub(crate) async fn lookup(&self, host: &str) -> io::Result<Vec<SocketAddr>> {
        if host.len() > MAX_HOST {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "resolver hostname too long",
            ));
        }
        if let Ok(address) = host.parse() {
            return Ok(vec![address]);
        }
        let (reply, receive) = tokio::sync::oneshot::channel();
        let shared = &self.service.shared;
        let id = {
            let mut state = shared.state.lock().unwrap();
            state.queue.retain(|request| !request.reply.is_closed());
            if state.stop || state.workers == 0 || state.queue.len() >= QUEUED {
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "discovery resolver unavailable or overloaded",
                ));
            }
            let id = state.next_id;
            state.next_id = state.next_id.wrapping_add(1);
            state.queue.push_back(Request {
                id,
                host: host.to_owned(),
                reply,
            });
            #[cfg(test)]
            {
                state.peak_queued = state.peak_queued.max(state.queue.len());
            }
            id
        };
        let _cancel = QueuedCancellation {
            shared: shared.clone(),
            id,
        };
        shared.wake.notify_one();
        receive
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::Interrupted, "discovery resolver stopped"))?
    }
    #[cfg(test)]
    pub(crate) fn controlled(
        operation: impl Fn(&str) -> io::Result<Vec<SocketAddr>> + Send + Sync + 'static,
    ) -> Self {
        Self::new(Arc::new(operation))
    }
    #[cfg(test)]
    pub(crate) async fn finish_controlled(&self) {
        {
            let mut state = self.service.shared.state.lock().unwrap();
            state.stop = true;
            state.queue.clear();
        }
        self.service.shared.wake.notify_all();
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                if self
                    .service
                    .handles
                    .lock()
                    .unwrap()
                    .iter()
                    .all(JoinHandle::is_finished)
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("released controlled resolver workers must become terminal");
        for handle in self.service.handles.lock().unwrap().drain(..) {
            handle.join().unwrap();
        }
    }
}
fn worker(shared: Arc<Shared>) {
    loop {
        let request = {
            let mut state = shared.state.lock().unwrap();
            loop {
                if state.stop {
                    return;
                }
                if let Some(request) = state.queue.pop_front() {
                    if request.reply.is_closed() {
                        continue;
                    }
                    state.running += 1;
                    #[cfg(test)]
                    {
                        state.peak_running = state.peak_running.max(state.running);
                    }
                    break request;
                }
                state = shared.wake.wait(state).unwrap();
            }
        };
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            (shared.operation)(&request.host)
        }))
        .unwrap_or_else(|_| Err(io::Error::other("native resolver panicked")))
        .map(|mut addresses| {
            addresses.truncate(MAX_ADDRESSES);
            addresses
        });
        shared.state.lock().unwrap().running -= 1;
        let _ = request.reply.send(result);
    }
}
// The global service owns its fixed handles until process termination. It never joins a
// potentially stalled NSS call during async stream/runtime shutdown, and never replaces one.
impl Drop for Service {
    fn drop(&mut self) {
        self.shared.state.lock().unwrap().stop = true;
        self.shared.wake.notify_all();
        let handles = self.handles.get_mut().unwrap();
        for index in (0..handles.len()).rev() {
            if handles[index].is_finished() {
                let _ = handles.swap_remove(index).join();
            }
        }
        // Production's OnceLock owner lives for the process. Controlled instances must call
        // finish_controlled after releasing their native operations to collect real terminals.
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Condvar, Mutex};
    use std::time::Duration;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancelled_tracker_and_dht_generations_keep_native_resolver_capacity() {
        let gate = Arc::new((Mutex::new(false), Condvar::new()));
        let started = Arc::new(AtomicUsize::new(0));
        let completed = Arc::new(AtomicUsize::new(0));
        let resolver = Resolver::controlled({
            let gate = gate.clone();
            let started = started.clone();
            let completed = completed.clone();
            move |_| {
                started.fetch_add(1, Ordering::SeqCst);
                let mut released = gate.0.lock().unwrap();
                while !*released {
                    released = gate.1.wait(released).unwrap();
                }
                completed.fetch_add(1, Ordering::SeqCst);
                Ok(vec!["127.0.0.1:1".parse().unwrap()])
            }
        });
        // Successive short-lived caller generations share the same actual resolver boundary.
        // Half use tracker policy resolution and half the production DHT bootstrap helper.
        for _ in 0..3 {
            let mut callers = tokio::task::JoinSet::new();
            for i in 0..8 {
                let resolver = resolver.clone();
                callers.spawn(async move {
                    let deadline = tokio::time::Instant::now() + Duration::from_millis(50);
                    if i % 2 == 0 {
                        let urls = vec!["udp://controlled.invalid:1".to_owned()];
                        let _ = tokio::time::timeout_at(
                            deadline,
                            crate::discover::resolve_trackers_with_resolver(
                                &urls,
                                crate::discover::TrackerPolicy {
                                    allow_non_global: true,
                                },
                                &resolver,
                            ),
                        )
                        .await;
                    } else {
                        crate::dht::resolve_bootstrap(
                            "controlled.invalid:1".into(),
                            resolver,
                            deadline,
                        )
                        .await;
                    }
                });
            }
            while let Some(result) = callers.join_next().await {
                result.unwrap();
            }
        }
        let still_owned = started.load(Ordering::SeqCst);
        let no_native_completion = completed.load(Ordering::SeqCst) == 0;
        // Literal endpoints remain usable even with every native operation held.
        let literal = resolver.lookup("127.0.0.1:9").await.unwrap();
        *gate.0.lock().unwrap() = true;
        gate.1.notify_all();
        tokio::time::timeout(Duration::from_secs(2), async {
            while completed.load(Ordering::SeqCst) < still_owned {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        resolver.finish_controlled().await;
        assert!(
            no_native_completion,
            "controlled native operation escaped its hold"
        );
        assert_eq!(literal, vec!["127.0.0.1:9".parse::<SocketAddr>().unwrap()]);
        assert!(still_owned <= 4, "cancelled generations started {still_owned} native resolver operations while prior calls still owned capacity");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn physical_worker_queue_overload_cancellation_and_recovery_are_bounded() {
        let gate = Arc::new((Mutex::new(false), Condvar::new()));
        let started = Arc::new(AtomicUsize::new(0));
        let resolver = Resolver::controlled({
            let gate = gate.clone();
            let started = started.clone();
            move |_| {
                started.fetch_add(1, Ordering::SeqCst);
                let mut released = gate.0.lock().unwrap();
                while !*released {
                    released = gate.1.wait(released).unwrap();
                }
                Ok(vec!["127.0.0.1:7".parse().unwrap()])
            }
        });
        let mut active = Vec::new();
        for _ in 0..WORKERS {
            let resolver = resolver.clone();
            active.push(tokio::spawn(async move {
                resolver.lookup("held.invalid:7").await
            }));
        }
        tokio::time::timeout(Duration::from_secs(2), async {
            while started.load(Ordering::SeqCst) != WORKERS {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let mut queued = Vec::new();
        for _ in 0..QUEUED {
            let resolver = resolver.clone();
            queued.push(tokio::spawn(async move {
                resolver.lookup("queued.invalid:7").await
            }));
        }
        tokio::time::timeout(Duration::from_secs(2), async {
            while resolver.service.shared.state.lock().unwrap().queue.len() != QUEUED {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let overloaded = resolver
            .lookup("overload.invalid:7")
            .await
            .unwrap_err()
            .kind();
        for _ in 0..2 {
            let call = queued.pop().unwrap();
            call.abort();
            let _ = call.await;
        }
        let queued_after_cancel = resolver.service.shared.state.lock().unwrap().queue.len();
        for call in active {
            call.abort();
            let _ = call.await;
        }
        let executing_after_cancel = resolver.service.shared.state.lock().unwrap().running;
        let no_replacement = started.load(Ordering::SeqCst);
        *gate.0.lock().unwrap() = true;
        gate.1.notify_all();
        for call in queued {
            assert!(call.await.unwrap().is_ok());
        }
        let recovered = resolver.lookup("recovered.invalid:7").await.unwrap();
        let (peak_running, peak_queued) = {
            let state = resolver.service.shared.state.lock().unwrap();
            (state.peak_running, state.peak_queued)
        };
        resolver.finish_controlled().await;
        assert_eq!(overloaded, io::ErrorKind::WouldBlock);
        assert_eq!(queued_after_cancel, QUEUED - 2);
        assert_eq!(executing_after_cancel, WORKERS);
        assert_eq!(no_replacement, WORKERS);
        assert_eq!(peak_running, WORKERS);
        assert_eq!(peak_queued, QUEUED);
        assert_eq!(
            recovered,
            vec!["127.0.0.1:7".parse::<SocketAddr>().unwrap()]
        );
    }

    #[tokio::test]
    async fn reduced_initialization_failure_error_and_panic_preserve_owned_workers() {
        for available in [0, 2] {
            let starts = AtomicUsize::new(0);
            let resolver = Resolver::with_spawner(
                Arc::new(|host| {
                    if host.starts_with("panic.") {
                        panic!("controlled native panic");
                    }
                    if host.starts_with("error.") {
                        return Err(io::Error::new(io::ErrorKind::NotFound, "controlled error"));
                    }
                    Ok(vec!["127.0.0.1:7".parse().unwrap(); 32])
                }),
                |task| {
                    if starts.fetch_add(1, Ordering::SeqCst) >= available {
                        return Err(io::Error::other("controlled spawn failure"));
                    }
                    std::thread::Builder::new().spawn(task)
                },
            );
            assert_eq!(resolver.service.handles.lock().unwrap().len(), available);
            let literal = resolver.lookup("127.0.0.1:7").await.unwrap();
            assert_eq!(literal.len(), 1);
            let result = resolver.lookup("normal.invalid:7").await;
            if available == 0 {
                assert_eq!(result.unwrap_err().kind(), io::ErrorKind::WouldBlock);
            } else {
                assert_eq!(result.unwrap().len(), MAX_ADDRESSES);
                assert!(resolver.lookup("panic.invalid:7").await.is_err());
                assert!(resolver.lookup("error.invalid:7").await.is_err());
                assert!(resolver.lookup("recovered.invalid:7").await.is_ok());
                assert_eq!(resolver.service.handles.lock().unwrap().len(), available);
            }
            resolver.finish_controlled().await;
        }
    }

    #[tokio::test]
    async fn normal_hostname_and_literal_resolution_positive_control() {
        let resolver = Resolver::global();
        let addresses = resolver.lookup("localhost:9").await.unwrap();
        assert!(addresses
            .iter()
            .any(|address| address.ip().is_loopback() && address.port() == 9));
        assert_eq!(
            resolver.lookup("127.0.0.1:9").await.unwrap(),
            vec!["127.0.0.1:9".parse::<SocketAddr>().unwrap()]
        );
    }
}
