use crate::session::Subscription;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const ACE_TOKEN_TTL: Duration = Duration::from_secs(6 * 60 * 60);
const ACE_TOKEN_CAPACITY: usize = 4096;

pub(super) struct AceLease {
    playback_id: String,
    session_key: String,
    expires_at: Instant,
    cancel: tokio::sync::watch::Sender<bool>,
    /// An HLS client has no long-lived response body to own its subscriber count. Keep one
    /// receiver in the bounded lease instead, so the manager reaper sees the client until stop,
    /// expiry, or capacity eviction. The receiver is never read; the packager uses its own raw
    /// receiver and remains the sole segmenting pipeline.
    _hls_pin: Option<Subscription>,
}

/// Owns a hidden direct-playback lease for exactly as long as its HTTP body exists.
pub(super) struct AceDirectLeaseGuard {
    pub(super) sessions: Arc<AceSessionStore>,
    pub(super) playback_id: String,
    pub(super) token: String,
}

impl Drop for AceDirectLeaseGuard {
    fn drop(&mut self) {
        self.sessions.remove_owned(&self.playback_id, &self.token);
    }
}

/// Bounded playback leases minted by `/ace/getstream`. A token is both authorization and the
/// compatibility client's ownership handle; revoking one never force-stops the shared source.
pub struct AceSessionStore {
    pub(super) leases: Mutex<HashMap<String, AceLease>>,
    ttl: Duration,
    capacity: usize,
    /// A single weak-reference reaper handles every HLS deadline for this store. Mutations bump
    /// the watch generation so capacity eviction, stop, and revoke cancel its current sleep and
    /// make it recalculate the next live deadline.
    hls_reaper_signal: tokio::sync::watch::Sender<u64>,
    hls_reaper_started: AtomicBool,
    hls_reaper_workers: Arc<AtomicUsize>,
}

struct HlsReaperGuard(Arc<AtomicUsize>);

impl Drop for HlsReaperGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

impl Default for AceSessionStore {
    fn default() -> Self {
        Self::new(ACE_TOKEN_TTL, ACE_TOKEN_CAPACITY)
    }
}

impl AceSessionStore {
    pub(super) fn new(ttl: Duration, capacity: usize) -> Self {
        let (hls_reaper_signal, _) = tokio::sync::watch::channel(0);
        Self {
            leases: Mutex::new(HashMap::new()),
            ttl,
            capacity,
            hls_reaper_signal,
            hls_reaper_started: AtomicBool::new(false),
            hls_reaper_workers: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub(super) fn mint(&self, playback_id: String, session_key: String) -> String {
        use rand::TryRng;
        let mut bytes = [0_u8; 32];
        rand::rngs::SysRng
            .try_fill_bytes(&mut bytes)
            .expect("OS randomness for playback token");
        let token = hex::encode(bytes);
        self.insert_at(token.clone(), playback_id, session_key, Instant::now());
        token
    }

    pub(super) fn mint_hls(
        self: &Arc<Self>,
        playback_id: String,
        session_key: String,
        pin: Subscription,
    ) -> String {
        use rand::TryRng;
        let mut bytes = [0_u8; 32];
        rand::rngs::SysRng
            .try_fill_bytes(&mut bytes)
            .expect("OS randomness for playback token");
        let token = hex::encode(bytes);
        self.insert_hls_at(token.clone(), playback_id, session_key, Instant::now(), pin);
        self.ensure_hls_reaper();
        self.wake_hls_reaper();
        token
    }

    fn ensure_hls_reaper(self: &Arc<Self>) {
        if self
            .hls_reaper_started
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return;
        }
        let weak_store = Arc::downgrade(self);
        let mut signal = self.hls_reaper_signal.subscribe();
        let workers = self.hls_reaper_workers.clone();
        workers.fetch_add(1, Ordering::SeqCst);
        tokio::spawn(async move {
            let _guard = HlsReaperGuard(workers);
            loop {
                let Some(store) = weak_store.upgrade() else {
                    break;
                };
                let deadline = store.next_hls_expiry();
                drop(store);

                match deadline {
                    Some(deadline) => {
                        tokio::select! {
                            _ = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
                                if let Some(store) = weak_store.upgrade() {
                                    store.purge_expired(Instant::now());
                                } else {
                                    break;
                                }
                            }
                            changed = signal.changed() => {
                                if changed.is_err() {
                                    break;
                                }
                            }
                        }
                    }
                    None => {
                        if signal.changed().await.is_err() {
                            break;
                        }
                    }
                }
            }
        });
    }

    fn next_hls_expiry(&self) -> Option<Instant> {
        self.leases
            .lock()
            .unwrap()
            .values()
            .filter(|lease| lease._hls_pin.is_some())
            .map(|lease| lease.expires_at)
            .min()
    }

    fn wake_hls_reaper(&self) {
        self.hls_reaper_signal
            .send_modify(|generation| *generation = generation.wrapping_add(1));
    }

    fn purge_expired(&self, now: Instant) {
        let mut leases = self.leases.lock().unwrap();
        leases.retain(|_, lease| {
            let keep = lease.expires_at > now;
            if !keep {
                let _ = lease.cancel.send(true);
            }
            keep
        });
    }

    pub(super) fn insert_at(
        &self,
        token: String,
        playback_id: String,
        session_key: String,
        now: Instant,
    ) {
        self.insert_with_pin(token, playback_id, session_key, now, None);
    }

    fn insert_with_pin(
        &self,
        token: String,
        playback_id: String,
        session_key: String,
        now: Instant,
        hls_pin: Option<Subscription>,
    ) {
        let mut hls_schedule_changed = hls_pin.is_some();
        self.purge_expired(now);
        let mut leases = self.leases.lock().unwrap();
        if leases.len() >= self.capacity {
            if let Some(oldest) = leases
                .iter()
                .min_by_key(|(_, lease)| lease.expires_at)
                .map(|(t, _)| t.clone())
            {
                if let Some(lease) = leases.remove(&oldest) {
                    hls_schedule_changed |= lease._hls_pin.is_some();
                    let _ = lease.cancel.send(true);
                }
            }
        }
        let (cancel, _) = tokio::sync::watch::channel(false);
        leases.insert(
            token,
            AceLease {
                playback_id,
                session_key,
                expires_at: now + self.ttl,
                cancel,
                _hls_pin: hls_pin,
            },
        );
        drop(leases);
        if hls_schedule_changed {
            self.wake_hls_reaper();
        }
    }

    pub(super) fn insert_hls_at(
        &self,
        token: String,
        playback_id: String,
        session_key: String,
        now: Instant,
        pin: Subscription,
    ) {
        self.insert_with_pin(token, playback_id, session_key, now, Some(pin));
    }

    pub(super) fn validate(&self, playback_id: &str, token: &str) -> Option<String> {
        self.validate_at(playback_id, token, Instant::now())
    }

    pub(super) fn validate_token(&self, token: &str) -> Option<(String, String)> {
        let now = Instant::now();
        self.purge_expired(now);
        self.leases
            .lock()
            .unwrap()
            .get(token)
            .map(|lease| (lease.playback_id.clone(), lease.session_key.clone()))
    }

    pub(super) fn validate_at(
        &self,
        playback_id: &str,
        token: &str,
        now: Instant,
    ) -> Option<String> {
        self.purge_expired(now);
        self.leases
            .lock()
            .unwrap()
            .get(token)
            .filter(|lease| lease.playback_id == playback_id)
            .map(|lease| lease.session_key.clone())
    }

    pub(super) fn revoke(&self, playback_id: &str, token: &str) -> bool {
        let mut leases = self.leases.lock().unwrap();
        let matches = leases.get(token).is_some_and(|lease| {
            lease.playback_id == playback_id && lease.expires_at > Instant::now()
        });
        if matches {
            if let Some(lease) = leases.remove(token) {
                let _ = lease.cancel.send(true);
            }
        }
        drop(leases);
        if matches {
            self.wake_hls_reaper();
        }
        matches
    }

    /// Remove a lease owned by an internal body guard even if its authorization deadline has
    /// just passed. Unlike `revoke`, this is not an authorization check: the guard was created
    /// with the exact id/token pair when the lease was minted and must always clean up its entry.
    fn remove_owned(&self, playback_id: &str, token: &str) -> bool {
        let mut leases = self.leases.lock().unwrap();
        let matches = leases
            .get(token)
            .is_some_and(|lease| lease.playback_id == playback_id);
        if matches {
            if let Some(lease) = leases.remove(token) {
                let _ = lease.cancel.send(true);
            }
        }
        drop(leases);
        if matches {
            self.wake_hls_reaper();
        }
        matches
    }

    pub(super) fn playback(
        &self,
        playback_id: &str,
        token: &str,
    ) -> Option<(String, Instant, tokio::sync::watch::Receiver<bool>)> {
        let now = Instant::now();
        self.purge_expired(now);
        self.leases
            .lock()
            .unwrap()
            .get(token)
            .filter(|lease| lease.playback_id == playback_id)
            .map(|lease| {
                (
                    lease.session_key.clone(),
                    lease.expires_at,
                    lease.cancel.subscribe(),
                )
            })
    }

    #[cfg(test)]
    pub(super) fn active_count(&self) -> usize {
        self.leases.lock().unwrap().len()
    }

    #[cfg(test)]
    pub(super) fn hls_reaper_workers(&self) -> Arc<AtomicUsize> {
        self.hls_reaper_workers.clone()
    }
}
