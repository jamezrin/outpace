//! Small, independently readable live metrics. No source lock is held by readers.
use crate::provider::{LivePositionStats, SourceStats, StatsReader};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::time::Instant;

const WINDOW: Duration = Duration::from_secs(5);

struct RateMeter {
    samples: VecDeque<(Instant, u64)>,
}
impl RateMeter {
    fn new(now: Instant, total: u64) -> Self {
        Self {
            samples: VecDeque::from([(now, total)]),
        }
    }
    fn observe(&mut self, now: Instant, total: u64) {
        if let Some(&(last, old)) = self.samples.back() {
            if now < last {
                return;
            }
            if total < old {
                self.samples.clear();
            } else if now == last {
                self.samples.pop_back();
            }
        }
        self.samples.push_back((now, total));
        while self.samples.len() > 6 {
            self.samples.pop_front();
        }
    }
    fn rate(&self, now: Instant, total: u64) -> u64 {
        let Some(&(last_at, last_total)) = self.samples.back() else {
            return 0;
        };
        if total < last_total || now < last_at {
            return 0;
        }
        let cutoff = now.checked_sub(WINDOW).unwrap_or(now);
        if last_at <= cutoff && total == last_total {
            return 0;
        }
        let &(at, baseline) = self
            .samples
            .iter()
            .rev()
            .find(|(at, _)| *at <= cutoff)
            .or_else(|| self.samples.front())
            .expect("meter always has a baseline");
        let elapsed = now.saturating_duration_since(at).as_secs_f64();
        if elapsed == 0.0 {
            return 0;
        }
        (total.saturating_sub(baseline) as f64 / elapsed) as u64
    }
}

struct Measurement {
    down: RateMeter,
    up: RateMeter,
    position: Option<LivePositionStats>,
    buffer_ms: u64,
}

pub(crate) struct LiveMetrics {
    downloaded: Arc<AtomicU64>,
    pub(crate) received: Arc<AtomicU64>,
    uploaded: Arc<AtomicU64>,
    peers: Arc<AtomicU32>,
    peers_served: Arc<AtomicU32>,
    measured: Mutex<Measurement>,
}
impl LiveMetrics {
    pub(crate) fn new(
        downloaded: Arc<AtomicU64>,
        received: Arc<AtomicU64>,
        uploaded: Arc<AtomicU64>,
        peers: Arc<AtomicU32>,
        peers_served: Arc<AtomicU32>,
    ) -> Arc<Self> {
        let now = Instant::now();
        let measured = Mutex::new(Measurement {
            down: RateMeter::new(now, received.load(Ordering::Relaxed)),
            up: RateMeter::new(now, uploaded.load(Ordering::Relaxed)),
            position: None,
            buffer_ms: 0,
        });
        Arc::new(Self {
            downloaded,
            received,
            uploaded,
            peers,
            peers_served,
            measured,
        })
    }
    pub(crate) fn publish_position(&self, position: Option<LivePositionStats>, buffer_ms: u64) {
        let mut measured = self.measured.lock().unwrap();
        measured.position = position;
        measured.buffer_ms = buffer_ms;
    }
    fn sample(&self) {
        let now = Instant::now();
        let mut measured = self.measured.lock().unwrap();
        measured
            .down
            .observe(now, self.received.load(Ordering::Relaxed));
        measured
            .up
            .observe(now, self.uploaded.load(Ordering::Relaxed));
    }
}
impl StatsReader for LiveMetrics {
    fn snapshot(&self) -> SourceStats {
        let now = Instant::now();
        let measured = self.measured.lock().unwrap();
        let download_rate = measured
            .down
            .rate(now, self.received.load(Ordering::Relaxed));
        SourceStats {
            peers: self.peers.load(Ordering::Relaxed),
            bitrate: download_rate.saturating_mul(8),
            buffer_ms: measured.buffer_ms,
            downloaded: self.downloaded.load(Ordering::Relaxed),
            uploaded: self.uploaded.load(Ordering::Relaxed),
            peers_served: self.peers_served.load(Ordering::Relaxed),
            download_rate,
            upload_rate: measured.up.rate(now, self.uploaded.load(Ordering::Relaxed)),
            livepos: measured.position.clone(),
        }
    }
}

pub(crate) struct MetricsSampler {
    task: tokio::task::JoinHandle<()>,
    metrics: Arc<LiveMetrics>,
}
impl Drop for MetricsSampler {
    fn drop(&mut self) {
        self.metrics.sample();
        self.task.abort();
    }
}
pub(crate) fn spawn_sampler(metrics: Arc<LiveMetrics>) -> MetricsSampler {
    let worker = metrics.clone();
    let task = tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(1));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tick.tick().await;
            worker.sample();
        }
    });
    MetricsSampler { task, metrics }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rate_measures_flow_and_expires() {
        let start = Instant::now();
        let mut meter = RateMeter::new(start, 0);
        for second in 1..=10 {
            let now = start + Duration::from_secs(second);
            meter.observe(now, second * 1024);
            assert_eq!(meter.rate(now, second * 1024), 1024);
        }
        assert_eq!(meter.rate(start + Duration::from_secs(15), 10240), 0);
    }
    #[test]
    fn rate_reads_are_pure() {
        let start = Instant::now();
        let meter = RateMeter::new(start, 0);
        let now = start + Duration::from_secs(1);
        assert_eq!(meter.rate(now, 1024), 1024);
        assert_eq!(meter.rate(now, 1024), 1024);
        assert_eq!(meter.samples.len(), 1);
    }
    #[test]
    fn rate_reset_is_not_traffic() {
        let start = Instant::now();
        let mut meter = RateMeter::new(start, 10000);
        assert_eq!(meter.rate(start + Duration::from_secs(1), 5), 0);
        meter.observe(start + Duration::from_secs(1), 5);
        assert_eq!(meter.rate(start + Duration::from_secs(2), 1029), 1024);
    }
    #[test]
    fn rate_history_is_bounded() {
        let start = Instant::now();
        let mut meter = RateMeter::new(start, 0);
        for second in 1..=10000 {
            meter.observe(start + Duration::from_secs(second), second);
            assert!(meter.samples.len() <= 6);
        }
    }
    #[test]
    fn bitrate_saturates_without_changing_totals() {
        let now = Instant::now();
        let received = Arc::new(AtomicU64::new(0));
        let metrics = LiveMetrics::new(
            Arc::new(AtomicU64::new(7)),
            received.clone(),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU32::new(0)),
            Arc::new(AtomicU32::new(0)),
        );
        received.store(u64::MAX, Ordering::Relaxed);
        // Inject the measurement epoch without sleeping.
        metrics.measured.lock().unwrap().down = RateMeter::new(now - Duration::from_secs(1), 0);
        assert_eq!(metrics.snapshot().bitrate, u64::MAX);
        assert_eq!(metrics.snapshot().downloaded, 7);
    }

    #[tokio::test(start_paused = true)]
    async fn upload_only_and_retained_reader_decay() {
        let downloaded = Arc::new(AtomicU64::new(81));
        let received = Arc::new(AtomicU64::new(0));
        let uploaded = Arc::new(AtomicU64::new(0));
        let metrics = LiveMetrics::new(
            downloaded,
            received,
            uploaded.clone(),
            Arc::new(AtomicU32::new(2)),
            Arc::new(AtomicU32::new(1)),
        );
        let sampler = spawn_sampler(metrics.clone());
        uploaded.store(1024, Ordering::Relaxed);
        tokio::time::advance(Duration::from_secs(1)).await;
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.upload_rate, 1024);
        assert_eq!(snapshot.bitrate, 0);
        assert_eq!(snapshot.downloaded, 81);
        assert_eq!(snapshot.peers, 2);
        drop(sampler);
        tokio::time::advance(Duration::from_secs(5)).await;
        assert_eq!(metrics.snapshot().upload_rate, 0);
    }
}
