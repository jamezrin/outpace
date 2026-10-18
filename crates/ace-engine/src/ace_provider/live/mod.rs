use super::*;
use ace_swarm::live_session::{follow_live, LiveCounters, LiveSeedOptions, LiveSessionInputs};

pub(super) struct AceSource {
    pub(super) metrics: Option<Arc<crate::live_stats::LiveMetrics>>,
    pub(super) _sampler: Option<crate::live_stats::MetricsSampler>,
    pub(super) rx: mpsc::Receiver<LiveOutput>,
    pub(super) discontinuity: bool,
    pub(super) peers: Arc<AtomicU32>,
    pub(super) downloaded: Arc<AtomicU64>,
    pub(super) uploaded: Arc<AtomicU64>,
    pub(super) peers_served: Arc<AtomicU32>,
    pub(super) metadata: StreamMetadata,
}

#[async_trait]
impl TsSource for AceSource {
    async fn next(&mut self) -> Option<Bytes> {
        let output = self.rx.recv().await?;
        self.discontinuity = output.discontinuity;
        Some(output.bytes)
    }
    fn take_discontinuity(&mut self) -> bool {
        std::mem::take(&mut self.discontinuity)
    }
    fn stats(&self) -> SourceStats {
        if let Some(metrics) = &self.metrics {
            return crate::provider::StatsReader::snapshot(metrics.as_ref());
        }
        SourceStats {
            peers: self.peers.load(Ordering::Relaxed),
            bitrate: 0,
            buffer_ms: 0,
            downloaded: self.downloaded.load(Ordering::Relaxed),
            uploaded: self.uploaded.load(Ordering::Relaxed),
            peers_served: self.peers_served.load(Ordering::Relaxed),
            ..SourceStats::default()
        }
    }
    fn stats_handle(&self) -> Option<crate::provider::StatsHandle> {
        self.metrics
            .as_ref()
            .map(|metrics| metrics.clone() as crate::provider::StatsHandle)
    }
    fn metadata(&self) -> StreamMetadata {
        self.metadata.clone()
    }
}
impl AceProvider {
    // Shared resolved-live entry point; injected discovery keeps protocol regressions offline.
    pub(super) async fn open_resolved(
        &self,
        id: &str,
        info: StreamInfo,
        discovery: PeerDiscovery,
    ) -> Result<Box<dyn TsSource>, ProviderError> {
        let resolved_at = Instant::now();
        // Bootstrap peers are the proven/direct path and must be tried without waiting for
        // tracker/DHT discovery. Background refill can still discover more peers after start.
        let mut initial_run = None;
        let peers = if self.bootstrap_peers.is_empty() {
            let mut run = DiscoveryRun::start(&discovery, DiscoveryOptions::default());
            self.warm_peers.initialized().await;
            let hints = self.warm_peers.candidate_addresses(&info.infohash);
            let mut peers: Vec<_> = hints.into_iter().collect();
            if peers.is_empty() {
                peers.extend(run.next_candidate().await);
            }
            while let Ok(peer) = run.try_next_candidate() {
                peers.push(peer);
            }
            if !peers.is_empty() {
                initial_run = Some(run);
            }
            peers
        } else {
            self.bootstrap_peers.clone()
        };
        crate::alog!(
            "[ace] discovery stage=first-candidates after_resolution_ms={} count={}",
            resolved_at.elapsed().as_millis(),
            peers.len()
        );
        crate::alog!("[ace] open {id}: discovered {} peer(s)", peers.len());
        if peers.is_empty() {
            return Err(ProviderError::Backend(
                "no peers (no trackers/bootstrap)".into(),
            ));
        }

        let (tx, rx) = mpsc::channel::<LiveOutput>(256);
        let peer_count = Arc::new(AtomicU32::new(0));
        let downloaded = Arc::new(AtomicU64::new(0));
        let uploaded = Arc::new(AtomicU64::new(0));
        let peers_served = Arc::new(AtomicU32::new(0));
        let identity = self.identity.clone();
        let stats_peers = peer_count.clone();
        let stats_downloaded = downloaded.clone();
        let stats_uploaded = uploaded.clone();
        let stats_peers_served = peers_served.clone();
        let metrics = crate::live_stats::LiveMetrics::new(
            stats_downloaded.clone(),
            Arc::new(AtomicU64::new(0)),
            stats_uploaded.clone(),
            stats_peers.clone(),
            stats_peers_served.clone(),
        );
        let sampler = crate::live_stats::spawn_sampler(metrics.clone());
        let prefetch_pieces = self.prefetch_policy_for(&info);
        let startup_buffer = self.startup_buffer;
        let bitrate = info.metadata.bitrate;
        let seed = LiveSeedOptions {
            metrics: Some(metrics.clone()),
            registry: self.seed_registry.clone(),
            store_factory: self.store_factory(&info),
            enabled: self.enable_seeding,
            prefetch_pieces,
            live_recovery: self.live_recovery.into(),
            warm_peers: self.warm_peers.clone(),
        };
        let announce_info = info.clone();
        let metadata = info.metadata.clone();
        let announce_port = self.announce_peer_port.clone();
        let reachability = self.reachability.clone();
        tokio::spawn(async move {
            // Run the download loop and the periodic seeder self-announce concurrently;
            // whichever ends first (normally `follow_live`, when the consumer drops) tears
            // down the other — no separate lifecycle to manage.
            tokio::select! {
                _ = follow_live(LiveSessionInputs { info, peers, identity, output: tx, counters: LiveCounters { peers: stats_peers, downloaded, uploaded, peers_served }, seed, discovery, reachability, initial_run }) => {},
                _ = announce_seeder_periodically(announce_info, announce_port) => {},
            }
        });
        let source = AceSource {
            metrics: Some(metrics),
            _sampler: Some(sampler),
            rx,
            discontinuity: false,
            peers: peer_count,
            downloaded: stats_downloaded,
            uploaded: stats_uploaded,
            peers_served: stats_peers_served,
            metadata,
        };
        Ok(StartupBufferedSource::new(
            Box::new(source),
            startup_buffer,
            bitrate,
        ))
    }
}
