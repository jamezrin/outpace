//! Daemon configuration and persistent node identity.

use ace_wire::identity::Identity;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

pub const MAX_LIVE_RECOVERY_ACTIVE_UPSTREAMS: usize = 256;
pub const MAX_LIVE_RECOVERY_PARALLEL_CONNECT: usize = 1024;
pub const MAX_LIVE_RECOVERY_PIECE_ADVANCE: u64 = 16_384;
pub const MAX_LIVE_RECOVERY_REASM_PIECES_AHEAD: u64 = 65_536;

/// Absolute ceiling for one configured in-memory retention pool. The address-space-relative
/// limit below is stricter on 32-bit targets; this ceiling keeps a single pool bounded on 64-bit
/// targets where `isize::MAX` is far larger than a practical allocation.
const MAX_CONFIGURED_MEMORY_BYTES: u64 = 8 * 1024 * 1024 * 1024;

/// Reserve three quarters of the largest representable object for the runtime, allocator,
/// indexes, connection buffers, and other active sessions. This is a per-pool guard, not a claim
/// that several pools can all grow to the returned limit simultaneously.
pub(crate) fn safe_in_memory_pool_limit(max_object_bytes: u64) -> u64 {
    (max_object_bytes / 4).min(MAX_CONFIGURED_MEMORY_BYTES)
}

/// Where the seed store (`PieceStore`) keeps piece data. Mirrors Acestream's
/// `--live-cache-type`. The disk backend trades RAM for capacity; both honor the same
/// `seed_store_bytes` budget.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum CacheType {
    /// Keep piece data in RAM (default).
    #[default]
    Memory,
    /// Spill piece data to disk under `cache_dir`.
    Disk,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct LiveRecoveryConfig {
    pub request_timeout_ms: u64,
    pub stale_upstream_timeout_ms: u64,
    pub request_check_interval_ms: u64,
    pub max_active_upstreams: usize,
    pub max_parallel_connect: usize,
    pub max_piece_advance: u64,
    pub max_reasm_pieces_ahead: u64,
}

impl Default for LiveRecoveryConfig {
    fn default() -> Self {
        Self {
            // A live follower rides the live edge, so this timer is the only thing between a
            // silent upstream and a visible playback gap: the player drains in realtime while we
            // wait one out. At 4s a single stuck piece outlived the retransmit path in practice
            // and playback instead waited out `stale_upstream_timeout_ms`, tearing the pool down
            // for a ~12s output gap where the real engine on the same swarm had none. 1500ms
            // re-requests the piece to a peer with spare capacity while the pool stays up, and
            // still leaves several retry rounds inside the stale budget.
            request_timeout_ms: 1500,
            stale_upstream_timeout_ms: 12000,
            request_check_interval_ms: 1000,
            max_active_upstreams: 4,
            max_parallel_connect: 12,
            max_piece_advance: 256,
            max_reasm_pieces_ahead: 512,
        }
    }
}

impl LiveRecoveryConfig {
    pub fn request_timeout(&self) -> Duration {
        Duration::from_millis(self.request_timeout_ms)
    }

    pub fn stale_upstream_timeout(&self) -> Duration {
        Duration::from_millis(self.stale_upstream_timeout_ms)
    }

    pub fn request_check_interval(&self) -> Duration {
        Duration::from_millis(self.request_check_interval_ms)
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.request_timeout_ms == 0 {
            return Err("OUTPACE_REQUEST_TIMEOUT_MS must be >= 1".into());
        }
        if self.stale_upstream_timeout_ms == 0 {
            return Err("OUTPACE_STALE_UPSTREAM_TIMEOUT_MS must be >= 1".into());
        }
        if self.request_check_interval_ms == 0 {
            return Err("OUTPACE_REQUEST_CHECK_INTERVAL_MS must be >= 1".into());
        }
        if self.request_timeout_ms >= self.stale_upstream_timeout_ms {
            return Err(
                "OUTPACE_REQUEST_TIMEOUT_MS must be < OUTPACE_STALE_UPSTREAM_TIMEOUT_MS".into(),
            );
        }
        if self.request_check_interval_ms > self.request_timeout_ms {
            return Err(
                "OUTPACE_REQUEST_CHECK_INTERVAL_MS must be <= OUTPACE_REQUEST_TIMEOUT_MS".into(),
            );
        }
        if self.max_active_upstreams == 0 {
            return Err("OUTPACE_MAX_ACTIVE_UPSTREAMS must be >= 1".into());
        }
        if self.max_parallel_connect == 0 {
            return Err("OUTPACE_MAX_PARALLEL_CONNECT must be >= 1".into());
        }
        if self.max_piece_advance == 0 {
            return Err("OUTPACE_MAX_PIECE_ADVANCE must be >= 1".into());
        }
        if self.max_active_upstreams > MAX_LIVE_RECOVERY_ACTIVE_UPSTREAMS {
            return Err(format!(
                "OUTPACE_MAX_ACTIVE_UPSTREAMS must be <= {MAX_LIVE_RECOVERY_ACTIVE_UPSTREAMS}"
            ));
        }
        if self.max_parallel_connect > MAX_LIVE_RECOVERY_PARALLEL_CONNECT {
            return Err(format!(
                "OUTPACE_MAX_PARALLEL_CONNECT must be <= {MAX_LIVE_RECOVERY_PARALLEL_CONNECT}"
            ));
        }
        if self.max_piece_advance > MAX_LIVE_RECOVERY_PIECE_ADVANCE {
            return Err(format!(
                "OUTPACE_MAX_PIECE_ADVANCE must be <= {MAX_LIVE_RECOVERY_PIECE_ADVANCE}"
            ));
        }
        if self.max_reasm_pieces_ahead > MAX_LIVE_RECOVERY_REASM_PIECES_AHEAD {
            return Err(format!(
                "OUTPACE_MAX_REASM_PIECES_AHEAD must be <= {MAX_LIVE_RECOVERY_REASM_PIECES_AHEAD}"
            ));
        }
        if self.max_reasm_pieces_ahead < self.max_piece_advance {
            return Err(
                "OUTPACE_MAX_REASM_PIECES_AHEAD must be >= OUTPACE_MAX_PIECE_ADVANCE".into(),
            );
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct HlsConfig {
    pub segment_packets: usize,
    pub window_segments: usize,
    pub segment_duration_ms: u64,
}

impl Default for HlsConfig {
    fn default() -> Self {
        Self {
            // ~12.3 MB per segment. The ceiling is a memory-safety bound, not the primary cut
            // mechanism (segments normally cut on a keyframe once the target duration elapses).
            // It must comfortably hold one target-duration segment of a peaky high-bitrate
            // stream -- e.g. 2160p HEVC in a high-motion scene can burst well past its ~3 MB
            // average -- so a keyframe-aligned cut lands before the ceiling in the common case.
            // Worst-case retained memory is bounded at (window + 1) * this (~86 MB here).
            segment_packets: 65_536,
            window_segments: 6,
            segment_duration_ms: 1000,
        }
    }
}

impl HlsConfig {
    pub fn segment_duration_secs(&self) -> f32 {
        self.segment_duration_ms as f32 / 1000.0
    }

    pub fn validate(&self) -> Result<(), String> {
        self.validate_for_object_limit(isize::MAX as u64)
    }

    pub(crate) fn validate_for_object_limit(&self, max_object_bytes: u64) -> Result<(), String> {
        if self.segment_packets < 3 {
            return Err("OUTPACE_HLS_SEGMENT_PACKETS must be >= 3".into());
        }
        if self.window_segments == 0 {
            return Err("OUTPACE_HLS_WINDOW_SEGMENTS must be >= 1".into());
        }
        if self.segment_duration_ms == 0 {
            return Err("OUTPACE_HLS_SEGMENT_DURATION_MS must be >= 1".into());
        }

        let segment_bytes = u64::try_from(self.segment_packets)
            .ok()
            .and_then(|packets| packets.checked_mul(188))
            .ok_or("OUTPACE_HLS_SEGMENT_PACKETS byte count overflows on this target")?;
        if segment_bytes > max_object_bytes {
            return Err(format!(
                "OUTPACE_HLS_SEGMENT_PACKETS requires a {segment_bytes}-byte Vec, exceeding this target's {max_object_bytes}-byte object limit"
            ));
        }

        // The live packager can retain `window_segments` completed segments plus its current
        // segment. Bound their payload bytes as a unit; VecDeque/Bytes metadata and the rest of
        // the daemon consume additional memory, which is why the pool limit is only one quarter
        // of the target's maximum object size (and capped at 8 GiB on 64-bit).
        let retained_segments = u64::try_from(self.window_segments)
            .ok()
            .and_then(|window| window.checked_add(1))
            .ok_or("OUTPACE_HLS_WINDOW_SEGMENTS overflows on this target")?;
        let retained_bytes = segment_bytes
            .checked_mul(retained_segments)
            .ok_or("configured HLS retained byte count overflows")?;
        let safe_limit = safe_in_memory_pool_limit(max_object_bytes);
        if retained_bytes > safe_limit {
            return Err(format!(
                "configured HLS window may retain {retained_bytes} bytes, exceeding this target's conservative {safe_limit}-byte in-memory pool limit"
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct Config {
    /// Address the HTTP API binds to.
    pub bind: SocketAddr,
    /// Address the RTMP ingest listener binds to.
    pub rtmp_bind: SocketAddr,
    /// Directory for the persistent identity seed and any caches.
    pub data_dir: PathBuf,
    /// Networks (providers) to enable.
    pub networks: Vec<String>,
    /// Address the peer-protocol listener binds to (inbound seeding). Started by default —
    /// see `enable_inbound`.
    pub peer_listen: SocketAddr,
    /// Byte ceiling for an infohash's shared reseed store (hard safety cap).
    pub seed_store_bytes: u64,
    /// Age bound (seconds) for a live reseed store: retain roughly this much recent downloaded
    /// data for reseeding instead of filling `seed_store_bytes`, so RAM tracks bitrate rather than
    /// always growing to the byte cap. `0` disables the age bound (byte-only, the pre-0.2 behavior).
    /// VOD stores are always byte-only. `OUTPACE_SEED_RETENTION_SECS`.
    pub seed_retention_secs: u64,
    /// Backend the seed store uses for piece data (`memory` | `disk`).
    pub cache_type: CacheType,
    /// Root directory for disk-mode piece files (one subdirectory per infohash). Only used when
    /// `cache_type` is `Disk`. Defaults to `<data_dir>/cache`.
    pub cache_dir: PathBuf,
    /// Pieces behind the live edge to start at, giving an immediate playback cushion.
    pub prefetch_pieces: u64,
    /// Depth of the per-session fan-out broadcast channel (messages buffered per client).
    /// Must be >= 1.
    pub session_buffer: usize,
    /// Live lag-recovery policy and active upstream bounds.
    pub live_recovery: LiveRecoveryConfig,
    /// HLS byte-segment packaging policy.
    pub hls: HlsConfig,
    /// Max simultaneously-unchoked peers per served stream (S2). Wired into the inbound serve
    /// path via the per-infohash `ServeCoordinator`: each stream unchokes up to this many
    /// interested peers plus one rotating optimistic slot (rotated by the daemon's rechoke ticker).
    pub max_unchoked: usize,
    /// Max concurrent inbound peer connections accepted by the listener.
    pub max_inbound_peers: usize,
    /// Idle-TTL (seconds) after which an OWNERLESS leech `SeedRegistry` entry (one with no live
    /// producer lease) is force-evicted by the reaper — a backstop against orphans created outside
    /// the lease API. Entries held by a live lease and all broadcasts are never reaped; normal
    /// teardown rides the lease drop. `OUTPACE_SEED_TTL_SECS`. 0 disables the reaper.
    pub seed_ttl_secs: u64,
    /// Reciprocal upload over connections we initiate (S1): answering a peer's
    /// `Interested`/chunk-requests and advertising `Have` for newly-completed pieces.
    pub enable_seeding: bool,
    /// Accept inbound peer connections and seed them (S2). Defaults ON to match how the
    /// original Acestream engine behaves out of the box — a full P2P participant that binds
    /// its peer port, accepts inbound peers, seeds, and self-announces to trackers + DHT. The
    /// live piece-header acceptance gap is closed (note 33). Only the HTTP API `bind` stays on
    /// localhost by default; the exposed listener is the peer port (`peer_listen`), as with
    /// Acestream. Set `OUTPACE_ENABLE_INBOUND=0` for a pure-leecher deployment.
    pub enable_inbound: bool,
    /// Map the inbound peer port on the home gateway (UPnP-IGD / NAT-PMP) so peers behind NAT
    /// can dial us, mirroring the reference engine's `acestream.upnp __forward`. Best-effort and
    /// non-fatal — a mapping failure logs a warning and the daemon continues. Only takes effect
    /// when `enable_inbound` is also on (no point mapping a closed listener). Defaults **off** so
    /// merging this has zero effect on default operation; enable with
    /// `OUTPACE_ENABLE_PORT_MAPPING=1`. See issue #20.
    pub enable_port_mapping: bool,
    /// Which gateway backend to use for port mapping: `auto` (UPnP then NAT-PMP), `upnp`,
    /// `natpmp`, or `none`. `OUTPACE_PORT_MAP_BACKEND`.
    pub port_map_backend: ace_swarm::portmap::PortMapBackend,
    /// External port to request on the gateway. When unset, request the same port as
    /// `peer_listen`. `OUTPACE_PORT_MAP_EXTERNAL_PORT`.
    pub port_map_external_port: Option<u16>,
    /// Expose Acestream-engine-compatible HTTP routes (`/ace/*`, `/server/api`). This is an
    /// experimental legacy adapter; outpace's native `/streams` and `/broadcast` API is the
    /// supported surface.
    pub experimental_ace_compat: bool,
}

impl Default for Config {
    fn default() -> Self {
        let data_dir = dirs::data_dir()
            .unwrap_or_else(|| PathBuf::from("."))
            .join("outpace");
        let cache_dir = data_dir.join("cache");
        Config {
            bind: "127.0.0.1:6878".parse().unwrap(),
            rtmp_bind: "127.0.0.1:1935".parse().unwrap(),
            data_dir,
            networks: vec!["ace".into()],
            peer_listen: "0.0.0.0:8621".parse().unwrap(),
            seed_store_bytes: 128 * 1024 * 1024,
            seed_retention_secs: 45,
            cache_type: CacheType::Memory,
            cache_dir,
            prefetch_pieces: 8,
            session_buffer: 256,
            live_recovery: LiveRecoveryConfig::default(),
            hls: HlsConfig::default(),
            max_unchoked: 8,
            max_inbound_peers: 64,
            seed_ttl_secs: 300,
            enable_seeding: true,
            enable_inbound: true,
            enable_port_mapping: false,
            port_map_backend: ace_swarm::portmap::PortMapBackend::Auto,
            port_map_external_port: None,
            experimental_ace_compat: false,
        }
    }
}

/// Load the persistent identity seed from `data_dir/identity.seed`, creating a fresh random
/// one (0600) on first run. The node_id is stable across restarts.
pub fn load_or_create_identity(data_dir: &Path) -> std::io::Result<Identity> {
    std::fs::create_dir_all(data_dir)?;
    let path = data_dir.join("identity.seed");
    let seed: [u8; 32] = match std::fs::read(&path) {
        Ok(b) if b.len() == 32 => b.try_into().unwrap(),
        _ => {
            let s: [u8; 32] = rand::random();
            write_private(&path, &s)?;
            s
        }
    };
    Ok(Identity::from_seed(seed))
}

#[cfg(unix)]
fn write_private(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;
    let mut f = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(path)?;
    f.write_all(bytes)
}

#[cfg(not(unix))]
fn write_private(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    std::fs::write(path, bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn live_recovery_rejects_oversized_allocation_bounds() {
        let mut config = LiveRecoveryConfig {
            max_active_upstreams: usize::MAX,
            ..LiveRecoveryConfig::default()
        };
        assert!(config
            .validate()
            .unwrap_err()
            .contains("OUTPACE_MAX_ACTIVE_UPSTREAMS"));

        config = LiveRecoveryConfig {
            max_parallel_connect: usize::MAX,
            ..LiveRecoveryConfig::default()
        };
        assert!(config
            .validate()
            .unwrap_err()
            .contains("OUTPACE_MAX_PARALLEL_CONNECT"));

        config = LiveRecoveryConfig {
            max_piece_advance: u64::MAX,
            max_reasm_pieces_ahead: u64::MAX,
            ..LiveRecoveryConfig::default()
        };
        assert!(config
            .validate()
            .unwrap_err()
            .contains("OUTPACE_MAX_PIECE_ADVANCE"));

        config = LiveRecoveryConfig {
            max_reasm_pieces_ahead: u64::MAX,
            ..LiveRecoveryConfig::default()
        };
        assert!(config
            .validate()
            .unwrap_err()
            .contains("OUTPACE_MAX_REASM_PIECES_AHEAD"));
    }

    #[test]
    fn simulated_32_bit_hls_bounds_segment_and_retained_window_bytes() {
        let object_limit = i32::MAX as u64;
        let mut config = HlsConfig {
            segment_packets: 1_000_000,
            window_segments: 1,
            segment_duration_ms: 1000,
        };
        config.validate_for_object_limit(object_limit).unwrap();

        config.window_segments = 2;
        let err = config.validate_for_object_limit(object_limit).unwrap_err();
        assert!(
            err.contains("HLS window may retain"),
            "unexpected error: {err}"
        );

        config.segment_packets = (usize::try_from(object_limit).unwrap() / 188) + 1;
        config.window_segments = 1;
        let err = config.validate_for_object_limit(object_limit).unwrap_err();
        assert!(err.contains("byte Vec"), "unexpected error: {err}");
    }

    #[test]
    fn hls_segment_packets_requires_at_least_three() {
        for segment_packets in [1, 2] {
            let config = HlsConfig {
                segment_packets,
                ..HlsConfig::default()
            };
            assert_eq!(
                config.validate().unwrap_err(),
                "OUTPACE_HLS_SEGMENT_PACKETS must be >= 3"
            );
        }

        HlsConfig {
            segment_packets: 3,
            ..HlsConfig::default()
        }
        .validate()
        .unwrap();
    }

    #[test]
    fn simulated_32_bit_hls_accepts_largest_window_and_rejects_one_more_segment() {
        let object_limit = i32::MAX as u64;
        let safe_limit = safe_in_memory_pool_limit(object_limit);
        let segment_packets = 3;
        let max_retained_segments = safe_limit / (segment_packets * 188);
        let mut config = HlsConfig {
            segment_packets: usize::try_from(segment_packets).unwrap(),
            window_segments: usize::try_from(max_retained_segments - 1).unwrap(),
            segment_duration_ms: 1000,
        };

        config.validate_for_object_limit(object_limit).unwrap();
        config.window_segments += 1;
        let err = config.validate_for_object_limit(object_limit).unwrap_err();
        assert!(
            err.contains("HLS window may retain"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn hls_rejects_checked_arithmetic_extremes() {
        let config = HlsConfig {
            segment_packets: 3,
            window_segments: usize::MAX,
            segment_duration_ms: 1000,
        };
        assert!(config.validate_for_object_limit(u64::MAX).is_err());
    }

    #[test]
    fn identity_is_stable_across_loads() {
        let dir = std::env::temp_dir().join(format!("outpace-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let a = load_or_create_identity(&dir).unwrap();
        let b = load_or_create_identity(&dir).unwrap();
        assert_eq!(a.node_id(), b.node_id());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn default_config_enables_ace_and_binds_6878() {
        let c = Config::default();
        assert_eq!(c.networks, vec!["ace".to_string()]);
        assert_eq!(c.bind.port(), 6878);
    }

    #[test]
    fn default_config_has_rtmp_bind_on_localhost_1935() {
        let c = Config::default();
        assert_eq!(c.rtmp_bind, "127.0.0.1:1935".parse().unwrap());
    }

    #[test]
    fn default_config_has_seeding_and_inbound_on() {
        let c = Config::default();
        assert_eq!(c.peer_listen.port(), 8621);
        assert_eq!(c.seed_store_bytes, 128 * 1024 * 1024);
        assert_eq!(c.seed_retention_secs, 45);
        assert_eq!(c.prefetch_pieces, 8);
        assert_eq!(c.session_buffer, 256);
        assert_eq!(c.max_unchoked, 8);
        assert_eq!(c.max_inbound_peers, 64);
        assert!(c.enable_seeding);
        assert!(
            c.enable_inbound,
            "inbound serving is on by default, matching the Acestream engine"
        );
        assert!(
            !c.experimental_ace_compat,
            "Acestream HTTP compatibility must be opt-in"
        );
        assert!(
            !c.enable_port_mapping,
            "port mapping must default OFF so merging has zero effect on default operation"
        );
        assert_eq!(
            c.port_map_backend,
            ace_swarm::portmap::PortMapBackend::Auto,
            "default backend is auto (only consulted when port mapping is enabled)"
        );
        assert_eq!(c.port_map_external_port, None);
    }

    #[test]
    fn default_cache_is_memory_under_data_dir() {
        let c = Config::default();
        assert_eq!(c.cache_type, CacheType::Memory);
        assert_eq!(c.cache_dir, c.data_dir.join("cache"));
    }

    #[test]
    fn default_seed_ttl_is_300s() {
        assert_eq!(Config::default().seed_ttl_secs, 300);
    }
}
