//! Daemon configuration and persistent node identity.

use ace_wire::identity::Identity;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

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
    /// Bytes of recently-seen piece data retained per active peer connection for reseeding.
    pub seed_store_bytes: u64,
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
    /// Max simultaneously-unchoked peers per served stream. NOT YET WIRED: `Choker` (the
    /// policy this would configure) has no production caller until the multi-peer S2 serve
    /// coordinator lands.
    pub max_unchoked: usize,
    /// Max concurrent inbound peer connections accepted by the listener.
    pub max_inbound_peers: usize,
    /// Idle-TTL (seconds) after which a leech `SeedRegistry` entry with a leaked producer lease is
    /// force-evicted by the reaper. Broadcasts are exempt. Backstop only — normal teardown rides
    /// the lease. `OUTPACE_SEED_TTL_SECS`.
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
            cache_type: CacheType::Memory,
            cache_dir,
            prefetch_pieces: 8,
            session_buffer: 256,
            max_unchoked: 8,
            max_inbound_peers: 64,
            seed_ttl_secs: 300,
            enable_seeding: true,
            enable_inbound: true,
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
