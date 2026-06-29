//! Daemon configuration and persistent node identity.

use ace_wire::identity::Identity;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct Config {
    /// Address the HTTP API binds to.
    pub bind: SocketAddr,
    /// Directory for the persistent identity seed and any caches.
    pub data_dir: PathBuf,
    /// Networks (providers) to enable.
    pub networks: Vec<String>,
}

impl Default for Config {
    fn default() -> Self {
        let data_dir = dirs::data_dir()
            .unwrap_or_else(|| PathBuf::from("."))
            .join("outpace");
        Config {
            bind: "127.0.0.1:6878".parse().unwrap(),
            data_dir,
            networks: vec!["ace".into()],
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
    use std::os::unix::fs::OpenOptionsExt;
    use std::io::Write;
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
}
