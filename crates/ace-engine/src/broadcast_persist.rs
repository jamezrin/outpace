//! On-disk persistence for minted broadcasts (issue #3).
//!
//! One JSON file per broadcast under `<data_dir>/broadcasts/<name>.json`, mode `0600` (it holds
//! the broadcaster's RSA signing key in the clear). This mirrors what the official source node
//! persists (note 25: `.acelive` transport + `.sauth` key + `.restart` last-piece), collapsed
//! into a single file per name:
//!
//! - `transport_hex` — the minted transport bytes (identity source of truth; infohash,
//!   content_id, and geometry are all re-derived from it, so they never drift).
//! - `key_pkcs1_pem` — the signing identity, restorable via `LiveSourceAuth::from_pkcs1_pem`.
//! - `next_piece` — the last-persisted piece cursor for ingest-resume continuity.
//!
//! This module is pure filesystem + JSON/hex: it knows nothing about `ace_wire` identity.
//! Semantic validation (transport decodes, embedded pubkey matches the key) is the registry's
//! job on reload.

use serde::{Deserialize, Serialize};
use std::io;
use std::path::{Path, PathBuf};

/// A broadcast's durable state, decoded from its on-disk record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistedBroadcast {
    pub transport: Vec<u8>,
    pub key_pkcs1_pem: String,
    pub next_piece: u64,
}

/// The exact JSON shape on disk. Transport bytes are hex so the record stays a single,
/// human-inspectable text file without pulling in a base64/serialization dependency.
#[derive(Serialize, Deserialize)]
struct OnDisk {
    transport_hex: String,
    key_pkcs1_pem: String,
    next_piece: u64,
}

/// Reads and writes broadcast records under `<data_dir>/broadcasts/`.
#[derive(Clone)]
pub struct BroadcastPersist {
    dir: PathBuf,
}

impl BroadcastPersist {
    pub fn new(data_dir: &Path) -> Self {
        BroadcastPersist {
            dir: data_dir.join("broadcasts"),
        }
    }

    fn path(&self, name: &str) -> PathBuf {
        self.dir.join(format!("{name}.json"))
    }

    /// Persist `rec` for `name` atomically (temp file + rename) at mode `0600`. Callers must
    /// pass an already-validated `name` (see `http::valid_broadcast_name`) — this becomes a
    /// filename.
    pub fn save(&self, name: &str, rec: &PersistedBroadcast) -> io::Result<()> {
        if !crate::broadcast::valid_broadcast_name(name) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid broadcast name",
            ));
        }
        std::fs::create_dir_all(&self.dir)?;
        let on_disk = OnDisk {
            transport_hex: hex::encode(&rec.transport),
            key_pkcs1_pem: rec.key_pkcs1_pem.clone(),
            next_piece: rec.next_piece,
        };
        let json = serde_json::to_vec_pretty(&on_disk)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let final_path = self.path(name);
        let tmp_path = self.dir.join(format!("{name}.json.tmp"));
        write_private(&tmp_path, &json)?;
        std::fs::rename(&tmp_path, &final_path)?;
        Ok(())
    }

    /// Load every persisted broadcast. Files that fail to read or parse are logged and skipped
    /// so one corrupt record never aborts startup.
    pub fn load_all(&self) -> Vec<(String, PersistedBroadcast)> {
        let mut out = Vec::new();
        let entries = match std::fs::read_dir(&self.dir) {
            Ok(e) => e,
            Err(_) => return out, // no broadcasts dir yet
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("json") {
                continue;
            }
            let Some(name) = path.file_stem().and_then(|s| s.to_str()) else {
                continue;
            };
            match load_record(&path) {
                Ok(rec) => out.push((name.to_string(), rec)),
                Err(e) => crate::alog!("[broadcast] skipping unreadable record {path:?}: {e}"),
            }
        }
        out
    }

    /// Load the single persisted record for `name`, or `None` if it is absent or unreadable.
    pub fn load(&self, name: &str) -> Option<PersistedBroadcast> {
        if !crate::broadcast::valid_broadcast_name(name) {
            return None;
        }
        load_record(&self.path(name)).ok()
    }

    /// Remove the persisted record for `name`. A missing file is not an error (idempotent).
    pub fn delete(&self, name: &str) -> io::Result<()> {
        if !crate::broadcast::valid_broadcast_name(name) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid broadcast name",
            ));
        }
        match std::fs::remove_file(self.path(name)) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e),
        }
    }
}

fn load_record(path: &Path) -> io::Result<PersistedBroadcast> {
    let bytes = std::fs::read(path)?;
    let on_disk: OnDisk = serde_json::from_slice(&bytes)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    let transport = hex::decode(&on_disk.transport_hex)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "bad transport_hex"))?;
    Ok(PersistedBroadcast {
        transport,
        key_pkcs1_pem: on_disk.key_pkcs1_pem,
        next_piece: on_disk.next_piece,
    })
}

#[cfg(unix)]
fn write_private(path: &Path, bytes: &[u8]) -> io::Result<()> {
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
fn write_private(path: &Path, bytes: &[u8]) -> io::Result<()> {
    std::fs::write(path, bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp_dir() -> PathBuf {
        let d = std::env::temp_dir().join(format!("outpace-bp-test-{}", rand::random::<u64>()));
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    fn sample() -> PersistedBroadcast {
        PersistedBroadcast {
            transport: vec![0xde, 0xad, 0xbe, 0xef, 0x00, 0x10],
            key_pkcs1_pem: "-----BEGIN RSA PRIVATE KEY-----\nABC\n-----END RSA PRIVATE KEY-----\n"
                .to_string(),
            next_piece: 51234,
        }
    }

    #[test]
    fn save_then_load_round_trips_a_record() {
        let dir = tmp_dir();
        let p = BroadcastPersist::new(&dir);
        p.save("news", &sample()).unwrap();
        let loaded = p.load_all();
        assert_eq!(loaded, vec![("news".to_string(), sample())]);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn save_rejects_invalid_names_before_constructing_paths() {
        let dir = tmp_dir();
        let p = BroadcastPersist::new(&dir);
        for name in [".", "..", "../../escape", "has/slash", "café"] {
            let err = p.save(name, &sample()).unwrap_err();
            assert_eq!(err.kind(), io::ErrorKind::InvalidInput, "{name:?}");
        }
        assert!(p.save(&"a".repeat(65), &sample()).is_err());
        assert!(!dir.join("escape.json").exists());
        assert!(!dir.join("broadcasts").exists());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn hex_round_trips_arbitrary_bytes() {
        let bytes: Vec<u8> = (0..=255).collect();
        assert_eq!(hex::decode(hex::encode(&bytes)).unwrap(), bytes);
        assert!(hex::decode("xyz").is_err());
        assert!(hex::decode("abc").is_err()); // odd length
    }

    #[test]
    fn save_is_atomic_and_leaves_no_tmp_file() {
        let dir = tmp_dir();
        let p = BroadcastPersist::new(&dir);
        p.save("chan", &sample()).unwrap();
        let files: Vec<String> = std::fs::read_dir(dir.join("broadcasts"))
            .unwrap()
            .flatten()
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        assert_eq!(files, vec!["chan.json".to_string()], "no leftover .tmp");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[cfg(unix)]
    #[test]
    fn saved_record_is_mode_0600() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tmp_dir();
        let p = BroadcastPersist::new(&dir);
        p.save("secret", &sample()).unwrap();
        let meta = std::fs::metadata(dir.join("broadcasts/secret.json")).unwrap();
        assert_eq!(meta.permissions().mode() & 0o777, 0o600);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn delete_removes_the_file_and_is_idempotent() {
        let dir = tmp_dir();
        let p = BroadcastPersist::new(&dir);
        p.save("gone", &sample()).unwrap();
        p.delete("gone").unwrap();
        assert!(p.load_all().is_empty());
        p.delete("gone").unwrap(); // idempotent
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn load_all_skips_a_corrupt_file_and_keeps_good_ones() {
        let dir = tmp_dir();
        let p = BroadcastPersist::new(&dir);
        p.save("good", &sample()).unwrap();
        std::fs::write(dir.join("broadcasts/bad.json"), b"{ not valid json").unwrap();
        let loaded = p.load_all();
        assert_eq!(loaded, vec![("good".to_string(), sample())]);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
