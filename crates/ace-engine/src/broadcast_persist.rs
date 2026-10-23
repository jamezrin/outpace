//! Private per-name signing identities and exclusive piece reservations.
//! Callers serialize this blocking boundary with the entry's owned I/O gate.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

#[derive(Clone, PartialEq, Eq)]
pub struct PersistedBroadcast {
    pub transport: Vec<u8>,
    pub key_pkcs1_pem: String,
    pub next_piece: u64,
    pub reserved_until: Option<u64>,
}

// Do not expose private key material through diagnostics.
impl std::fmt::Debug for PersistedBroadcast {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PersistedBroadcast")
            .field("next_piece", &self.next_piece)
            .field("reserved_until", &self.reserved_until)
            .finish_non_exhaustive()
    }
}

#[derive(Serialize, Deserialize)]
struct OnDisk {
    transport_hex: String,
    key_pkcs1_pem: String,
    next_piece: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    reserved_until: Option<u64>,
}

/// Observes the actual operation immediately before it runs. A test may block or fail it;
/// the filesystem implementation and its ordering remain the production implementation.
#[cfg(test)]
pub(crate) trait TestPersistIo: Send + Sync {
    fn before(
        &self,
        operation: PersistOperation,
        path: &Path,
        other: Option<&Path>,
    ) -> io::Result<()>;
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PersistOperation {
    CreateDirectory,
    CreateTemp,
    Write,
    FileSync,
    Replace,
    DirectorySync,
    Delete,
    CleanupTemp,
}

type NameLock = Arc<Mutex<()>>;

#[derive(Clone)]
pub struct BroadcastPersist {
    dir: PathBuf,
    // Also serialize direct blocking users of clones (the registry additionally owns its
    // asynchronous per-entry gate). No async executor thread waits on this lock.
    serial: Arc<Mutex<BTreeMap<String, NameLock>>>,
    #[cfg(test)]
    observer: Option<Arc<dyn TestPersistIo>>,
}

impl BroadcastPersist {
    pub fn new(data_dir: &Path) -> Self {
        Self {
            dir: data_dir.join("broadcasts"),
            serial: Arc::new(Mutex::new(BTreeMap::new())),
            #[cfg(test)]
            observer: None,
        }
    }

    #[cfg(test)]
    pub(crate) fn with_test_io(data_dir: &Path, observer: Arc<dyn TestPersistIo>) -> Self {
        Self {
            observer: Some(observer),
            ..Self::new(data_dir)
        }
    }

    fn path(&self, name: &str) -> io::Result<PathBuf> {
        if !crate::broadcast::valid_broadcast_name(name) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid broadcast name",
            ));
        }
        Ok(self.dir.join(format!("{name}.json")))
    }

    fn serial_for(&self, name: &str) -> Arc<Mutex<()>> {
        self.serial
            .lock()
            .unwrap()
            .entry(name.to_owned())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }

    pub fn save(&self, name: &str, rec: &PersistedBroadcast) -> io::Result<()> {
        let final_path = self.path(name)?;
        let serial = self.serial_for(name);
        let _serial = serial.lock().unwrap();
        self.create_directories(&self.dir)?;
        let json = serde_json::to_vec_pretty(&OnDisk {
            transport_hex: hex::encode(&rec.transport),
            key_pkcs1_pem: rec.key_pkcs1_pem.clone(),
            next_piece: rec.next_piece,
            reserved_until: rec.reserved_until,
        })
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid broadcast record"))?;
        let (tmp, mut file) = loop {
            let tmp = self
                .dir
                .join(format!(".{name}.{:016x}.tmp", rand::random::<u64>()));
            #[cfg(test)]
            self.observe(PersistOperation::CreateTemp, &tmp, Some(&final_path))?;
            let mut opts = std::fs::OpenOptions::new();
            opts.write(true).create_new(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                opts.mode(0o600);
            }
            match opts.open(&tmp) {
                Ok(file) => break (tmp, file),
                Err(e) if e.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(e) => return Err(e),
            }
        };
        let result = (|| {
            #[cfg(test)]
            self.observe(PersistOperation::Write, &tmp, None)?;
            file.write_all(&json)?;
            #[cfg(test)]
            self.observe(PersistOperation::FileSync, &tmp, None)?;
            file.sync_all()?;
            // Close before replacement, including on Windows.
            drop(file);
            #[cfg(test)]
            self.observe(PersistOperation::Replace, &tmp, Some(&final_path))?;
            std::fs::rename(&tmp, &final_path)?;
            self.sync_directory(&self.dir)
        })();
        if result.is_err() {
            #[cfg(test)]
            if self
                .observe(PersistOperation::CleanupTemp, &tmp, None)
                .is_ok()
            {
                let _ = std::fs::remove_file(&tmp);
            }
            #[cfg(not(test))]
            let _ = std::fs::remove_file(&tmp);
        }
        result
    }

    /// Missing is distinct from unreadable, corrupt or invalid.
    pub fn load(&self, name: &str) -> io::Result<Option<PersistedBroadcast>> {
        let path = self.path(name)?;
        match load_record(&path) {
            Ok(rec) => Ok(Some(rec)),
            Err(e) if e.kind() == io::ErrorKind::NotFound => match std::fs::symlink_metadata(&path)
            {
                Err(missing) if missing.kind() == io::ErrorKind::NotFound => Ok(None),
                _ => Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "unreadable broadcast record",
                )),
            },
            Err(e) => Err(e),
        }
    }

    pub fn load_all(&self) -> io::Result<Vec<(String, io::Result<PersistedBroadcast>)>> {
        let entries = match std::fs::read_dir(&self.dir) {
            Ok(entries) => entries,
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                match std::fs::symlink_metadata(&self.dir) {
                    Err(missing) if missing.kind() == io::ErrorKind::NotFound => {
                        return Ok(Vec::new())
                    }
                    _ => return Err(e),
                }
            }
            Err(e) => return Err(e),
        };
        let mut out = Vec::new();
        for entry in entries {
            let path = entry?.path();
            if path.extension().and_then(|s| s.to_str()) != Some("json") {
                continue;
            }
            let name = path.file_stem().and_then(|s| s.to_str()).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "invalid broadcast filename")
            })?;
            let record = if crate::broadcast::valid_broadcast_name(name) {
                load_record(&path)
            } else {
                Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "invalid broadcast name",
                ))
            };
            out.push((name.to_owned(), record));
        }
        out.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(out)
    }

    pub fn delete(&self, name: &str) -> io::Result<()> {
        let path = self.path(name)?;
        let serial = self.serial_for(name);
        let _serial = serial.lock().unwrap();
        #[cfg(test)]
        self.observe(PersistOperation::Delete, &path, None)?;
        match std::fs::remove_file(&path) {
            Ok(()) => self.sync_directory(&self.dir),
            // Retrying after an uncertain directory sync must sync again, even when unlink
            // already succeeded. A genuinely missing directory needs no namespace update.
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                match std::fs::symlink_metadata(&self.dir) {
                    Ok(_) => self.sync_directory(&self.dir),
                    Err(missing) if missing.kind() == io::ErrorKind::NotFound => Ok(()),
                    Err(error) => Err(error),
                }
            }
            Err(e) => Err(e),
        }
    }

    fn create_directories(&self, path: &Path) -> io::Result<()> {
        let missing = match std::fs::metadata(path) {
            Ok(meta) if meta.is_dir() => false,
            Ok(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::NotADirectory,
                    "invalid state directory",
                ))
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => true,
            Err(e) => return Err(e),
        };
        // Settle ancestor links too: runtime identity setup may just have created the data
        // directory hierarchy. Empty relative parents refer to the current directory.
        let parent = path.parent().map(|parent| {
            if parent.as_os_str().is_empty() {
                Path::new(".")
            } else {
                parent
            }
        });
        if let Some(parent) = parent.filter(|parent| *parent != path) {
            self.create_directories(parent)?;
        }
        if missing {
            #[cfg(test)]
            self.observe(PersistOperation::CreateDirectory, path, None)?;
            match std::fs::create_dir(path) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::AlreadyExists && path.is_dir() => {}
                Err(e) => return Err(e),
            }
        }
        self.sync_directory(path)?;
        if let Some(parent) = parent.filter(|parent| *parent != path) {
            self.sync_directory(parent)?;
        }
        Ok(())
    }

    fn sync_directory(&self, path: &Path) -> io::Result<()> {
        #[cfg(unix)]
        {
            #[cfg(test)]
            self.observe(PersistOperation::DirectorySync, path, None)?;
            let dir = std::fs::File::open(path)?;
            #[cfg(target_os = "macos")]
            {
                use std::os::fd::AsRawFd;
                // File::sync_all on macOS requests F_FULLFSYNC; directories need fsync.
                if unsafe { libc::fsync(dir.as_raw_fd()) } != 0 {
                    return Err(io::Error::last_os_error());
                }
                Ok(())
            }
            #[cfg(not(target_os = "macos"))]
            dir.sync_all()
        }
        #[cfg(not(unix))]
        {
            // Approved Windows boundary: process restart with surviving filesystem state.
            // No unsupported directory flush, elevation or power-loss promise.
            let _ = path;
            Ok(())
        }
    }

    #[cfg(test)]
    fn observe(&self, op: PersistOperation, path: &Path, other: Option<&Path>) -> io::Result<()> {
        match &self.observer {
            Some(observer) => observer.before(op, path, other),
            None => Ok(()),
        }
    }
}

fn load_record(path: &Path) -> io::Result<PersistedBroadcast> {
    let bytes = std::fs::read(path)?;
    let on_disk: OnDisk = serde_json::from_slice(&bytes)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid broadcast record"))?;
    let transport = hex::decode(&on_disk.transport_hex)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid broadcast record"))?;
    Ok(PersistedBroadcast {
        transport,
        key_pkcs1_pem: on_disk.key_pkcs1_pem,
        next_piece: on_disk.next_piece,
        reserved_until: on_disk.reserved_until,
    })
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
            reserved_until: Some(51234),
        }
    }

    #[test]
    fn save_then_load_round_trips_a_record() {
        let dir = tmp_dir();
        let p = BroadcastPersist::new(&dir);
        p.save("news", &sample()).unwrap();
        let loaded = p.load_all().unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].0, "news");
        assert_eq!(loaded[0].1.as_ref().unwrap(), &sample());
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
        assert!(p.load_all().unwrap().is_empty());
        p.delete("gone").unwrap(); // idempotent
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn load_all_retains_a_corrupt_file_as_an_error_and_keeps_good_ones() {
        let dir = tmp_dir();
        let p = BroadcastPersist::new(&dir);
        p.save("good", &sample()).unwrap();
        std::fs::write(dir.join("broadcasts/bad.json"), b"{ not valid json").unwrap();
        let loaded = p.load_all().unwrap();
        assert_eq!(loaded.len(), 2);
        assert_eq!(
            loaded
                .iter()
                .find(|(name, _)| name == "good")
                .unwrap()
                .1
                .as_ref()
                .unwrap(),
            &sample()
        );
        assert!(loaded
            .iter()
            .find(|(name, _)| name == "bad")
            .unwrap()
            .1
            .is_err());
        let _ = std::fs::remove_dir_all(&dir);
    }
}

#[cfg(test)]
pub(crate) mod tests_pull206;
