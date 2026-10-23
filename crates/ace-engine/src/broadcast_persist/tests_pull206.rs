//! Persistence contract tests against the real filesystem implementation.

use super::{BroadcastPersist, PersistOperation, PersistedBroadcast, TestPersistIo};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

fn temp_dir() -> PathBuf {
    let path = std::env::temp_dir().join(format!("outpace-persist206-{}", rand::random::<u64>()));
    std::fs::create_dir_all(&path).unwrap();
    path
}

fn sample(reserved_until: Option<u64>) -> PersistedBroadcast {
    PersistedBroadcast {
        transport: vec![0x10, 0x20, 0x30],
        key_pkcs1_pem: "synthetic test key material".into(),
        next_piece: 7,
        reserved_until,
    }
}

#[derive(Clone, Debug)]
pub(crate) struct Event {
    pub(crate) operation: PersistOperation,
    pub(crate) path: PathBuf,
    pub(crate) other: Option<PathBuf>,
}

#[derive(Clone, Copy)]
enum Failure {
    Operation(PersistOperation, usize),
    #[cfg(unix)]
    DirectorySyncAfterReplace,
}

struct Gate {
    operation: PersistOperation,
    entered: std::sync::mpsc::SyncSender<()>,
    release: std::sync::mpsc::Receiver<()>,
    watchdog: Duration,
}

/// Observes and controls the actual calls made by BroadcastPersist; it never implements file IO.
pub(crate) struct Observer {
    events: Mutex<Vec<Event>>,
    failure: Mutex<Option<Failure>>,
    gate: Mutex<Option<Gate>>,
    collide_once: Mutex<bool>,
}

impl Observer {
    fn new() -> Self {
        Self {
            events: Mutex::new(Vec::new()),
            failure: Mutex::new(None),
            gate: Mutex::new(None),
            collide_once: Mutex::new(false),
        }
    }

    pub(crate) fn events(&self) -> Vec<Event> {
        self.events.lock().unwrap().clone()
    }

    pub(crate) fn fail_at(&self, operation: PersistOperation, occurrence: usize) {
        *self.failure.lock().unwrap() = Some(Failure::Operation(operation, occurrence));
    }

    pub(crate) fn fail_next(&self, operation: PersistOperation) {
        let seen = self
            .events
            .lock()
            .unwrap()
            .iter()
            .filter(|event| event.operation == operation)
            .count();
        self.fail_at(operation, seen + 1);
    }

    #[cfg(unix)]
    pub(crate) fn fail_directory_sync_after_replace(&self) {
        *self.failure.lock().unwrap() = Some(Failure::DirectorySyncAfterReplace);
    }

    pub(crate) fn gate(
        &self,
        operation: PersistOperation,
    ) -> (
        std::sync::mpsc::Receiver<()>,
        std::sync::mpsc::SyncSender<()>,
    ) {
        self.gate_with_watchdog(operation, Duration::from_secs(5))
    }

    pub(crate) fn gate_with_watchdog(
        &self,
        operation: PersistOperation,
        watchdog: Duration,
    ) -> (
        std::sync::mpsc::Receiver<()>,
        std::sync::mpsc::SyncSender<()>,
    ) {
        let (entered_tx, entered_rx) = std::sync::mpsc::sync_channel(1);
        let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
        *self.gate.lock().unwrap() = Some(Gate {
            operation,
            entered: entered_tx,
            release: release_rx,
            watchdog,
        });
        (entered_rx, release_tx)
    }

    pub(crate) fn collide_on_first_temp(&self) {
        *self.collide_once.lock().unwrap() = true;
    }
}

impl TestPersistIo for Observer {
    fn before(
        &self,
        operation: PersistOperation,
        path: &Path,
        other: Option<&Path>,
    ) -> io::Result<()> {
        let event = Event {
            operation,
            path: path.to_owned(),
            other: other.map(Path::to_owned),
        };
        let prior = self.events.lock().unwrap().clone();
        self.events.lock().unwrap().push(event.clone());

        if operation == PersistOperation::CreateTemp
            && std::mem::take(&mut *self.collide_once.lock().unwrap())
        {
            // Leave one collision so the real create_new loop must choose another private temp.
            std::fs::write(path, b"collision")?;
        }

        let gate = {
            let mut gate = self.gate.lock().unwrap();
            if gate
                .as_ref()
                .is_some_and(|gate| gate.operation == operation)
            {
                gate.take()
            } else {
                None
            }
        };
        if let Some(gate) = gate {
            gate.entered
                .send(())
                .map_err(|_| io::Error::other("test gate closed"))?;
            gate.release
                .recv_timeout(gate.watchdog)
                .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "test gate timed out"))?;
        }

        let mut failure = self.failure.lock().unwrap();
        let should_fail = match *failure {
            Some(Failure::Operation(expected, occurrence)) if expected == operation => {
                prior
                    .iter()
                    .filter(|event| event.operation == operation)
                    .count()
                    + 1
                    == occurrence
            }
            #[cfg(unix)]
            Some(Failure::DirectorySyncAfterReplace) => {
                operation == PersistOperation::DirectorySync
                    && prior
                        .iter()
                        .any(|event| event.operation == PersistOperation::Replace)
            }
            _ => false,
        };
        if should_fail {
            *failure = None;
            return Err(io::Error::other("injected persistence fault"));
        }
        Ok(())
    }
}

#[test]
fn pull206_persisted_reservation_round_trips_and_legacy_defaults_to_none() {
    let dir = temp_dir();
    let persist = BroadcastPersist::new(&dir);
    persist.save("current", &sample(Some(128))).unwrap();
    assert_eq!(persist.load("current").unwrap(), Some(sample(Some(128))));

    let path = dir.join("broadcasts/legacy.json");
    std::fs::write(
        &path,
        br#"{"transport_hex":"102030","key_pkcs1_pem":"synthetic test key material","next_piece":7}"#,
    ).unwrap();
    assert_eq!(persist.load("legacy").unwrap(), Some(sample(None)));
    assert_eq!(persist.load("missing").unwrap(), None);
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn pull206_load_all_preserves_bad_record_name_as_an_error() {
    let dir = temp_dir();
    let persist = BroadcastPersist::new(&dir);
    persist.save("healthy", &sample(Some(128))).unwrap();
    std::fs::write(dir.join("broadcasts/unreadable.json"), b"{bad-json").unwrap();

    let loaded = persist.load_all().unwrap();
    assert_eq!(
        loaded.len(),
        2,
        "the invalid name must survive directory enumeration"
    );
    let healthy = loaded.iter().find(|(name, _)| name == "healthy").unwrap();
    assert_eq!(healthy.1.as_ref().unwrap(), &sample(Some(128)));
    let invalid = loaded
        .iter()
        .find(|(name, _)| name == "unreadable")
        .unwrap();
    assert!(invalid.1.is_err());

    let absent = BroadcastPersist::new(&dir.join("absent"));
    assert!(absent.load_all().unwrap().is_empty());
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn pull206_platform_save_delete_order() {
    let dir = temp_dir();
    let observer = Arc::new(Observer::new());
    observer.collide_on_first_temp();
    let persist = BroadcastPersist::with_test_io(&dir, observer.clone());

    persist.save("ordered", &sample(Some(128))).unwrap();
    let events = observer.events();
    let temps: Vec<_> = events
        .iter()
        .filter(|event| event.operation == PersistOperation::CreateTemp)
        .collect();
    assert!(
        temps.len() >= 2,
        "create_new collision retries a unique private temp"
    );
    for event in &temps {
        let final_path = event.other.as_ref().unwrap();
        assert_eq!(
            event.path.parent(),
            final_path.parent(),
            "temp is beside final file"
        );
        assert!(event
            .path
            .file_name()
            .unwrap()
            .to_string_lossy()
            .ends_with(".tmp"));
    }
    let sync = events
        .iter()
        .position(|event| event.operation == PersistOperation::FileSync)
        .unwrap();
    let replace = events
        .iter()
        .position(|event| event.operation == PersistOperation::Replace)
        .unwrap();
    assert!(sync < replace);
    #[cfg(unix)]
    assert!(events.iter().enumerate().any(
        |(index, event)| index > replace && event.operation == PersistOperation::DirectorySync
    ));
    #[cfg(windows)]
    assert!(!events
        .iter()
        .any(|event| event.operation == PersistOperation::DirectorySync));
    assert_eq!(persist.load("ordered").unwrap(), Some(sample(Some(128))));

    for (operation, occurrence) in [
        (PersistOperation::CreateDirectory, 1),
        (PersistOperation::CreateTemp, 1),
        (PersistOperation::Write, 1),
        (PersistOperation::FileSync, 1),
        (PersistOperation::Replace, 1),
    ] {
        let fresh = temp_dir();
        let observer = Arc::new(Observer::new());
        observer.fail_at(operation, occurrence);
        let failing = BroadcastPersist::with_test_io(&fresh, observer);
        assert!(
            failing.save("failed", &sample(Some(128))).is_err(),
            "{operation:?}"
        );
        let _ = std::fs::remove_dir_all(fresh);
    }
    #[cfg(unix)]
    {
        let fresh = temp_dir();
        let observer = Arc::new(Observer::new());
        observer.fail_directory_sync_after_replace();
        let failing = BroadcastPersist::with_test_io(&fresh, observer);
        assert!(failing.save("uncertain", &sample(Some(128))).is_err());
        let _ = std::fs::remove_dir_all(fresh);
    }
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn pull206_delete_failure_is_visible_and_retryable() {
    let dir = temp_dir();
    let observer = Arc::new(Observer::new());
    let persist = BroadcastPersist::with_test_io(&dir, observer.clone());
    persist.save("retry", &sample(Some(128))).unwrap();
    observer.fail_at(PersistOperation::Delete, 1);
    assert!(persist.delete("retry").is_err());
    assert!(persist.load("retry").unwrap().is_some());
    persist.delete("retry").unwrap();
    assert_eq!(persist.load("retry").unwrap(), None);
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn pull206_blocked_save_and_delete_are_serialized() {
    let dir = temp_dir();
    let observer = Arc::new(Observer::new());
    let persist = BroadcastPersist::with_test_io(&dir, observer.clone());
    let (save_entered, release_save) = observer.gate(PersistOperation::FileSync);
    let save = std::thread::spawn({
        let persist = persist.clone();
        move || persist.save("race", &sample(Some(128)))
    });
    save_entered.recv_timeout(Duration::from_secs(5)).unwrap();

    let (delete_started_tx, delete_started_rx) = std::sync::mpsc::sync_channel(1);
    let delete = std::thread::spawn({
        let persist = persist.clone();
        move || {
            delete_started_tx.send(()).unwrap();
            persist.delete("race")
        }
    });
    delete_started_rx
        .recv_timeout(Duration::from_secs(5))
        .unwrap();
    assert!(observer
        .events()
        .iter()
        .all(|event| event.operation != PersistOperation::Delete));
    release_save.send(()).unwrap();
    save.join().unwrap().unwrap();
    delete.join().unwrap().unwrap();
    assert_eq!(
        persist.load("race").unwrap(),
        None,
        "late save cannot resurrect after delete"
    );
    let _ = std::fs::remove_dir_all(dir);
}

#[cfg(test)]
pub(crate) fn persistence_observer() -> Arc<Observer> {
    Arc::new(Observer::new())
}
