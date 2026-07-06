# Inbound Seeding Lifecycle & Policy Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Bound `SeedRegistry` growth by tying entry lifetime to its producer (with a TTL backstop), make the disk cache self-cleaning (folding in #36), and give `max_unchoked` real effect via a per-stream serve coordinator.

**Architecture:** Registry entries gain a producer refcount and an RAII `SeedLease`; the leech loop and each broadcast hold a lease whose `Drop` evicts the entry at zero. A per-infohash `ServeCoordinator` drives the existing `Choker` across all inbound connections for a stream. Disk stores get a process-unique directory and a `Drop` that removes it, so eviction/DELETE/exit all clean up uniformly.

**Tech Stack:** Rust, tokio (`sync::Mutex`, `sync::watch`, `sync::Semaphore`), `std::sync::Mutex` for the registry map. Crates: `ace-swarm` (store, seed, listen), `ace-engine` (ace_provider, broadcast, http, runtime, config).

**Spec:** `docs/superpowers/specs/2026-07-06-inbound-seeding-lifecycle-design.md`

**Gates (run before every commit):** `cargo test --workspace` and `cargo clippy --workspace --all-targets`. Note: one pre-existing ace-media mpegts-fixture test fails on a missing fixture — not a regression.

---

## File map

| File | Responsibility | Change |
|---|---|---|
| `crates/ace-swarm/src/store.rs` | `PieceStore` + `BackendKind` | Disk `Drop` cleanup lives in the backend (Task 2) |
| `crates/ace-swarm/src/store/disk.rs` | `DiskBackend` | Add `Drop` removing its own dir (Task 2) |
| `crates/ace-engine/src/ace_provider.rs` | `build_piece_store`, leech loop | Unique dir name (Task 1); hold `SeedLease` (Task 4) |
| `crates/ace-engine/src/runtime.rs` | daemon wiring | Startup root wipe (Task 1); TTL reaper + rechoke ticker (Tasks 6, 11) |
| `crates/ace-swarm/src/listen.rs` | `SeedRegistry`, `SeedLease`, `PeerListener` | Leases + TTL + coordinator accessors (Tasks 3, 6, 9, 11) |
| `crates/ace-engine/src/broadcast.rs` | `BroadcastRegistry` | Hold leases, drop `remove_cache_dir` (Task 5) |
| `crates/ace-engine/src/http.rs` | `broadcast_delete` | Drop explicit `remove` calls (Task 5) |
| `crates/ace-swarm/src/seed.rs` | `SeederSession::serve`, `ServeCoordinator` | Coordinator type + serve integration (Tasks 8, 10) |
| `crates/ace-engine/src/config.rs` | `Config` | `enable_inbound` note (Task 7); TTL field (Task 6) |
| `README.md` | operator docs | dir-name + `enable_inbound` + TTL notes (Tasks 5, 7) |

---

# PHASE 1 — Lifecycle + disk cleanup (#36)

## Task 1: Process-unique disk directory + startup root wipe

**Files:**
- Modify: `crates/ace-engine/src/ace_provider.rs:117-144` (`build_piece_store`)
- Modify: `crates/ace-engine/src/runtime.rs:143-152` (startup cache prep)
- Test: `crates/ace-engine/src/ace_provider.rs` (tests module)

- [ ] **Step 1: Write the failing test** — add to the `#[cfg(test)] mod tests` in `ace_provider.rs`:

```rust
#[test]
fn disk_store_dir_is_process_unique_per_instance() {
    let tmp = std::env::temp_dir().join(format!("outpace-uniqdir-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&tmp);
    std::fs::create_dir_all(&tmp).unwrap();
    let ih = [7u8; 20];
    // Two stores for the SAME infohash must land in DIFFERENT directories.
    let _s1 = build_piece_store(1 << 20, 1 << 14, 1 << 20, CacheType::Disk, &tmp, &ih);
    let _s2 = build_piece_store(1 << 20, 1 << 14, 1 << 20, CacheType::Disk, &tmp, &ih);
    let dirs: Vec<_> = std::fs::read_dir(&tmp)
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .collect();
    assert_eq!(dirs.len(), 2, "each store instance owns its own dir: {dirs:?}");
    assert!(
        dirs.iter().all(|d| d.starts_with(&infohash_hex(&ih))),
        "dir names keep the readable infohash prefix: {dirs:?}"
    );
    let _ = std::fs::remove_dir_all(&tmp);
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p ace-engine disk_store_dir_is_process_unique_per_instance`
Expected: FAIL — both stores share `<tmp>/<infohash_hex>` so `dirs.len()` is 1.

- [ ] **Step 3: Implement the unique dir** — replace the `CacheType::Disk` arm and the doc comment of `build_piece_store`:

```rust
/// Build a [`PieceStore`] for `infohash` honoring the configured cache backend. In disk mode the
/// store lives under `<cache_dir>/<infohash_hex>-<generation>`, where `generation` is a
/// process-unique counter so each store instance owns a private directory no other instance can
/// touch (its `Drop` removes exactly that dir — see `DiskBackend`). If the directory cannot be
/// prepared (an unexpected mid-run I/O error — the common misconfiguration is caught at startup)
/// we log and fall back to a memory store so a live stream keeps serving rather than dying.
pub(crate) fn build_piece_store(
    piece_length: u64,
    chunk_length: u64,
    max_bytes: u64,
    cache_type: CacheType,
    cache_dir: &Path,
    infohash: &[u8; 20],
) -> PieceStore {
    match cache_type {
        CacheType::Memory => PieceStore::new(piece_length, chunk_length, max_bytes),
        CacheType::Disk => {
            let dir = cache_dir.join(disk_store_subdir(infohash));
            PieceStore::with_backend(
                piece_length,
                chunk_length,
                max_bytes,
                BackendKind::Disk { dir: dir.clone() },
            )
            .unwrap_or_else(|e| {
                crate::alog!(
                    "[cache] disk cache unavailable at {}: {e}; falling back to memory",
                    dir.display()
                );
                PieceStore::new(piece_length, chunk_length, max_bytes)
            })
        }
    }
}

/// Per-instance disk cache subdirectory name: `<infohash_hex>-<generation>`. The readable infohash
/// prefix aids operators; the monotonic suffix guarantees a fresh directory per store instance so
/// a stale store's `Drop` can never delete a re-created same-infohash store's data.
fn disk_store_subdir(infohash: &[u8; 20]) -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static GENERATION: AtomicU64 = AtomicU64::new(0);
    let gen = GENERATION.fetch_add(1, Ordering::Relaxed);
    format!("{}-{gen}", infohash_hex(infohash))
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p ace-engine disk_store_dir_is_process_unique_per_instance`
Expected: PASS.

- [ ] **Step 5: Startup root wipe** — in `runtime.rs`, replace the disk-cache prep block (currently `create_dir_all` only) at `build_runtime`:

```rust
    // Fail fast on a misconfigured disk cache, and start from a clean slate: wipe the cache root
    // so per-infohash dirs orphaned by a hard crash (no `Drop` ran) don't survive a restart. The
    // cache is ephemeral (piece data goes stale; broadcasts rebuild theirs from live ingest), so
    // wiping is always safe. A bad OUTPACE_CACHE_DIR surfaces here rather than degrading per stream.
    if config.cache_type == CacheType::Disk {
        if config.cache_dir.exists() {
            std::fs::remove_dir_all(&config.cache_dir).map_err(|e| {
                format!("cannot clear OUTPACE_CACHE_DIR {}: {e}", config.cache_dir.display())
            })?;
        }
        std::fs::create_dir_all(&config.cache_dir).map_err(|e| {
            format!("cannot create OUTPACE_CACHE_DIR {}: {e}", config.cache_dir.display())
        })?;
    }
```

- [ ] **Step 6: Run the gates**

Run: `cargo test --workspace` then `cargo clippy --workspace --all-targets`
Expected: PASS (modulo the known mpegts-fixture failure).

- [ ] **Step 7: Commit**

```bash
git add crates/ace-engine/src/ace_provider.rs crates/ace-engine/src/runtime.rs
git commit -m "ace-engine: process-unique disk cache dir + startup root wipe (#36)"
```

---

## Task 2: DiskBackend self-cleaning Drop

**Files:**
- Modify: `crates/ace-swarm/src/store/disk.rs`
- Test: `crates/ace-swarm/src/store/disk.rs` (tests module — create if absent)

- [ ] **Step 1: Write the failing test** — add at the bottom of `disk.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dropping_backend_removes_its_directory() {
        let tmp = std::env::temp_dir().join(format!("outpace-diskdrop-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&tmp);
        let dir = tmp.join("aabb-0");
        {
            let mut b = DiskBackend::new(dir.clone(), 4).unwrap();
            b.put(0, 0, [0u8; 8], b"data");
            assert!(dir.exists(), "dir exists while backend is alive");
        }
        assert!(!dir.exists(), "dir is removed when the backend drops");
        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    fn dropping_one_backend_does_not_touch_another_infohash_generation() {
        let tmp = std::env::temp_dir().join(format!("outpace-diskdrop2-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&tmp);
        let dir_a = tmp.join("aabb-0");
        let dir_b = tmp.join("aabb-1"); // same infohash, newer generation
        let b_new = DiskBackend::new(dir_b.clone(), 4).unwrap();
        {
            let _b_old = DiskBackend::new(dir_a.clone(), 4).unwrap();
        } // old drops here
        assert!(dir_b.exists(), "the newer instance's dir survives the old one's Drop");
        drop(b_new);
        let _ = std::fs::remove_dir_all(&tmp);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p ace-swarm dropping_backend_removes_its_directory`
Expected: FAIL — no `Drop`, so `dir.exists()` is still true after the scope.

- [ ] **Step 3: Implement `Drop`** — add after the `impl DiskBackend` block in `disk.rs`:

```rust
impl Drop for DiskBackend {
    /// Remove this store's private directory. Because the owning `PieceStore` is `Arc`-shared,
    /// this runs only when the last holder (registry entry, in-flight seed peers, ingest) releases
    /// it — so a live writer can never resurrect a dir after teardown, and the per-instance
    /// `-<generation>` name means we only ever delete our own data. Best-effort: a failed unlink is
    /// logged, not fatal. Sync I/O (v1 tradeoff, see module header / #37).
    fn drop(&mut self) {
        if let Err(e) = std::fs::remove_dir_all(&self.dir) {
            if e.kind() != std::io::ErrorKind::NotFound {
                crate::swarm_log!("[cache] disk cleanup failed for {}: {e}", self.dir.display());
            }
        }
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p ace-swarm dropping_backend`
Expected: PASS (both tests).

- [ ] **Step 5: Run the gates**

Run: `cargo test --workspace` then `cargo clippy --workspace --all-targets`
Expected: PASS (modulo known mpegts failure).

- [ ] **Step 6: Commit**

```bash
git add crates/ace-swarm/src/store/disk.rs
git commit -m "ace-swarm: DiskBackend Drop removes its own cache dir (#36)"
```

---

## Task 3: SeedRegistry producer refcount + SeedLease

**Files:**
- Modify: `crates/ace-swarm/src/listen.rs` (SeedEntry, SeedRegistry, new SeedLease)
- Test: `crates/ace-swarm/src/listen.rs` (tests module)

- [ ] **Step 1: Write the failing tests** — add to the tests module in `listen.rs`:

```rust
    #[test]
    fn lease_evicts_entry_when_last_producer_drops() {
        let reg = SeedRegistry::new();
        let ih = [3u8; 20];
        let (_store, lease) = reg.lease_store(ih, || PieceStore::new(4, 4, 1024));
        assert!(reg.serves(&ih), "served while a producer holds the lease");
        drop(lease);
        assert!(!reg.serves(&ih), "entry evicted when the last producer drops");
    }

    #[test]
    fn two_leases_refcount_the_same_entry() {
        let reg = SeedRegistry::new();
        let ih = [4u8; 20];
        let (a, l1) = reg.lease_store(ih, || PieceStore::new(4, 4, 1024));
        let (b, l2) = reg.lease_store(ih, || panic!("second lease must reuse the store"));
        assert!(Arc::ptr_eq(&a, &b), "both leases share one store");
        drop(l1);
        assert!(reg.serves(&ih), "entry survives while the second producer holds it");
        drop(l2);
        assert!(!reg.serves(&ih), "entry evicted only when both producers drop");
    }

    #[test]
    fn broadcast_lease_owns_both_infohash_and_content_id() {
        let reg = SeedRegistry::new();
        let ih = [5u8; 20];
        let cid = [6u8; 20];
        let lease = reg.lease_broadcast(ih, cid, vec![1, 2, 3], || PieceStore::new(4, 4, 1024));
        assert!(reg.serves(&ih) && reg.serves(&cid));
        assert_eq!(&*reg.metadata(&cid).unwrap(), &[1, 2, 3]);
        drop(lease);
        assert!(!reg.serves(&ih), "infohash entry evicted");
        assert!(!reg.serves(&cid), "content_id entry evicted");
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p ace-swarm lease_evicts_entry_when_last_producer_drops`
Expected: FAIL — `lease_store` does not exist.

- [ ] **Step 3: Implement leases** — in `listen.rs`, replace the `SeedEntry` struct and the top of `SeedRegistry` (keep `register`/`register_metadata`/`get`/`metadata`/`serves`/`remove` as-is; replace the KNOWN GAP doc comment):

```rust
/// Who keeps a registry entry alive. `Leech` entries are refcounted by `SeedLease`s (the download
/// loop) and are also eligible for the idle-TTL backstop reaper; `Broadcast` entries are
/// operator-controlled (removed only when their lease drops on DELETE) and exempt from the reaper.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub enum OwnerKind {
    #[default]
    Leech,
    Broadcast,
}

#[derive(Default)]
struct SeedEntry {
    store: Option<SharedStore>,
    metadata: Option<SharedMetadata>,
    /// Number of live `SeedLease`s referencing this key; the entry is removed at zero.
    producers: usize,
    kind: OwnerKind,
}

/// Maps infohash -> the store and/or metadata we'd serve. Entry lifetime is anchored by
/// `SeedLease` (producer refcount): the leech download loop and each broadcast hold a lease and
/// the entry is evicted when the last one drops. An idle-TTL reaper (see `SeedRegistry::reap`)
/// backstops leaked leases for `Leech` entries.
#[derive(Clone, Default)]
pub struct SeedRegistry {
    stores: Arc<StdMutex<HashMap<[u8; 20], SeedEntry>>>,
}
```

Then add the lease methods inside `impl SeedRegistry` (after `get_or_create`):

```rust
    /// Acquire a leech producer lease for `infohash`, creating the store via `make` if absent.
    /// The returned `SeedLease` refcounts the entry; when the last lease for this infohash drops,
    /// the entry (and its store) is evicted. Two concurrent leech sessions for one infohash share
    /// a single store and the entry survives until both leases drop.
    pub fn lease_store(
        &self,
        infohash: [u8; 20],
        make: impl FnOnce() -> PieceStore,
    ) -> (SharedStore, SeedLease) {
        let mut map = self.stores.lock().unwrap();
        let entry = map.entry(infohash).or_default();
        let store = entry
            .store
            .get_or_insert_with(|| Arc::new(Mutex::new(make())))
            .clone();
        entry.producers += 1;
        let lease = SeedLease {
            registry: Arc::downgrade(&self.stores),
            keys: vec![infohash],
        };
        (store, lease)
    }

    /// Acquire a broadcast producer lease owning both the `infohash` (store) and `content_id`
    /// (BEP-9 metadata) keys, creating the store via `make` if absent. Marks the entry
    /// `Broadcast` (reaper-exempt). Dropping the returned lease evicts both keys.
    pub fn lease_broadcast(
        &self,
        infohash: [u8; 20],
        content_id: [u8; 20],
        metadata: Vec<u8>,
        make: impl FnOnce() -> PieceStore,
    ) -> (SharedStore, SeedLease) {
        let mut map = self.stores.lock().unwrap();
        let store = {
            let entry = map.entry(infohash).or_default();
            entry.producers += 1;
            entry.kind = OwnerKind::Broadcast;
            entry
                .store
                .get_or_insert_with(|| Arc::new(Mutex::new(make())))
                .clone()
        };
        {
            let meta_entry = map.entry(content_id).or_default();
            meta_entry.producers += 1;
            meta_entry.kind = OwnerKind::Broadcast;
            meta_entry.metadata = Some(Arc::new(metadata));
        }
        let lease = SeedLease {
            registry: Arc::downgrade(&self.stores),
            keys: vec![infohash, content_id],
        };
        (store, lease)
    }
```

Finally, add the `SeedLease` type at module scope (after the `impl SeedRegistry` block):

```rust
/// RAII producer handle for one or more `SeedRegistry` keys. Dropping it decrements each key's
/// producer count and evicts any entry that reaches zero. Not `Clone` — a clone would need to bump
/// the count; acquire another lease via the registry instead.
pub struct SeedLease {
    registry: std::sync::Weak<StdMutex<HashMap<[u8; 20], SeedEntry>>>,
    keys: Vec<[u8; 20]>,
}

impl Drop for SeedLease {
    fn drop(&mut self) {
        let Some(map) = self.registry.upgrade() else {
            return;
        };
        let mut map = map.lock().unwrap();
        for key in &self.keys {
            if let Some(entry) = map.get_mut(key) {
                entry.producers = entry.producers.saturating_sub(1);
                if entry.producers == 0 {
                    map.remove(key);
                }
            }
        }
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p ace-swarm lease_`
Expected: PASS (all three lease tests + the existing registry tests still green).

- [ ] **Step 5: Run the gates**

Run: `cargo test --workspace` then `cargo clippy --workspace --all-targets`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/ace-swarm/src/listen.rs
git commit -m "ace-swarm: SeedRegistry producer refcount + SeedLease"
```

---

## Task 4: Leech loop holds a SeedLease

**Files:**
- Modify: `crates/ace-engine/src/ace_provider.rs` (`follow_live` store acquisition near line 1326)

**Context:** `follow_live` currently does `let store = seed.registry.get_or_create(info.infohash, || build_piece_store(...))`. The store is used for the whole loop. Holding the lease for the loop's lifetime and dropping it on return is the leak fix.

- [ ] **Step 1: Write the failing test** — add to the `ace_provider.rs` tests module:

```rust
#[tokio::test]
async fn leech_lease_evicts_registry_entry_when_dropped() {
    // Acquire and drop a leech lease directly (mirrors what follow_live does across its lifetime).
    let reg = ace_swarm::listen::SeedRegistry::new();
    let ih = [9u8; 20];
    {
        let (_store, _lease) =
            reg.lease_store(ih, || ace_swarm::store::PieceStore::new(1 << 20, 1 << 14, 1 << 20));
        assert!(reg.serves(&ih), "served while the leech loop holds its lease");
    }
    assert!(!reg.serves(&ih), "entry evicted after the leech loop drops its lease");
}
```

- [ ] **Step 2: Run test to verify it passes for the registry, then wire follow_live**

Run: `cargo test -p ace-engine leech_lease_evicts_registry_entry_when_dropped`
Expected: PASS (this asserts the Task 3 API from the engine crate). If it fails to compile, ensure `PieceStore` and `SeedRegistry` are reachable (`ace_swarm::store::PieceStore`, `ace_swarm::listen::SeedRegistry`).

- [ ] **Step 3: Wire `follow_live`** — replace the store acquisition at `ace_provider.rs:1326`:

```rust
    let (store, _seed_lease) = seed.registry.lease_store(info.infohash, || {
        build_piece_store(
            info.piece_length,
            info.chunk_length,
            seed.store_bytes,
            seed.cache_type,
            &seed.cache_dir,
            &info.infohash,
        )
    });
```

The `_seed_lease` binding lives until `follow_live` returns (when the consumer drops the `AceSource` receiver → the download task ends), at which point its `Drop` evicts the registry entry. Do **not** drop it early or bind it to `_` (which drops immediately). Confirm no `return`/`?` path in `follow_live` moves `store` out in a way that shortens the lease — the lease is independent of `store`, so any early return still drops it correctly.

- [ ] **Step 4: Run the gates**

Run: `cargo test --workspace` then `cargo clippy --workspace --all-targets`
Expected: PASS. Watch for an "unused variable `_seed_lease`" lint — the leading underscore suppresses it while keeping the value alive.

- [ ] **Step 5: Commit**

```bash
git add crates/ace-engine/src/ace_provider.rs
git commit -m "ace-engine: leech loop holds a SeedLease so its registry entry is evicted on teardown"
```

---

## Task 5: Broadcast holds a lease; drop remove_cache_dir

**Files:**
- Modify: `crates/ace-engine/src/broadcast.rs` (`by_name` map type, `start_or_resume`, `reconstruct`, `delete`, `get`, remove `remove_cache_dir`)
- Modify: `crates/ace-engine/src/http.rs:206-211` (`broadcast_delete`)
- Modify: `README.md`
- Test: `crates/ace-engine/src/broadcast.rs` (tests module)

**Context:** `Broadcast` is `Clone` and shared with the ingest task, so the lease (not `Clone`) cannot live inside it. Store the lease in a non-clonable wrapper kept in `by_name`; `get`/callers still receive a `Broadcast` clone. Removing the entry from `by_name` drops the lease → evicts both registry keys. The existing `cursor.mark_removed()` already stops writers persisting; combined with the store's `Drop` cleanup (Task 2), an in-flight ingest can no longer resurrect the cache dir.

- [ ] **Step 1: Write the failing test** — replace the disk-cleanup test `deleting_a_broadcast_removes_its_disk_cache_dir` (around broadcast.rs:460-500; it currently calls `remove_cache_dir`) with one that drives the lease path:

```rust
    #[tokio::test]
    async fn deleting_a_broadcast_evicts_its_seed_registry_entries() {
        let dir = unique_dir("bc-delete-evicts");
        let data_dir = dir.join("data");
        std::fs::create_dir_all(&data_dir).unwrap();
        let cache_dir = dir.join("cache");
        let reg = BroadcastRegistry::with_persist(&data_dir, CacheType::Memory, cache_dir);
        let seed = SeedRegistry::new();
        let (bc, _) = reg
            .start_or_resume("chan", "Chan", &[], &seed, 1 << 20)
            .await;
        assert!(seed.serves(&bc.infohash) && seed.serves(&bc.content_id));

        reg.delete("chan").await;
        assert!(!seed.serves(&bc.infohash), "infohash entry evicted on delete");
        assert!(!seed.serves(&bc.content_id), "content_id entry evicted on delete");
        let _ = std::fs::remove_dir_all(&dir);
    }
```

Add a small `unique_dir` helper to the tests module if one is not already present:

```rust
    fn unique_dir(tag: &str) -> std::path::PathBuf {
        let d = std::env::temp_dir().join(format!("outpace-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        d
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p ace-engine deleting_a_broadcast_evicts_its_seed_registry_entries`
Expected: FAIL to compile / assert — `start_or_resume` still uses `get_or_create` and `delete` doesn't drop a lease, so the seed entries persist after delete.

- [ ] **Step 3: Change the `by_name` map to hold leases** — in `broadcast.rs`:

Add the wrapper near the `Broadcast` struct:

```rust
/// A minted broadcast plus the `SeedLease` anchoring its `SeedRegistry` entries. The lease is kept
/// out of the cloneable `Broadcast` (ingest holds a clone) so that removing this wrapper from
/// `by_name` — and nothing else — evicts the registry entries.
struct BroadcastEntry {
    broadcast: Broadcast,
    _lease: ace_swarm::listen::SeedLease,
}
```

Change the field type:

```rust
    by_name: Mutex<BTreeMap<String, BroadcastEntry>>,
```

Update `get`:

```rust
    pub async fn get(&self, name: &str) -> Option<Broadcast> {
        self.by_name.lock().await.get(name).map(|e| e.broadcast.clone())
    }
```

Update `start_or_resume`: the early-return existing check, the store acquisition, and the insert:

```rust
        if let Some(existing) = map.get(name) {
            return (existing.broadcast.clone(), false);
        }
```

Replace `register_metadata` + `get_or_create` with `lease_broadcast`:

```rust
        let (store, lease) = seed_registry.lease_broadcast(
            infohash,
            content_id,
            transport_bytes.clone(),
            || {
                build_piece_store(
                    PIECE_LENGTH,
                    CHUNK_LENGTH,
                    store_bytes,
                    self.cache_type,
                    &self.cache_dir,
                    &infohash,
                )
            },
        );
```

And the insert at the end of `start_or_resume`:

```rust
        map.insert(
            name.to_string(),
            BroadcastEntry { broadcast: broadcast.clone(), _lease: lease },
        );
        (broadcast, true)
```

- [ ] **Step 4: Update `reconstruct` and `reload_persisted`** — `reconstruct` must return the lease alongside the broadcast. Change its signature and body:

```rust
    fn reconstruct(
        &self,
        name: &str,
        rec: &PersistedBroadcast,
        seed_registry: &SeedRegistry,
        store_bytes: u64,
    ) -> Option<(Broadcast, ace_swarm::listen::SeedLease)> {
        let auth = LiveSourceAuth::from_pkcs1_pem(&rec.key_pkcs1_pem).ok()?;
        let decoded = decode_transport(&rec.transport).ok()?;
        if decoded.pubkey != auth.pubkey_der() {
            return None;
        }
        let infohash = infohash_of_transport(&rec.transport);
        let content_id = transport_file_hash(&rec.transport);
        let piece_length = decoded.piece_length;
        let chunk_length = decoded.chunk_length;
        let (store, lease) = seed_registry.lease_broadcast(
            infohash,
            content_id,
            rec.transport.clone(),
            || {
                build_piece_store(
                    piece_length,
                    chunk_length,
                    store_bytes,
                    self.cache_type,
                    &self.cache_dir,
                    &infohash,
                )
            },
        );
        let resume_at = rec.next_piece.saturating_add(CURSOR_PERSIST_INTERVAL);
        let cursor = BroadcastCursor::new(
            resume_at,
            self.sink_for(name, &rec.transport, rec.key_pkcs1_pem.clone()),
        );
        Some((
            Broadcast {
                infohash,
                content_id,
                transport_bytes: Arc::new(rec.transport.clone()),
                store,
                auth: Arc::new(auth),
                cursor,
            },
            lease,
        ))
    }
```

Update the two `reconstruct` call sites. In `start_or_resume`:

```rust
        if let Some(persist) = &self.persist {
            if let Some(rec) = persist.load(name) {
                if let Some((bc, lease)) = self.reconstruct(name, &rec, seed_registry, store_bytes) {
                    map.insert(
                        name.to_string(),
                        BroadcastEntry { broadcast: bc.clone(), _lease: lease },
                    );
                    return (bc, false);
                }
                crate::alog!("[broadcast] {name}: persisted record invalid; re-minting");
            }
        }
```

In `reload_persisted`:

```rust
            match self.reconstruct(&name, &rec, seed_registry, store_bytes) {
                Some((bc, lease)) => {
                    map.insert(
                        name.clone(),
                        BroadcastEntry { broadcast: bc.clone(), _lease: lease },
                    );
                    out.push(bc);
                }
                None => crate::alog!("[broadcast] {name}: persisted record invalid; skipping"),
            }
```

- [ ] **Step 5: Simplify `delete` and remove `remove_cache_dir`** — `delete` now evicts the registry entries by dropping the wrapper's lease; it still marks the cursor removed and deletes the persisted record:

```rust
    /// Forget `name`: drop it from memory (dropping its `SeedLease`, which evicts its `SeedRegistry`
    /// entries and, in disk mode, lets the store's `Drop` clean the cache dir once ingest releases
    /// its `Arc`) and delete its persisted record. Marks the cursor removed so a stale in-flight
    /// ingest can't rewrite the file. Returns the removed broadcast.
    pub async fn delete(&self, name: &str) -> Option<Broadcast> {
        let removed = self.by_name.lock().await.remove(name);
        if let Some(entry) = &removed {
            entry.broadcast.cursor.mark_removed();
        }
        if let Some(persist) = &self.persist {
            if let Err(e) = persist.delete(name) {
                crate::alog!("[broadcast] {name}: delete failed: {e}");
            }
        }
        removed.map(|e| e.broadcast)
    }
```

Delete the entire `remove_cache_dir` method (broadcast.rs:181-188).

- [ ] **Step 6: Update `broadcast_delete` in http.rs** — replace the body block that calls the removed methods:

```rust
    if bs.registry.delete(&name).await.is_some() {
        // Registry-entry eviction (and disk-dir cleanup) now ride the broadcast's SeedLease drop
        // inside `registry.delete` — no explicit seed_registry.remove / remove_cache_dir needed.
        crate::alog!("[broadcast] {name}: deleted");
    }
```

- [ ] **Step 7: Fix any remaining `remove_cache_dir` references** — search and remove/adjust:

Run: `grep -rn "remove_cache_dir\|get_or_create" crates/ace-engine/src`
Expected after edits: no production references (test-only `get_or_create` on `SeedRegistry` is fine; there should be no `remove_cache_dir` left). Update any test that called `remove_cache_dir` to assert dir cleanup via the store-drop path instead, or delete it if now covered by Task 2.

- [ ] **Step 8: Run tests**

Run: `cargo test -p ace-engine broadcast`
Expected: PASS (the new eviction test + existing broadcast tests, adjusted for the `BroadcastEntry` map).

- [ ] **Step 9: README note** — in the disk-cache section of `README.md`, update the directory-name description:

```markdown
In disk mode each served stream keeps its pieces under
`<OUTPACE_CACHE_DIR>/<infohash_hex>-<generation>` (a process-unique suffix per store instance).
The directory is removed automatically when the stream is torn down (leech consumer disconnects,
broadcast `DELETE`, or process exit), and the whole cache root is wiped on startup, so no
per-stream directories accumulate.
```

- [ ] **Step 10: Run the gates**

Run: `cargo test --workspace` then `cargo clippy --workspace --all-targets`
Expected: PASS.

- [ ] **Step 11: Commit**

```bash
git add crates/ace-engine/src/broadcast.rs crates/ace-engine/src/http.rs README.md
git commit -m "ace-engine: broadcasts hold a SeedLease; drop bespoke remove_cache_dir (#4, #36)"
```

---

## Task 6: Idle-TTL reaper backstop

**Files:**
- Modify: `crates/ace-engine/src/config.rs` (new `seed_ttl` field + env var)
- Modify: `crates/ace-engine/src/runtime.rs` (parse env; spawn reaper)
- Modify: `crates/ace-swarm/src/listen.rs` (`last_active` on entry; `touch` on `get`; `reap` method)
- Test: `crates/ace-swarm/src/listen.rs` tests

**Context:** The lease is the primary mechanism; this reaper force-evicts `Leech` entries idle beyond a TTL (a leaked lease). `Broadcast` entries are exempt.

- [ ] **Step 1: Write the failing test** — add to `listen.rs` tests:

```rust
    #[test]
    fn reaper_evicts_idle_leech_entries_but_spares_broadcasts() {
        let reg = SeedRegistry::new();
        let leech = [1u8; 20];
        let bcast_ih = [2u8; 20];
        let bcast_cid = [3u8; 20];
        // Leak the leases so only the reaper can reclaim them.
        std::mem::forget(reg.lease_store(leech, || PieceStore::new(4, 4, 1024)).1);
        std::mem::forget(reg.lease_broadcast(bcast_ih, bcast_cid, vec![9], || PieceStore::new(4, 4, 1024)).1);

        // A zero TTL makes every entry "idle now".
        let evicted = reg.reap(std::time::Duration::from_secs(0));
        assert_eq!(evicted, 1, "exactly the leech entry is reaped");
        assert!(!reg.serves(&leech), "idle leech entry evicted");
        assert!(reg.serves(&bcast_ih), "broadcast infohash exempt");
        assert!(reg.serves(&bcast_cid), "broadcast content_id exempt");
    }

    #[test]
    fn get_touches_last_active_so_a_served_entry_is_not_reaped() {
        let reg = SeedRegistry::new();
        let ih = [8u8; 20];
        std::mem::forget(reg.lease_store(ih, || PieceStore::new(4, 4, 1024)).1);
        // A generous TTL: the freshly-touched entry is not yet idle.
        let _ = reg.get(&ih); // touch
        let evicted = reg.reap(std::time::Duration::from_secs(3600));
        assert_eq!(evicted, 0);
        assert!(reg.serves(&ih));
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p ace-swarm reaper_evicts_idle_leech`
Expected: FAIL — `reap` does not exist.

- [ ] **Step 3: Add `last_active` + `touch` + `reap`** — in `listen.rs`:

Add the field to `SeedEntry`:

```rust
struct SeedEntry {
    store: Option<SharedStore>,
    metadata: Option<SharedMetadata>,
    producers: usize,
    kind: OwnerKind,
    /// Wall-clock millis of the last `get`/create touch, for the idle-TTL reaper.
    last_active: u64,
}
```

Add a monotonic-ish timestamp helper and set `last_active` on create in `lease_store`/`lease_broadcast` (add `entry.last_active = now_millis();` right after `entry.producers += 1;` in each). Add near the top of the module:

```rust
fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}
```

Make `get` touch the entry:

```rust
    /// The store for `infohash`, if we serve it. Touches the entry's activity clock (used by the
    /// idle-TTL reaper) so an actively-served stream is never reaped.
    pub fn get(&self, infohash: &[u8; 20]) -> Option<SharedStore> {
        let mut map = self.stores.lock().unwrap();
        let entry = map.get_mut(infohash)?;
        entry.last_active = now_millis();
        entry.store.clone()
    }
```

Add `reap`:

```rust
    /// Force-evict `Leech` entries idle for longer than `ttl` (a leaked-lease backstop; `Broadcast`
    /// entries are operator-controlled and exempt). Returns how many entries were removed. The
    /// primary teardown path is `SeedLease` drop; this only catches producers that never dropped.
    pub fn reap(&self, ttl: std::time::Duration) -> usize {
        let now = now_millis();
        let ttl_ms = ttl.as_millis() as u64;
        let mut map = self.stores.lock().unwrap();
        let before = map.len();
        map.retain(|_, e| {
            e.kind == OwnerKind::Broadcast || now.saturating_sub(e.last_active) < ttl_ms
        });
        before - map.len()
    }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p ace-swarm reaper_evicts_idle_leech get_touches_last_active`
Expected: PASS.

- [ ] **Step 5: Add the config field** — in `config.rs`, add to `Config` (after `max_inbound_peers`):

```rust
    /// Idle-TTL (seconds) after which a leech `SeedRegistry` entry with a leaked producer lease is
    /// force-evicted by the reaper. Broadcasts are exempt. Backstop only — normal teardown rides
    /// the lease. `OUTPACE_SEED_TTL_SECS`.
    pub seed_ttl_secs: u64,
```

And in `Default`:

```rust
            seed_ttl_secs: 300,
```

- [ ] **Step 6: Parse the env var + spawn the reaper** — in `runtime.rs`. Add the env parse near the other `OUTPACE_*` parses (around line 76):

```rust
    if let Ok(v) = std::env::var("OUTPACE_SEED_TTL_SECS") {
        config.seed_ttl_secs = v.parse()?;
    }
```

In `build_runtime`, after `manager.spawn_reaper();`, spawn the seed-registry reaper (skip when TTL is 0 = disabled):

```rust
    if config.seed_ttl_secs > 0 {
        let seed_registry_reap = seed_registry.clone();
        let ttl = std::time::Duration::from_secs(config.seed_ttl_secs);
        tokio::spawn(async move {
            // Sweep at a fraction of the TTL so an idle entry is reclaimed within ~1.25x the TTL.
            let interval = (ttl / 4).max(std::time::Duration::from_secs(5));
            loop {
                tokio::time::sleep(interval).await;
                let n = seed_registry_reap.reap(ttl);
                if n > 0 {
                    crate::alog!("[seed] reaped {n} idle leech registry entr(y/ies)");
                }
            }
        });
    }
```

- [ ] **Step 7: Config default test** — add to `config.rs` tests, extend `default_config_has_seeding_and_inbound_on` or add:

```rust
    #[test]
    fn default_seed_ttl_is_300s() {
        assert_eq!(Config::default().seed_ttl_secs, 300);
    }
```

- [ ] **Step 8: Run the gates**

Run: `cargo test --workspace` then `cargo clippy --workspace --all-targets`
Expected: PASS.

- [ ] **Step 9: Commit**

```bash
git add crates/ace-swarm/src/listen.rs crates/ace-engine/src/config.rs crates/ace-engine/src/runtime.rs
git commit -m "ace-swarm/ace-engine: idle-TTL reaper backstop for leaked leech leases"
```

---

## Task 7: Record the enable_inbound decision

**Files:**
- Modify: `README.md`
- Modify: `crates/ace-engine/src/config.rs` (doc-comment tweak only, optional)

- [ ] **Step 1: README note** — in the config/deployment section of `README.md`, add:

```markdown
### `OUTPACE_ENABLE_INBOUND` (default: on)

Inbound peer serving (S2) is **on by default**, intentionally matching the Acestream engine's
out-of-the-box behavior: a full P2P participant that binds its peer port (`OUTPACE_PEER_LISTEN`),
accepts inbound peers, seeds, and self-announces to trackers + DHT. Only the HTTP API
(`OUTPACE_BIND`) stays on localhost by default; the exposed surface is the peer port, as with
Acestream. Set `OUTPACE_ENABLE_INBOUND=0` for a pure-leecher deployment (no inbound listener, no
seeder self-announce).
```

- [ ] **Step 2: Verify the existing default test still asserts ON**

Run: `cargo test -p ace-engine default_config_has_seeding_and_inbound_on`
Expected: PASS (already asserts `c.enable_inbound`).

- [ ] **Step 3: Commit**

```bash
git add README.md crates/ace-engine/src/config.rs
git commit -m "docs: record enable_inbound default-on as an intentional decision (#4)"
```

---

# PHASE 2 — Serve coordinator (max_unchoked)

## Task 8: ServeCoordinator core

**Files:**
- Modify: `crates/ace-swarm/src/seed.rs` (new `ServeCoordinator` + `PeerHandle`, alongside `Choker`)
- Test: `crates/ace-swarm/src/seed.rs` tests

**Context:** One coordinator per infohash. It tracks the interested set (stable order) and drives `Choker` to bound the unchoked set to `max_unchoked` (+1 rotating optimistic). Each connection gets a `watch::Receiver<bool>` (true = unchoked) it observes; the coordinator flips senders when the chosen set changes.

- [ ] **Step 1: Write the failing tests** — add to `seed.rs` tests:

```rust
    #[test]
    fn coordinator_never_unchokes_more_than_max_plus_optimistic() {
        let coord = ServeCoordinator::new(2);
        let mut rxs = Vec::new();
        let mut ids = Vec::new();
        for _ in 0..5 {
            let (id, rx) = coord.join();
            ids.push(id);
            rxs.push(rx);
        }
        for id in &ids {
            coord.set_interested(*id, true);
        }
        let unchoked = rxs.iter().filter(|rx| *rx.borrow()).count();
        assert!(unchoked <= 3, "max_unchoked(2) + 1 optimistic = 3, got {unchoked}");
        assert!(unchoked >= 2, "should unchoke up to the cap when enough are interested");
    }

    #[test]
    fn coordinator_unchokes_a_single_interested_peer() {
        let coord = ServeCoordinator::new(4);
        let (id, rx) = coord.join();
        assert!(!*rx.borrow(), "not unchoked before Interested");
        coord.set_interested(id, true);
        assert!(*rx.borrow(), "unchoked after Interested when under the cap");
    }

    #[test]
    fn coordinator_rechokes_when_a_peer_leaves() {
        let coord = ServeCoordinator::new(1);
        let (a, rx_a) = coord.join();
        let (b, rx_b) = coord.join();
        coord.set_interested(a, true);
        coord.set_interested(b, true);
        // With max_unchoked=1, first-come `a` is unchoked; `b` is the optimistic (+1) slot.
        assert!(*rx_a.borrow());
        coord.leave(a);
        // `b` must now be in the guaranteed slot.
        assert!(*rx_b.borrow(), "remaining interested peer is unchoked after the other leaves");
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p ace-swarm coordinator_`
Expected: FAIL — `ServeCoordinator` does not exist.

- [ ] **Step 3: Implement `ServeCoordinator`** — add to `seed.rs` (after `Choker`):

```rust
use std::collections::HashMap as StdHashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex as StdMutex;
use tokio::sync::watch;

/// Per-infohash multi-peer serve coordinator. Tracks every inbound connection for one stream and
/// applies [`Choker`] so no more than `max_unchoked` (+1 rotating optimistic) peers are unchoked at
/// once. Each connection observes a `watch<bool>` (true = unchoked) and sends Choke/Unchoke on the
/// wire when it flips. Recompute runs on every interest change, peer leave, and rechoke tick.
pub struct ServeCoordinator {
    choker: Choker,
    next_id: AtomicU64,
    state: StdMutex<CoordState>,
}

#[derive(Default)]
struct CoordState {
    /// Interested peers in stable (join) order — the order `Choker::choose` consumes.
    interested: Vec<u64>,
    /// Per-peer unchoke signal sender.
    senders: StdHashMap<u64, watch::Sender<bool>>,
    tick: u64,
}

impl ServeCoordinator {
    pub fn new(max_unchoked: usize) -> Arc<Self> {
        Arc::new(ServeCoordinator {
            choker: Choker::new(max_unchoked),
            next_id: AtomicU64::new(1),
            state: StdMutex::new(CoordState::default()),
        })
    }

    /// Register a connection. Returns its peer id and a receiver that reports its unchoke state
    /// (starts choked). Call [`leave`](Self::leave) when the connection ends.
    pub fn join(&self) -> (u64, watch::Receiver<bool>) {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = watch::channel(false);
        self.state.lock().unwrap().senders.insert(id, tx);
        (id, rx)
    }

    /// Report a peer's interest. Recomputes the unchoked set.
    pub fn set_interested(&self, peer: u64, interested: bool) {
        {
            let mut st = self.state.lock().unwrap();
            let present = st.interested.iter().position(|&p| p == peer);
            match (interested, present) {
                (true, None) => st.interested.push(peer),
                (false, Some(i)) => {
                    st.interested.remove(i);
                }
                _ => {}
            }
        }
        self.recompute();
    }

    /// Deregister a connection (also drops it from the interested set). Recomputes.
    pub fn leave(&self, peer: u64) {
        {
            let mut st = self.state.lock().unwrap();
            st.senders.remove(&peer);
            if let Some(i) = st.interested.iter().position(|&p| p == peer) {
                st.interested.remove(i);
            }
        }
        self.recompute();
    }

    /// Advance the optimistic-unchoke rotation and recompute. Called periodically by the listener's
    /// rechoke ticker (via `SeedRegistry::rechoke_all`).
    pub fn rechoke(&self) {
        self.state.lock().unwrap().tick += 1;
        self.recompute();
    }

    /// Apply the choker and flip each peer's watch to its desired state. `watch::send` only wakes
    /// receivers when the value actually changes, so unchanged peers cost nothing.
    fn recompute(&self) {
        let st = self.state.lock().unwrap();
        let chosen = self.choker.choose(&st.interested, st.tick);
        for (id, tx) in st.senders.iter() {
            let want = chosen.contains(id);
            if *tx.borrow() != want {
                let _ = tx.send(want);
            }
        }
    }
}
```

Note: `Choker::choose` takes `&[u64]` and a `tick` — already defined. `Arc` is already imported in `seed.rs`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p ace-swarm coordinator_`
Expected: PASS (all three).

- [ ] **Step 5: Run the gates**

Run: `cargo test --workspace` then `cargo clippy --workspace --all-targets`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/ace-swarm/src/seed.rs
git commit -m "ace-swarm: ServeCoordinator — per-stream max_unchoked policy over the Choker"
```

---

## Task 9: Registry stores a coordinator per entry

**Files:**
- Modify: `crates/ace-swarm/src/listen.rs` (SeedEntry `coordinator` field; `coordinator_for`; `rechoke_all`)
- Test: `crates/ace-swarm/src/listen.rs` tests

- [ ] **Step 1: Write the failing test**:

```rust
    #[test]
    fn coordinator_for_is_stable_per_infohash() {
        let reg = SeedRegistry::new();
        let ih = [11u8; 20];
        let (_s, _l) = reg.lease_store(ih, || PieceStore::new(4, 4, 1024));
        let c1 = reg.coordinator_for(&ih, 4).unwrap();
        let c2 = reg.coordinator_for(&ih, 4).unwrap();
        assert!(Arc::ptr_eq(&c1, &c2), "one coordinator per served infohash");
        assert!(
            reg.coordinator_for(&[99u8; 20], 4).is_none(),
            "no coordinator for an unserved infohash"
        );
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p ace-swarm coordinator_for_is_stable_per_infohash`
Expected: FAIL — `coordinator_for` does not exist.

- [ ] **Step 3: Implement** — in `listen.rs`, add the import and field, then the methods.

Add near the top: `use crate::seed::ServeCoordinator;`

Add to `SeedEntry`:

```rust
    /// Lazily-created multi-peer serve coordinator (S2). Shares the entry's lifetime.
    coordinator: Option<Arc<ServeCoordinator>>,
```

Add methods to `impl SeedRegistry`:

```rust
    /// The serve coordinator for `infohash`, creating it (with `max_unchoked`) on first use.
    /// Returns `None` if we don't serve this infohash (entry absent), so a racing eviction can't
    /// resurrect a bare entry.
    pub fn coordinator_for(
        &self,
        infohash: &[u8; 20],
        max_unchoked: usize,
    ) -> Option<Arc<ServeCoordinator>> {
        let mut map = self.stores.lock().unwrap();
        let entry = map.get_mut(infohash)?;
        Some(
            entry
                .coordinator
                .get_or_insert_with(|| ServeCoordinator::new(max_unchoked))
                .clone(),
        )
    }

    /// Advance every live coordinator's optimistic-unchoke rotation. Called by the listener's
    /// periodic rechoke ticker.
    pub fn rechoke_all(&self) {
        let coords: Vec<_> = {
            let map = self.stores.lock().unwrap();
            map.values().filter_map(|e| e.coordinator.clone()).collect()
        };
        for c in coords {
            c.rechoke();
        }
    }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p ace-swarm coordinator_for_is_stable_per_infohash`
Expected: PASS.

- [ ] **Step 5: Run the gates**

Run: `cargo test --workspace` then `cargo clippy --workspace --all-targets`
Expected: PASS. (If a cyclic-use lint appears from `listen.rs` importing `seed::ServeCoordinator`, confirm both modules are siblings under `ace-swarm` — they are — so no cycle.)

- [ ] **Step 6: Commit**

```bash
git add crates/ace-swarm/src/listen.rs
git commit -m "ace-swarm: SeedRegistry holds a per-infohash ServeCoordinator"
```

---

## Task 10: SeederSession::serve honors the coordinator

**Files:**
- Modify: `crates/ace-swarm/src/seed.rs` (`SeederSession::serve` signature + loop)
- Modify: `crates/ace-swarm/src/listen.rs` (`handle_inbound` passes the coordinator)
- Test: `crates/ace-swarm/src/seed.rs` tests

**Context:** `serve` gains an `Option<Arc<ServeCoordinator>>`. When `Some`, it registers, reports interest to the coordinator instead of unchoking inline, and only answers chunk requests while its watch says unchoked. When `None`, it keeps today's inline-unchoke behavior (preserves existing single-peer serve tests). Deregistration on any exit path via an RAII guard.

- [ ] **Step 1: Write the failing test** — an integration-style test that a coordinator-gated serve does not send `Unchoke` until the coordinator chooses the peer. Add to `seed.rs` tests (mirror the existing `serves_ut_metadata_piece_when_metadata_is_registered` harness for wiring a `PeerSession` over a duplex pipe):

```rust
    #[tokio::test]
    async fn coordinator_gated_serve_unchokes_only_when_chosen() {
        use tokio::io::duplex;
        // max_unchoked = 0 means the guaranteed set is empty; with one interested peer the
        // optimistic (+1) slot still unchokes it — so use a peer that is NOT interested to assert
        // it stays choked, then a second run where it becomes interested.
        let coord = ServeCoordinator::new(1);
        let store = Arc::new(Mutex::new(PieceStore::new(1 << 20, 1 << 14, 1 << 20)));
        let identity = Identity::from_seed([1u8; 32]);
        let (client, server) = duplex(64 * 1024);

        let coord_srv = coord.clone();
        let srv = tokio::spawn(async move {
            let mut session = PeerSession::new(server);
            let _ = SeederSession::serve(
                &mut session,
                Some(store),
                None,
                [0u8; 8],
                &identity,
                [0, 0, 0, 0],
                Some(coord_srv),
            )
            .await;
        });

        // Drive the client past the handshake, then send Interested and observe an Unchoke.
        let mut client = PeerSession::new(client);
        // (Reuse the existing test helper that performs the leech-side extended handshake read;
        //  see serves_ut_metadata_piece_when_metadata_is_registered for the exact calls.)
        // ... perform handshake ...
        client.send(&PeerMessage::Interested).await.unwrap();
        // The coordinator has one guaranteed slot (max_unchoked=1); this lone interested peer is
        // chosen, so an Unchoke must arrive.
        let mut saw_unchoke = false;
        for _ in 0..8 {
            match tokio::time::timeout(std::time::Duration::from_millis(200), client.read_message()).await {
                Ok(Ok(PeerMessage::Unchoke)) => { saw_unchoke = true; break; }
                Ok(Ok(_)) => continue,
                _ => break,
            }
        }
        assert!(saw_unchoke, "coordinator chose the lone interested peer; serve must Unchoke it");
        srv.abort();
    }
```

Note: fill the handshake `...` using the exact calls from the sibling `serves_ut_metadata_piece_when_metadata_is_registered` test (it already establishes a `PeerSession` and reads the signed extended handshake). Keep the assertion focused on the Unchoke transition.

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p ace-swarm coordinator_gated_serve_unchokes_only_when_chosen`
Expected: FAIL to compile — `serve` has no `coordinator` parameter.

- [ ] **Step 3: Add the coordinator parameter + RAII leave guard** — change `SeederSession::serve`'s signature (add the trailing param) and its unchoke handling:

Signature:

```rust
    #[allow(clippy::too_many_arguments)]
    pub async fn serve<S: AsyncRead + AsyncWrite + Unpin>(
        session: &mut PeerSession<S>,
        store: Option<Arc<Mutex<PieceStore>>>,
        metadata: Option<Arc<Vec<u8>>>,
        piece_header: [u8; 8],
        identity: &Identity,
        peer_ip: [u8; 4],
        coordinator: Option<Arc<ServeCoordinator>>,
    ) -> Result<()> {
```

Right after the signed extended handshake is sent (after the `send_signed_extended_handshake` call, before `let mut unchoked = false;`), register with the coordinator and install a leave guard:

```rust
        // Coordinator-gated (S2 multi-peer) unchoke, or legacy inline unchoke when absent.
        let mut coord_rx = coordinator.as_ref().map(|c| c.join());
        let peer_id = coord_rx.as_ref().map(|(id, _)| *id);
        // Deregister on every exit path (error, close, drop).
        struct LeaveGuard(Option<(Arc<ServeCoordinator>, u64)>);
        impl Drop for LeaveGuard {
            fn drop(&mut self) {
                if let Some((c, id)) = self.0.take() {
                    c.leave(id);
                }
            }
        }
        let _leave = LeaveGuard(match (&coordinator, peer_id) {
            (Some(c), Some(id)) => Some((c.clone(), id)),
            _ => None,
        });
        let mut unchoked = false;
```

Replace the `PeerMessage::Interested if !unchoked` arm with interest reporting (coordinator) or inline unchoke (legacy):

```rust
                PeerMessage::Interested => {
                    match (&coordinator, peer_id) {
                        (Some(c), Some(id)) => c.set_interested(id, true),
                        _ => {
                            if !unchoked {
                                session.send(&PeerMessage::Unchoke).await?;
                                unchoked = true;
                                if debug {
                                    crate::swarm_log!("[seed-session] -> Unchoke");
                                }
                            }
                        }
                    }
                }
                PeerMessage::NotInterested => {
                    if let (Some(c), Some(id)) = (&coordinator, peer_id) {
                        c.set_interested(id, false);
                    }
                }
```

Gate the chunk-request answer on `unchoked` (add the guard to the existing `Unknown { id: 6, .. }` arm — a choked peer's requests are dropped):

```rust
                PeerMessage::Unknown { id: 6, payload } if payload.len() >= 10 && unchoked => {
```

Convert the read loop to a `select!` that also observes coordinator unchoke flips. Replace `let msg = session.read_message().await?;` at the top of the loop with:

```rust
            let msg = tokio::select! {
                m = session.read_message() => m?,
                changed = async {
                    coord_rx.as_mut().unwrap().1.changed().await
                }, if coord_rx.is_some() => {
                    changed.map_err(|_| ace_peer::PeerError::InfohashMismatch)?; // sender dropped: entry gone
                    let want = *coord_rx.as_ref().unwrap().1.borrow();
                    if want != unchoked {
                        unchoked = want;
                        session
                            .send(&if want { PeerMessage::Unchoke } else { PeerMessage::Choke })
                            .await?;
                        if debug {
                            crate::swarm_log!("[seed-session] -> {}", if want { "Unchoke" } else { "Choke" });
                        }
                    }
                    continue;
                }
            };
```

(If `ace_peer::PeerError` has a more suitable "closed" variant than `InfohashMismatch`, use it; the mapping only matters when the coordinator/entry is torn down mid-serve, which ends the session cleanly.)

- [ ] **Step 4: Update `handle_inbound` to pass a coordinator** — in `listen.rs`, thread `max_unchoked` into `PeerListener::serve` and `handle_inbound`, and fetch the coordinator:

Change `PeerListener::serve`'s signature to accept `max_unchoked: usize`, store it, and pass it into `handle_inbound`. Then in `handle_inbound`:

```rust
    let store = registry.get(&peer_hs.infohash);
    let metadata = registry.metadata(&peer_hs.infohash);
    if store.is_none() && metadata.is_none() {
        return Err(ace_peer::PeerError::InfohashMismatch);
    }
    let coordinator = registry.coordinator_for(&peer_hs.infohash, max_unchoked);
    SeederSession::serve(
        &mut session,
        store,
        metadata,
        piece_header,
        identity,
        peer_ip,
        coordinator,
    )
    .await
```

Add `max_unchoked: usize` to `handle_inbound`'s parameters and pass it from the accept loop (capture it into the spawned task like `registry`/`identity`).

- [ ] **Step 5: Fix other `SeederSession::serve` callers** — search and add the `None` coordinator arg where serve is called outside the listener (e.g. existing seed.rs tests):

Run: `grep -rn "SeederSession::serve" crates`
For each non-coordinator caller (existing tests), append `, None` as the final argument.

- [ ] **Step 6: Run tests**

Run: `cargo test -p ace-swarm seed::` then `cargo test -p ace-swarm coordinator_gated_serve`
Expected: PASS (new test + all existing seed tests, now passing `None`).

- [ ] **Step 7: Run the gates**

Run: `cargo test --workspace` then `cargo clippy --workspace --all-targets`
Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add crates/ace-swarm/src/seed.rs crates/ace-swarm/src/listen.rs
git commit -m "ace-swarm: SeederSession honors the per-stream ServeCoordinator (max_unchoked)"
```

---

## Task 11: Wire max_unchoked + rechoke ticker through the daemon

**Files:**
- Modify: `crates/ace-engine/src/runtime.rs` (pass `max_unchoked` to `PeerListener::serve`; spawn the rechoke ticker)
- Modify: `crates/ace-swarm/src/listen.rs` (`PeerListener::serve` spawns/accepts the rechoke cadence — or the ticker lives in runtime)
- Test: `crates/ace-engine/src/runtime.rs` tests (smoke)

**Context:** The accept loop already exists in `runtime.rs` around line 247-268 calling `PeerListener::serve(...)`. Pass `config.max_unchoked` and spawn a periodic `seed_registry.rechoke_all()` task.

- [ ] **Step 1: Pass `max_unchoked` into the listener** — in `runtime.rs` where `PeerListener::serve` is invoked, add `config.max_unchoked` as the new argument (matching Task 10's signature change). Example shape:

```rust
        let max_unchoked = config.max_unchoked;
        tokio::spawn(async move {
            ace_swarm::listen::PeerListener::serve(
                peer_listener,
                seed_registry_for_listener,
                our_peer_id,
                piece_header,
                max_inbound,
                identity_for_listener,
                max_unchoked,
            )
            .await;
        });
```

(Adjust the exact captured-variable names to those already used in the block at runtime.rs:247-268.)

- [ ] **Step 2: Spawn the rechoke ticker** — in `build_runtime` (only meaningful when inbound is enabled, but harmless otherwise since no coordinators exist), after the seed reaper spawn:

```rust
    if config.enable_inbound {
        let rechoke_registry = seed_registry.clone();
        tokio::spawn(async move {
            // BitTorrent's classic rechoke cadence is ~10s; rotate the optimistic slot on that beat.
            let mut ticker = tokio::time::interval(std::time::Duration::from_secs(10));
            loop {
                ticker.tick().await;
                rechoke_registry.rechoke_all();
            }
        });
    }
```

- [ ] **Step 3: Smoke test** — extend the existing `build_runtime_*` test (or add one) to assert the daemon builds with a small `max_unchoked` and inbound on:

```rust
    #[tokio::test]
    async fn build_runtime_wires_max_unchoked_without_error() {
        let dir = std::env::temp_dir().join(format!("outpace-rt-mu-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let mut config = Config::default();
        config.data_dir = dir.clone();
        config.cache_dir = dir.join("cache");
        config.max_unchoked = 2;
        config.enable_inbound = true;
        // Bind ephemeral ports so the test never collides.
        config.bind = "127.0.0.1:0".parse().unwrap();
        config.peer_listen = "127.0.0.1:0".parse().unwrap();
        config.rtmp_bind = "127.0.0.1:0".parse().unwrap();
        let runtime = build_runtime(config, vec![]).await.unwrap();
        assert_eq!(runtime.config.max_unchoked, 2);
        let _ = std::fs::remove_dir_all(&dir);
    }
```

- [ ] **Step 4: Run tests**

Run: `cargo test -p ace-engine build_runtime`
Expected: PASS.

- [ ] **Step 5: Run the gates**

Run: `cargo test --workspace` then `cargo clippy --workspace --all-targets`
Expected: PASS.

- [ ] **Step 6: Update `config.rs` doc comment** — remove the "NOT YET WIRED" note from `max_unchoked`:

```rust
    /// Max simultaneously-unchoked peers per served stream (S2). Wired into the inbound serve
    /// path via the per-infohash `ServeCoordinator`; each stream unchokes up to this many
    /// interested peers plus one rotating optimistic slot.
    pub max_unchoked: usize,
```

- [ ] **Step 7: Commit**

```bash
git add crates/ace-engine/src/runtime.rs crates/ace-engine/src/config.rs crates/ace-swarm/src/listen.rs
git commit -m "ace-engine: wire max_unchoked + rechoke ticker into the inbound serve path (#4, #16)"
```

---

## Task 12: Final verification + PR

- [ ] **Step 1: Full gate**

Run: `cargo test --workspace`
Expected: PASS except the known pre-existing ace-media mpegts-fixture failure.

Run: `cargo clippy --workspace --all-targets`
Expected: no warnings.

- [ ] **Step 2: Grep for leftovers**

Run: `grep -rn "remove_cache_dir\|NOT YET WIRED\|get_or_create" crates/ace-engine/src crates/ace-swarm/src`
Expected: no `remove_cache_dir`; no `NOT YET WIRED` on `max_unchoked`; `get_or_create` only if retained for tests.

- [ ] **Step 3: Manual spec-coverage pass** — confirm against the spec's acceptance criteria:
  - Registry growth bounded by lease + TTL reaper (Tasks 3, 4, 5, 6). ✓
  - `max_unchoked` has production effect (Tasks 8-11). ✓
  - `enable_inbound` default intentionally chosen + documented (Task 7). ✓
  - Disk dirs cleaned on teardown; no resurrection; no cross-restart orphans (Tasks 1, 2, 5). ✓
  - Existing inbound seeder tests pass (Task 10 preserves `None` path). ✓

- [ ] **Step 4: Open the PR**

```bash
git push -u origin feat/inbound-seeding-lifecycle
gh pr create --title "Productize inbound seeding lifecycle & policy (#4, #36)" \
  --body "Implements #4 and folds in #36. See docs/superpowers/specs/2026-07-06-inbound-seeding-lifecycle-design.md.

- SeedRegistry entries are refcounted by RAII SeedLease (leech loop + broadcasts); evicted on producer teardown, with an idle-TTL reaper backstop for leaked leases.
- Disk stores get a process-unique <infohash_hex>-<generation> dir and a Drop that removes it; cache root wiped on startup. Closes the DELETE-vs-ingest race and removes bespoke remove_cache_dir (#36).
- max_unchoked wired via a per-infohash ServeCoordinator driving the existing Choker; SeederSession consults it instead of unchoking inline (unblocks #16).
- enable_inbound stays ON, now documented as an intentional decision.

Closes #4. Closes #36."
```

Leave the PR for the maintainer to merge.

---

## Self-review notes (for the executing agent)

- **Type consistency:** `SeedLease` (Task 3) is used verbatim in `BroadcastEntry` (Task 5) and returned by `reconstruct`; `ServeCoordinator::new/join/set_interested/leave/rechoke` (Task 8) are the exact methods called in Tasks 9-11; `coordinator_for(&self, &[u8;20], usize)` and `rechoke_all(&self)` (Task 9) match their callers in Tasks 10-11.
- **`serve` signature** gains exactly one trailing `Option<Arc<ServeCoordinator>>` — every caller updated in Task 10 Step 5.
- **Watch semantics:** `recompute` only `send`s on change, so the serve loop's `changed().await` fires only on real transitions.
- **Lease liveness:** Task 4's `_seed_lease` and Task 5's `_lease` fields must keep their leading underscore (alive-but-unused), never bare `_` (drops immediately).
- **Known failure:** the ace-media mpegts-fixture test failure is pre-existing; do not treat it as a regression.
