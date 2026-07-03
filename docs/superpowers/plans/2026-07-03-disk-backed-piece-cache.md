# Disk-Backed Piece Cache — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an optional on-disk backend to `PieceStore` (the seed store), selected by
config, so outpace can retain far more reseed data without consuming RAM — mirroring
Acestream's disk-cache option. Reuses the existing `seed_store_bytes` budget for both
backends.

**Architecture:** Refactor `PieceStore`'s internals behind a `Backend` enum. `Memory` holds
today's `BTreeMap` logic verbatim; `Disk` stores one file per piece under a per-infohash
directory, with an in-memory index so all metadata queries (`has_piece`, `have_pieces`,
`window`, `piece_header`) stay in RAM and only `chunk()`/`put_chunk*` touch disk. The public
API is unchanged, so the three call sites (download reseed, broadcast origination, broadcast
ingest) are untouched beyond how the store is constructed. The disk cache is **ephemeral** —
not reloaded across restarts (live data goes stale), which also avoids serving evicted-stale
pieces.

**Tech Stack:** Rust 2021, std::fs, serde, `tempfile` (dev-dep, for tests). Crates
`ace-swarm`, `ace-engine`.

**Spec:** `docs/superpowers/specs/2026-07-03-cache-config-and-disk-backing-design.md`.

> ⚠️ **Assumptions to confirm** (made while the user was away): the disk cache backs the seed
> store `PieceStore`, and the choice is a `memory | disk` mode switch (not RAM+disk tiering).

**I/O tradeoff (v1):** the store stays synchronous. Chunk writes are 16 KiB and piece reads
are ≤1 MiB, hitting page cache almost immediately on the live path. If profiling later shows
reactor stalls under disk mode, move the disk ops in `chunk()`/`put_chunk*` to
`spawn_blocking` — the `Backend` enum boundary keeps that localized. Documented, not
implemented, in v1.

---

## File Structure

- Modify `crates/ace-swarm/src/store.rs`: `Backend` enum, `MemoryBackend`, `DiskBackend`, new constructors. (Split `DiskBackend` into `crates/ace-swarm/src/store_disk.rs` if `store.rs` grows past ~400 lines.)
- Modify `crates/ace-swarm/Cargo.toml`: add `tempfile` as a `[dev-dependencies]`.
- Modify `crates/ace-engine/src/config.rs`: `CacheType` enum, `cache_type` + `cache_dir` fields.
- Modify `crates/ace-engine/src/runtime.rs`: parse `OUTPACE_CACHE_TYPE` + `OUTPACE_CACHE_DIR`; thread into providers/broadcast.
- Modify `crates/ace-engine/src/ace_provider.rs`: `SeedConfig` carries backend choice; build store via `with_backend` at both `get_or_create` sites.
- Modify `crates/ace-engine/src/broadcast.rs` + `crates/ace-engine/src/broadcast_ingest.rs` (via caller): same backend choice for origination/ingest stores.
- Modify `README.md`: document the new env vars + behavior.

---

## Task B1: extract current logic into `MemoryBackend` behind a `Backend` enum

**Files:**
- Modify: `crates/ace-swarm/src/store.rs`

- [ ] **Step 1: Confirm the existing tests are green (baseline)**

Run: `cargo test -p ace-swarm store`
Expected: PASS — this task is a pure refactor; these same tests gate it.

- [ ] **Step 2: Introduce the enum and move the maps into `MemoryBackend`**

In `store.rs`, replace the two map fields on `PieceStore` with a `backend`, and add:

```rust
#[derive(Debug)]
enum Backend {
    Memory(MemoryBackend),
}

#[derive(Debug, Default)]
struct MemoryBackend {
    /// piece index -> (chunk index -> TS payload bytes)
    pieces: std::collections::BTreeMap<u64, std::collections::BTreeMap<u16, Vec<u8>>>,
    /// piece index -> Acestream's 8-byte live piece header.
    headers: std::collections::BTreeMap<u64, [u8; 8]>,
}
```

`PieceStore` keeps `piece_length`, `chunk_length`, `max_bytes`, `cur_bytes`, and gains
`backend: Backend`. `new` builds `Backend::Memory(MemoryBackend::default())`.

- [ ] **Step 3: Delegate every public method to the backend**

Move the body of each public method (`put_chunk_with_header`, `chunk`, `piece_header`,
`has_piece`, `have_pieces`, `window`) into a matching method on `MemoryBackend`, and have the
`PieceStore` method `match &mut self.backend { Backend::Memory(m) => m.<op>(...) }`. Byte
accounting / eviction stays on `PieceStore` (it is backend-agnostic — it calls
`self.backend`'s evict-lowest-piece op). `put_chunk` and `chunks_per_piece` are unchanged.

- [ ] **Step 4: Run the existing tests, verify still green**

Run: `cargo test -p ace-swarm store`
Expected: PASS — identical behavior.

- [ ] **Step 5: Commit**

```bash
git add crates/ace-swarm/src/store.rs
git commit -m "ace-swarm: refactor PieceStore internals behind a Backend enum"
```

---

## Task B2: implement `DiskBackend`

**Files:**
- Modify: `crates/ace-swarm/src/store.rs` (or new `crates/ace-swarm/src/store_disk.rs`)
- Modify: `crates/ace-swarm/Cargo.toml`

Layout per piece: file `<dir>/<piece>.piece`; the 8-byte header at offset 0; chunk `i` at
offset `8 + i * chunk_length`. An in-memory index answers all metadata queries.

- [ ] **Step 1: Add `tempfile` dev-dep**

In `crates/ace-swarm/Cargo.toml`:

```toml
[dev-dependencies]
tempfile = "3"
```

- [ ] **Step 2: Write the failing disk-backend test suite**

Add to `store.rs` tests (a helper builds a `DiskBackend`-backed `PieceStore` in a tempdir;
`PieceStore::new_disk` lands in Task B3, so this test also drives that constructor — expect it
to fail to compile until B3, which is fine; keep B2's impl + B3's constructor in the same
red-green cycle if simpler):

```rust
    #[test]
    fn disk_backend_round_trips_and_evicts() {
        let dir = tempfile::tempdir().unwrap();
        // piece_length 8, chunk_length 4, budget 8 bytes -> holds 1 piece (2 chunks)
        let mut s = PieceStore::new_disk(8, 4, 8, dir.path().join("ih")).unwrap();
        s.put_chunk_with_header(10, 0, [7u8; 8], &[1, 2, 3, 4]);
        s.put_chunk(10, 1, &[5, 6, 7, 8]);
        assert_eq!(s.chunk(10, 0), Some(&[1, 2, 3, 4][..]));
        assert_eq!(s.chunk(10, 1), Some(&[5, 6, 7, 8][..]));
        assert_eq!(s.piece_header(10), Some([7u8; 8]));
        assert!(s.has_piece(10));
        assert_eq!(s.window(), Some((10, 10)));
        assert!(dir.path().join("ih/10.piece").exists());

        // Writing a second piece exceeds the 8-byte budget -> evict piece 10 (lowest).
        s.put_chunk(11, 0, &[9, 9, 9, 9]);
        assert!(!s.has_piece(10), "lowest piece evicted");
        assert!(!dir.path().join("ih/10.piece").exists(), "evicted file deleted");
        assert!(s.has_piece(11));
    }

    #[test]
    fn disk_backend_fails_fast_on_unwritable_dir() {
        // A path under a file (not a dir) cannot be created.
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let bad = tmp.path().join("cannot/exist");
        assert!(PieceStore::new_disk(8, 4, 8, bad).is_err());
    }
```

- [ ] **Step 3: Run, verify it fails**

Run: `cargo test -p ace-swarm disk_backend`
Expected: FAIL — `new_disk` / `DiskBackend` not defined.

- [ ] **Step 4: Implement `DiskBackend`**

```rust
use std::collections::BTreeMap;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;

#[derive(Debug)]
struct PieceIndex {
    header: [u8; 8],
    /// present chunk indices for this piece.
    present: std::collections::BTreeSet<u16>,
}

#[derive(Debug)]
struct DiskBackend {
    dir: PathBuf,
    chunk_length: u64,
    index: BTreeMap<u64, PieceIndex>,
}

impl DiskBackend {
    fn new(dir: PathBuf, chunk_length: u64) -> std::io::Result<Self> {
        // Ephemeral: wipe any stale dir from a prior run, then recreate.
        if dir.exists() {
            std::fs::remove_dir_all(&dir)?;
        }
        std::fs::create_dir_all(&dir)?;
        Ok(DiskBackend { dir, chunk_length, index: BTreeMap::new() })
    }

    fn piece_path(&self, piece: u64) -> PathBuf {
        self.dir.join(format!("{piece}.piece"))
    }

    fn put(&mut self, piece: u64, chunk: u16, header: [u8; 8], data: &[u8]) {
        let path = self.piece_path(piece);
        let mut f = std::fs::OpenOptions::new()
            .create(true).read(true).write(true).open(&path)
            .expect("open piece file");
        // Header at offset 0.
        f.seek(SeekFrom::Start(0)).unwrap();
        f.write_all(&header).unwrap();
        // Chunk at its fixed offset.
        let off = 8 + chunk as u64 * self.chunk_length;
        f.seek(SeekFrom::Start(off)).unwrap();
        f.write_all(data).unwrap();
        let e = self.index.entry(piece).or_insert_with(|| PieceIndex {
            header,
            present: Default::default(),
        });
        e.header = header;
        e.present.insert(chunk);
    }

    fn get(&self, piece: u64, chunk: u16) -> Option<Vec<u8>> {
        let e = self.index.get(&piece)?;
        if !e.present.contains(&chunk) {
            return None;
        }
        let mut f = std::fs::File::open(self.piece_path(piece)).ok()?;
        let off = 8 + chunk as u64 * self.chunk_length;
        f.seek(SeekFrom::Start(off)).ok()?;
        let mut buf = vec![0u8; self.chunk_length as usize];
        let n = f.read(&mut buf).ok()?;
        buf.truncate(n);
        Some(buf)
    }

    fn header(&self, piece: u64) -> Option<[u8; 8]> {
        self.index.get(&piece).map(|e| e.header)
    }
    fn has(&self, piece: u64) -> bool { self.index.contains_key(&piece) }
    fn pieces(&self) -> Vec<u64> { self.index.keys().copied().collect() }
    fn window(&self) -> Option<(u64, u64)> {
        let lo = *self.index.keys().next()?;
        let hi = *self.index.keys().next_back()?;
        Some((lo, hi))
    }
    /// Evict the lowest piece; return bytes freed (for the caller's byte accounting).
    fn evict_lowest(&mut self) -> u64 {
        if let Some((&piece, e)) = self.index.iter().next().map(|(k, v)| (k, v)) {
            let freed = e.present.len() as u64 * self.chunk_length;
            let _ = std::fs::remove_file(self.piece_path(piece));
            self.index.remove(&piece);
            freed
        } else {
            0
        }
    }
}
```

> Note: match the exact byte-accounting contract `MemoryBackend` uses in Task B1 (bytes are
> counted per stored chunk of `chunk_length`). If the memory path counts actual `data.len()`
> rather than `chunk_length`, mirror that here so eviction thresholds agree. Reconcile against
> the real code during implementation; the test's 8-byte/2-chunk budget assumes
> `chunk_length`-granular accounting.

Add `Disk(DiskBackend)` to the `Backend` enum and extend every `match` in `PieceStore`'s
public methods with the `Disk` arm delegating to the methods above.

- [ ] **Step 5: Run, verify it passes**

Run: `cargo test -p ace-swarm store`
Expected: PASS (memory + disk suites).

- [ ] **Step 6: Commit**

```bash
git add crates/ace-swarm/src/store.rs crates/ace-swarm/Cargo.toml
git commit -m "ace-swarm: add file-per-piece DiskBackend for PieceStore"
```

---

## Task B3: constructors selecting the backend

**Files:**
- Modify: `crates/ace-swarm/src/store.rs`

- [ ] **Step 1: Write a failing test for `with_backend`**

```rust
    #[test]
    fn with_backend_disk_matches_memory_semantics() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = PieceStore::with_backend(
            8, 4, 1024, BackendKind::Disk { dir: dir.path().join("ih") },
        ).unwrap();
        s.put_chunk(3, 0, &[1, 2, 3, 4]);
        assert_eq!(s.chunk(3, 0), Some(&[1, 2, 3, 4][..]));
    }
```

- [ ] **Step 2: Run, verify it fails**

Run: `cargo test -p ace-swarm with_backend_disk_matches_memory_semantics`
Expected: FAIL — `with_backend` / `BackendKind` not defined.

- [ ] **Step 3: Add the public backend selector + constructors**

```rust
/// Backend selection for `PieceStore::with_backend`.
pub enum BackendKind {
    Memory,
    Disk { dir: PathBuf },
}

impl PieceStore {
    pub fn with_backend(
        piece_length: u64,
        chunk_length: u64,
        max_bytes: u64,
        kind: BackendKind,
    ) -> std::io::Result<Self> {
        let backend = match kind {
            BackendKind::Memory => Backend::Memory(MemoryBackend::default()),
            BackendKind::Disk { dir } => Backend::Disk(DiskBackend::new(dir, chunk_length)?),
        };
        Ok(PieceStore { piece_length, chunk_length, max_bytes, cur_bytes: 0, backend })
    }

    /// Convenience: disk-backed store rooted at `dir`.
    pub fn new_disk(
        piece_length: u64,
        chunk_length: u64,
        max_bytes: u64,
        dir: PathBuf,
    ) -> std::io::Result<Self> {
        Self::with_backend(piece_length, chunk_length, max_bytes, BackendKind::Disk { dir })
    }
}
```

Keep the existing `PieceStore::new(..)` returning a memory store (back-compat; it can delegate
to `with_backend(.., BackendKind::Memory).unwrap()` — memory never errors).

- [ ] **Step 4: Run, verify it passes**

Run: `cargo test -p ace-swarm store`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/ace-swarm/src/store.rs
git commit -m "ace-swarm: add with_backend/new_disk PieceStore constructors"
```

---

## Task B4: config — `cache_type` + `cache_dir`

**Files:**
- Modify: `crates/ace-engine/src/config.rs`
- Modify: `crates/ace-engine/src/runtime.rs`

- [ ] **Step 1: Write failing config tests**

In `config.rs` tests:

```rust
    #[test]
    fn default_cache_is_memory_under_data_dir() {
        let c = Config::default();
        assert!(matches!(c.cache_type, CacheType::Memory));
        assert_eq!(c.cache_dir, c.data_dir.join("cache"));
    }
```

In `runtime.rs` `env_tests` (serialized under the same lock as Plan A, if present):

```rust
    #[test]
    fn parses_cache_type_and_dir() {
        let _g = ENV_LOCK.lock().unwrap();
        std::env::set_var("OUTPACE_CACHE_TYPE", "disk");
        std::env::set_var("OUTPACE_CACHE_DIR", "/tmp/sc-cache-test");
        let c = config_from_env().unwrap();
        assert!(matches!(c.cache_type, crate::config::CacheType::Disk));
        assert_eq!(c.cache_dir, std::path::PathBuf::from("/tmp/sc-cache-test"));
        std::env::remove_var("OUTPACE_CACHE_TYPE");
        std::env::remove_var("OUTPACE_CACHE_DIR");
    }
```

- [ ] **Step 2: Run, verify they fail**

Run: `cargo test -p ace-engine cache`
Expected: FAIL — `CacheType`/fields not defined.

- [ ] **Step 3: Add the enum, fields, defaults, and parse arms**

In `config.rs`:

```rust
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum CacheType {
    Memory,
    Disk,
}
```

Add to `Config`: `pub cache_type: CacheType,` and `pub cache_dir: PathBuf,`. In `Default`
(note `cache_dir` derives from `data_dir`, which is computed at the top of `default()` —
reuse that local):

```rust
            cache_type: CacheType::Memory,
            cache_dir: data_dir.join("cache"),
```

In `runtime.rs::config_from_env` (after the size arm):

```rust
    if let Ok(v) = std::env::var("OUTPACE_CACHE_TYPE") {
        config.cache_type = match v.as_str() {
            "memory" => CacheType::Memory,
            "disk" => CacheType::Disk,
            other => return Err(format!("invalid OUTPACE_CACHE_TYPE: {other}").into()),
        };
    }
    if let Ok(v) = std::env::var("OUTPACE_CACHE_DIR") {
        config.cache_dir = v.into();
    }
```

(Import `CacheType` in `runtime.rs`.)

- [ ] **Step 4: Run, verify they pass**

Run: `cargo test -p ace-engine cache`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/ace-engine/src/config.rs crates/ace-engine/src/runtime.rs
git commit -m "ace-engine: add cache_type + cache_dir config"
```

---

## Task B5: thread backend choice into the three store construction sites

**Files:**
- Modify: `crates/ace-engine/src/ace_provider.rs`
- Modify: `crates/ace-engine/src/broadcast.rs`
- Modify: `crates/ace-engine/src/broadcast_ingest.rs` (via its caller)
- Modify: `crates/ace-engine/src/runtime.rs`

The three `PieceStore::new(...)` sites: download reseed at ace_provider.rs:1193 and :1832
(inside `seed.registry.get_or_create`), broadcast origination at broadcast.rs:92, and the
broadcast-ingest store (constructed by its caller). Each becomes backend-aware, deriving a
per-infohash dir `<cache_dir>/<infohash_hex>` in disk mode.

- [ ] **Step 1: Add a shared helper to build a store from config**

Add to `ace_provider.rs` (or a small shared module) a helper both paths call:

```rust
/// Build a `PieceStore` for `infohash` honoring the configured cache backend.
pub(crate) fn build_piece_store(
    piece_length: u64,
    chunk_length: u64,
    max_bytes: u64,
    cache_type: crate::config::CacheType,
    cache_dir: &std::path::Path,
    infohash: &[u8; 20],
) -> std::io::Result<ace_swarm::store::PieceStore> {
    use ace_swarm::store::{BackendKind, PieceStore};
    let kind = match cache_type {
        crate::config::CacheType::Memory => BackendKind::Memory,
        crate::config::CacheType::Disk => {
            let hex = infohash.iter().map(|b| format!("{b:02x}")).collect::<String>();
            BackendKind::Disk { dir: cache_dir.join(hex) }
        }
    };
    PieceStore::with_backend(piece_length, chunk_length, max_bytes, kind)
}
```

- [ ] **Step 2: Extend `SeedConfig` and use the helper at the reseed sites**

Add `cache_type: CacheType` and `cache_dir: PathBuf` to `SeedConfig` (ace_provider.rs:~110);
populate from `self` where `SeedConfig` is built (ace_provider.rs:~300). Add
`with_cache(cache_type, cache_dir)` builders to `AceProvider` (mirroring
`with_seed_store_bytes`). At ace_provider.rs:1193 and :1832 replace `PieceStore::new(...)` in
the `get_or_create` closure with `build_piece_store(.., seed.cache_type, &seed.cache_dir,
&info.infohash).expect("build piece store")` (or propagate the error — the surrounding fn is
fallible).

- [ ] **Step 3: Same for broadcast origination + ingest**

In `broadcast.rs:92`, thread the configured cache into the `Broadcast`/registry (add fields
mirroring `store_bytes`) and build via `build_piece_store`. Do the same for the
broadcast-ingest store at its construction site.

- [ ] **Step 4: Wire config in `build_runtime`**

In `runtime.rs`, pass `config.cache_type` / `config.cache_dir` into the `AceProvider` builder
chain (runtime.rs:81) and the broadcast registry construction.

- [ ] **Step 5: Add a teardown cleanup**

On stream/broadcast teardown, remove `<cache_dir>/<infohash_hex>` (folds into the
`SeedRegistry` eviction follow-up already noted in `README.md`). If that eviction hook
doesn't exist yet, add a `remove_infohash_dir` call at the existing teardown/reaper path.

- [ ] **Step 6: Write an integration test for disk-mode file creation**

```rust
    #[tokio::test]
    async fn disk_mode_writes_piece_files() {
        // Build a broadcast/store with cache_type = Disk under a tempdir, feed one TS body,
        // assert a file appears under <cache_dir>/<infohash_hex>/.
        // (Model on broadcast_ingest.rs tests, swapping the store constructor for disk mode.)
    }
```

Flesh this out against the ingest test at `broadcast_ingest.rs:81+`, which already builds a
`PieceStore` and feeds a body; swap in disk mode and assert the file exists.

- [ ] **Step 7: Run, verify green**

Run: `cargo test -p ace-engine`
Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add crates/ace-engine/src/
git commit -m "ace-engine: select PieceStore backend from cache config"
```

---

## Task B6: docs

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Document the disk cache**

```markdown
- `OUTPACE_CACHE_TYPE` (`memory` | `disk`, default `memory`) — where the seed store keeps
  piece data. `disk` trades RAM for capacity.
- `OUTPACE_CACHE_DIR` (default `<data_dir>/cache`) — root dir for disk-mode piece files,
  one subdir per infohash.
- `OUTPACE_SEED_STORE_BYTES` sizes **both** backends. The disk cache is ephemeral (cleared
  on start, not reloaded across restarts). Disk I/O is currently synchronous; move to
  `spawn_blocking` if profiling shows reactor stalls.
```

- [ ] **Step 2: Commit**

```bash
git add README.md
git commit -m "docs: document disk-backed piece cache"
```

---

## Verification

- `cargo test` — all green (existing memory-store tests unchanged; new disk suites pass;
  live-network tests remain `#[ignore]`).
- `cargo clippy --all-targets -- -D warnings` — clean.
- Manual: `OUTPACE_CACHE_TYPE=disk OUTPACE_CACHE_DIR=/tmp/sc-cache cargo run -p ace-engine -- serve`,
  play or broadcast a stream, confirm piece files appear under `/tmp/sc-cache/<infohash>/`,
  RSS stays low relative to `memory` mode, and the dir is cleared on restart. Confirm the
  default (`memory`) path is byte-for-byte unchanged.
