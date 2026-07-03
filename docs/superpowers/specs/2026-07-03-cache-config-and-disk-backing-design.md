# Cache Configuration & Disk-Backed Piece Store — Design

**Date:** 2026-07-03
**Status:** Draft for review

> ⚠️ **Assumptions (user was away during brainstorming — confirm or redirect):**
> 1. The on-disk cache backs the **seed store** (`PieceStore`), not a new playback-seek
>    buffer. Rationale: `PieceStore` is already a bounded byte-budget FIFO of piece data;
>    it is the only "cache" in the system. outpace is live-only and has no seek model,
>    so a VOD-style playback disk cache has no consumer today.
> 2. Memory-vs-disk is a **mode switch** (`memory | disk`), mirroring Acestream's
>    `--live-cache-type`. A tiered (RAM + disk spillover) mode is explicitly out of scope
>    for v1 but the backend abstraction leaves room for it.

## Motivation

Today outpace has three memory buffers, only one of which is configurable:

| Buffer | Where | Default | Configurable today? |
|---|---|---|---|
| Seed store (`PieceStore`) | `ace-swarm/src/store.rs` | 128 MiB | ✅ `OUTPACE_SEED_STORE_BYTES` |
| Playback prefetch window (`PREFETCH_PIECES`) | `ace-engine/src/ace_provider.rs:43` | 8 pieces | ❌ hardcoded |
| Session fan-out channel (`StreamManager.buffer`) | `ace-engine/src/manager.rs:32` | 256 msgs | ❌ hardcoded |

Two gaps, split into two independent plans:

- **Plan A — env-var config for existing tunables.** Expose `PREFETCH_PIECES` and the
  session fan-out buffer via env vars, using the established `seed_store_bytes` threading
  pattern. Pure plumbing of things that already exist. No new subsystems.
- **Plan B — disk-backed piece cache.** Let the seed store spill to disk so operators can
  retain far more reseed data without paying RAM, mirroring Acestream's disk cache option.

The two plans share only `Config`/`runtime.rs` touch points and do not conflict.

---

## Plan A: env-var configuration of existing tunables

### What gets exposed

| New env var | `Config` field | Default | Wires into |
|---|---|---|---|
| `OUTPACE_PREFETCH_PIECES` | `prefetch_pieces: u64` | 8 | `AceProvider` (new `with_prefetch_pieces` builder; replaces the `PREFETCH_PIECES` const use at `ace_provider.rs:883`) |
| `OUTPACE_SESSION_BUFFER` | `session_buffer: usize` | 256 | `StreamManager` (replaces the hardcoded `buffer: 256` at `manager.rs:32`) |

Deliberately **out of scope** (YAGNI — peer-scheduling internals, not "buffer/cache"
sizing the user asked about): `MAX_PIECE_ADVANCE`, `MAX_PARALLEL_CONNECT`,
`MAX_ACTIVE_UPSTREAMS`, `BACKGROUND_DISCOVERY_PEER_TARGET`. A one-line note in the plan
records how to expose them later if wanted.

### Validation

- `session_buffer` must be `>= 1` — `tokio::sync::broadcast::channel(0)` panics. Reject 0 at
  parse time with a clear error.
- `prefetch_pieces` of 0 is legal (start exactly at the live edge, no cushion) but risky;
  accept it and document the tradeoff. No upper clamp — `MAX_PIECE_ADVANCE` already guards
  the request frontier downstream.

### Pattern

Copy the exact shape already used for `seed_store_bytes`: `Config` field with a `Default`,
an env parse arm in `config_from_env` (`runtime.rs:29`), and either a builder
(`with_prefetch_pieces`, like `with_seed_store_bytes` at `ace_provider.rs:164`) or a
constructor argument (`StreamManager`). No architectural change.

---

## Plan B: disk-backed piece cache

### Goal

Add an optional disk backend to `PieceStore` selected by config, preserving its exact
public API so the three call sites (`SeedRegistry` download reseed, broadcast origination,
broadcast ingest) are untouched beyond how the store is constructed.

### Backend abstraction

`PieceStore` currently *is* the memory implementation (two `BTreeMap`s + a byte counter).
Refactor its internals behind an enum so the public methods delegate:

```rust
pub struct PieceStore {
    piece_length: u64,
    chunk_length: u64,
    max_bytes: u64,
    cur_bytes: u64,
    backend: Backend,
}

enum Backend {
    Memory(MemoryBackend), // today's BTreeMap logic, moved verbatim
    Disk(DiskBackend),
}
```

The public surface is unchanged: `put_chunk`, `put_chunk_with_header`, `chunk`,
`piece_header`, `has_piece`, `have_pieces`, `window`, `chunks_per_piece`. Memory path stays
byte-for-byte behaviorally identical — existing `store.rs` tests must pass untouched.

### DiskBackend layout

- One directory per store instance, keyed by infohash: `<cache_dir>/<infohash_hex>/`.
- One file per piece: `<piece>.piece`, holding that piece's chunks. The 8-byte Acestream
  live piece header is stored at the file head; chunks follow at fixed
  `chunk_length` offsets (chunk index → offset is arithmetic, no per-chunk index needed on
  disk).
- **In-memory index** kept for the metadata-only queries so they never touch disk:
  `has_piece`, `have_pieces`, `window`, `piece_header` (headers cached in RAM — 8 bytes each
  is cheap), and per-piece present-chunk bitsets. Only `chunk()` reads bytes from disk;
  only `put_chunk*` writes.
- FIFO eviction (lowest piece index first, same policy as memory) deletes the piece file and
  drops its index entry.

### I/O model (the one real risk)

`PieceStore` is documented "pure (no I/O)". Disk mode breaks that. The callers hold it as
`Arc<Mutex<PieceStore>>` on async paths, so synchronous disk I/O runs under the lock and on
the reactor thread. For v1:

- Keep the store **synchronous**. Chunk writes are 16 KiB and piece reads are ≤1 MiB — small
  and, on the live path, hitting page cache almost immediately. This keeps the refactor
  contained and the API sync.
- Document the tradeoff explicitly and leave a follow-up marker: if profiling shows reactor
  stalls under disk mode, move `chunk()`/`put_chunk*` disk ops to `spawn_blocking` (or make
  the backend async), which the enum boundary makes a localized change.

This is a conscious v1 simplification, called out so review can veto it.

### Lifecycle / correctness

- **Ephemeral, not persistent.** Live piece data goes stale on restart, so the disk cache is
  not reloaded across daemon restarts. On store creation the infohash directory is created
  fresh; a stale directory from a prior run is wiped first. This also sidesteps
  serving-evicted-stale-data correctness questions.
- On stream/broadcast teardown, remove the infohash directory (ties into the existing
  `SeedRegistry` eviction follow-up tracked in issue #4).
- If disk mode is selected but the cache dir is unwritable, fail fast at startup with a clear
  error rather than silently degrading to memory.

### Config additions

| New env var | `Config` field | Default | Meaning |
|---|---|---|---|
| `OUTPACE_CACHE_TYPE` | `cache_type: CacheType` (`Memory`\|`Disk`) | `Memory` | Selects the backend, like Acestream's `--live-cache-type` |
| `OUTPACE_CACHE_DIR` | `cache_dir: PathBuf` | `<data_dir>/cache` | Where disk piece files live |

The **existing** `seed_store_bytes` (`OUTPACE_SEED_STORE_BYTES`) is reused as the byte
budget for *both* backends — no new size knob. This is what ties Plan B back to the original
question: the cache size is already tunable; Plan B only adds *where* it lives.

Threading: `SeedRegistry::get_or_create` and the broadcast/ingest construction sites build
the `PieceStore` with the configured backend + a per-infohash dir derived from `cache_dir`.
That means the backend choice and dir must reach those sites (via `SeedConfig` for the
download path, and the broadcast registry for origination).

### Non-goals for v1

- Tiered RAM+disk spillover.
- Cross-restart persistence / cache warming.
- A separate disk size limit distinct from `seed_store_bytes`.
- Any playback-seek / VOD cache (outpace is live-only).

---

## Testing strategy

- **Plan A:** unit tests that `config_from_env` parses the two vars and rejects
  `session_buffer=0`; a wiring test that `AceProvider::with_prefetch_pieces` /
  `StreamManager` honor the value (assert against the value used, mirroring the existing
  `default_config_*` tests).
- **Plan B:** the current `store.rs` tests run unchanged against the memory backend; a
  parallel suite runs the same scenarios against a `DiskBackend` in a `tempdir` (put/get,
  header round-trip, FIFO eviction deletes files, `window`/`have_pieces` correctness,
  eviction below new cursor). Plus a fail-fast test for an unwritable cache dir.

## Rollout

Both default to today's behavior (`Memory`, prefetch 8, buffer 256), so neither plan changes
default runtime behavior — they only add opt-in knobs.
