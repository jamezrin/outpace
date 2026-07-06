# Inbound Seeding Lifecycle & Policy — Design

**Date:** 2026-07-06
**Status:** Draft for review
**Issues:** #4 (productize inbound seeding lifecycle and policy), folds in #36 (disk cache
per-infohash dir cleanup). Unblocks #16 (`max_unchoked` / `Choker` wiring).

## Motivation

Inbound seeding works and is wire-compatible, but it is still conservative plumbing with
three lifecycle/policy gaps:

1. **`SeedRegistry` entries never die.** Entries are keyed by infohash and only ever added.
   The leech path (`follow_live` → `get_or_create`) registers a store in a spawned task that
   ends when the consumer drops, but the registry entry (and, in disk mode, its
   `<cache_dir>/<infohash>` directory) survives forever. A long-running daemon that leeches
   many transient streams accumulates one orphaned store + dir per stream. Only
   `DELETE /broadcast/{name}` evicts anything, and it races an in-flight ingest (#36).

2. **`max_unchoked` is dead config.** `Choker` exists (`ace-swarm/src/seed.rs`) but has no
   caller. Each inbound connection is served independently by `SeederSession::serve` and
   unchokes on its first `Interested` — there is no per-stream coordination, so the knob has
   no production effect.

3. **`enable_inbound` default is undocumented as a decision.** It defaults ON (matching
   Acestream), but the issue asks for that to be an intentional, recorded choice.

This design ties registry-entry lifetime to its producer, adds a backstop eviction policy,
builds the per-stream serve coordinator that gives `max_unchoked` real effect, folds in the
#36 disk cleanup by making the disk store self-cleaning, and records the `enable_inbound`
decision.

## Current architecture (what we're changing)

- **`SeedRegistry`** (`ace-swarm/src/listen.rs`): `Arc<StdMutex<HashMap<[u8;20], SeedEntry>>>`,
  `SeedEntry { store, metadata }`. Keyed by infohash (store) and content_id (metadata).
  `get_or_create` / `register` / `register_metadata` / `get` / `metadata` / `serves` /
  `remove`. Entries never evicted (documented KNOWN GAP at listen.rs:29).
- **Leech producer** — `ace_provider::follow_live` calls `seed.registry.get_or_create(info.infohash, …)`
  inside a task spawned by `AceProvider::open`. Task ends when the `AceSource` receiver drops;
  the registry entry stays. `StreamManager`'s reaper evicts sessions by `(network,id)` and
  never touches `seed_registry` (and does not know the infohash).
- **Broadcast producer** — `BroadcastRegistry::start_or_resume` / `reconstruct` register store
  + metadata. `DELETE /broadcast/{name}` (http.rs:207-209) calls `seed_registry.remove(infohash)`,
  `seed_registry.remove(content_id)`, and `registry.remove_cache_dir(infohash)`.
- **Inbound serve (S2)** — `PeerListener::serve` accepts, uses `registry.serves(ih)` as the
  accept predicate, `registry.get(ih)` for the store, then spawns `SeederSession::serve` per
  connection (bounded by a `max_inbound` semaphore). `serve` unchokes inline on first
  `Interested`; no cross-connection state.
- **Disk backend** — `DiskBackend::new(dir)` wipes and recreates its own subdir; `build_piece_store`
  (disk mode) derives `dir = <cache_dir>/<infohash_hex>`. No `Drop` cleanup. `build_runtime`
  `create_dir_all`s the cache root but never wipes it.

---

## A. SeedRegistry leases — the lifetime anchor

**Decision:** RAII owner-lease + producer refcount, with an idle-TTL reaper backstop (section B).

Extend `SeedEntry` with a producer refcount, an owner kind, and a last-active timestamp:

```rust
enum OwnerKind { Leech, Broadcast }

struct SeedEntry {
    store: Option<SharedStore>,
    metadata: Option<SharedMetadata>,
    coordinator: Option<Arc<ServeCoordinator>>, // section C
    producers: usize,
    kind: OwnerKind,
    last_active: u64, // millis; set at creation, bumped on get() (section B)
}
```

Producers acquire an RAII `SeedLease`:

```rust
/// Dropping the lease decrements the producer count for its key(s); the entry is removed
/// when the count reaches 0. Not `Clone` (clone would need to bump the count).
pub struct SeedLease { /* Weak<registry inner> + keys + … */ }
```

Registry API:

- `lease_store(infohash, make) -> (SharedStore, SeedLease)` — leech path. Bumps `producers`
  (creating the store via `make` if absent), `kind = Leech`. Returns the shared store and the
  lease. Two leech sessions on one infohash → `producers == 2`, one shared store; the entry
  survives until both leases drop (fixes the same-infohash-two-sessions hazard the old blunt
  `remove` had).
- `lease_broadcast(infohash, content_id, metadata, make) -> SeedLease` — broadcast path. One
  lease owning **both** keys (store under `infohash`, metadata under `content_id`),
  `kind = Broadcast`. Registers metadata + store atomically.
- `get` / `serves` / `metadata` — unchanged (inbound serve path + accept predicate).
- `remove` — retained for the idempotent force-remove used by the reaper/tests; no longer the
  primary teardown path.

`SeedLease::drop` decrements `producers` for each owned key under one lock acquisition and
removes any entry that hits 0 (dropping the registry's `Arc<Mutex<PieceStore>>` and the
coordinator).

**Producers holding leases:**

- **Leech** — `follow_live` holds its `SeedLease` for the loop's lifetime and lets it drop on
  return. This is the leak fix and needs no manager/infohash coupling: the producer that
  created the entry is the one that evicts it. `SeedConfig`/the download loop thread the lease
  where the store is used today.
- **Broadcast** — the `Broadcast` struct (broadcast.rs) holds its `SeedLease`. `by_name.remove`
  on `DELETE` drops the `Broadcast`, dropping the lease → both keys evicted. **This lets us
  delete the explicit `seed_registry.remove(infohash)` / `remove(content_id)` calls in
  http.rs.** Resume-from-memory and reload-from-disk each (re)establish a lease.

---

## B. Idle-TTL reaper — the backstop

The lease refcount is the primary mechanism; the issue also asks for an eviction policy for
"stale entries that cannot be tied to an active owner." With leases, **every production entry is
lease-created with `producers >= 1`** and is removed the instant its count hits 0 — so the only
entries that can persist "untied to an active owner" are `producers == 0` orphans created outside
the lease API (the legacy `register*`/`get_or_create` methods). The reaper targets exactly those.

> **Design correction (during implementation):** the original draft had the reaper evict any idle
> `Leech` entry keyed on `last_active`. That is unsound: the leech producer writes chunks straight
> into its held store `Arc` and never re-touches the registry, so an idle-from-the-registry's-view
> but actively-writing leech (its lease still held, `producers > 0`) would be wrongly evicted. The
> corrected reaper only evicts **ownerless** (`producers == 0`) idle entries. A genuinely leaked
> lease (a stuck task that holds `producers > 0` forever) is deliberately left alone — it cannot be
> soundly distinguished from a slow-but-alive producer, and reaping it would not fix the stuck task.

- Each entry carries a `last_active` millis stamp, set at creation and bumped on `registry.get()`
  (the inbound serve touch), giving a fresh orphan a full TTL grace.
- A periodic sweep (spawned in `build_runtime`) force-evicts entries that are **`Leech`-kind AND
  `producers == 0` AND idle** beyond a TTL (`OUTPACE_SEED_TTL_SECS`, default 300s; 0 disables).
  Owned entries (`producers > 0`) and all `Broadcast`-kind entries are **never** reaped —
  broadcast lifetime is operator-controlled (servable/resumable until `DELETE`).
- This bounds registry growth by an explicit, testable policy that can never evict a live producer.
  In today's code no production path creates ownerless entries, so the reaper is a pure invariant
  backstop (a no-op unless a future non-lease caller leaves an orphan).

**Interaction with in-flight serves:** eviction removes the registry's strong `Arc`, but an
inbound peer mid-serve holds its own clone; the store stays alive for that peer and is fully
dropped (and, in disk mode, cleaned — section D) only when the last clone releases.

---

## C. Per-infohash serve coordinator — wires `max_unchoked`

**Decision:** full per-stream coordinator driving the existing `Choker`.

New `ServeCoordinator` (`ace-swarm`), one per infohash, stored in the `SeedEntry` (so its
lifetime rides the entry and is evicted with it). Created lazily when the first inbound peer
for that infohash registers.

Responsibilities:

- Track connected inbound peers and the current `Interested` set (caller-stable order).
- Apply `Choker::choose(interested, tick)` (up to `max_unchoked` + one rotating optimistic).
- Push `Unchoke` / `Choke` transitions to each connection as the chosen set changes.

Integration:

- Each inbound connection **registers** with the coordinator on accept, receiving a
  `PeerHandle { peer_id, commands }` where `commands` is a `watch`/`mpsc` receiver for
  choke/unchoke.
- `SeederSession::serve` stops unchoking inline. It:
  - on `Interested` / `NotInterested`, reports to the coordinator (does **not** unchoke inline);
  - `select!`s peer reads against `commands`;
  - answers chunk-requests **only while unchoked** (a choked peer's requests are dropped,
    matching BT semantics; a future reject is #18);
  - deregisters on disconnect (removing itself from the interested/unchoked sets).
- A **single rechoke ticker** in the listener periodically calls `registry.rechoke_tick()`,
  which advances every live coordinator's `tick` and recomputes its chosen set — no per-entry
  task to spawn/abort. On any interest change / tick / peer-leave, the coordinator diffs the
  new chosen set against the current unchoked set and sends only the transitions.
- **S1 is untouched.** The reciprocal path (`ace_provider::follow_one_peer`) inlines its own
  serve loop and serves a single peer; the coordinator is S2-only.

`max_unchoked` (and the rechoke interval) are threaded from `Config` into `PeerListener::serve`
so coordinators are built with the configured bound.

`ServeCoordinator` is a pure-ish unit under test with fake `PeerHandle`s: assert the unchoked
set never exceeds `max_unchoked` (+1 optimistic), and that join/leave/tick produce the right
Choke/Unchoke transitions.

---

## D. Disk store self-cleaning + unique dir + startup wipe (#36)

**Decision:** unique per-instance subdir + wipe cache root on startup.

- **Private dir per store instance.** `build_piece_store` (disk mode) derives
  `dir = <cache_dir>/<infohash_hex>-<generation>` where `generation` is a process-unique
  monotonic `AtomicU64`. Each store instance therefore owns a directory no other instance can
  touch. (README updated: dir name is `<infohash>-<generation>`, not bare `<infohash>`.)
- **`Drop` on `DiskBackend`** removes **its own** `dir` (best-effort, sync). Because the store
  is `Arc`-shared, `Drop` fires only when the **last** holder — registry entry + any in-flight
  inbound peers + ingest — releases it. This **closes the #36 DELETE-vs-ingest race
  automatically**: the dir is never wiped while ingest still holds an `Arc` that could
  recreate it. It also can never clobber a freshly re-leeched same-infohash store, since that
  store lives under a different `-<generation>` dir.
  - **Requirement:** `DELETE /broadcast` must actually stop the ingest source so its `Arc`
    releases and cleanup can proceed. Verify the current handler tears ingest down; implement
    if it does not. (This is #36's "stop writers first" made structural.)
- **Startup root wipe.** In disk mode, `build_runtime` wipes the cache **root**
  (`remove_dir_all` + recreate) instead of only `create_dir_all`, so orphans from a hard crash
  don't survive a restart. Safe because the cache is ephemeral (disk.rs header: wiped on
  creation, never reloaded); broadcasts rebuild piece data from live ingest on reload.
- **Delete `BroadcastRegistry::remove_cache_dir`** and its http.rs:209 call + duplicated path
  derivation. Cleanup now lives entirely at the store/backend layer.
- **Accepted v1 limitation:** `Drop`'s `remove_dir_all` is synchronous and may run on the
  reactor — the same tradeoff as the rest of the v1 disk backend. #37 covers moving disk I/O
  off the reactor and applies here too.

---

## E. enable_inbound

**Decision:** keep the default **ON**, and record it as intentional.

Rationale (already argued at config.rs:56): ON matches how the Acestream engine behaves out of
the box — a full P2P participant that binds its peer port, accepts inbound peers, seeds, and
self-announces. Only the HTTP API `bind` stays on localhost by default; the exposed listener is
the peer port, as with Acestream. `OUTPACE_ENABLE_INBOUND=0` gives a pure-leecher deployment.

Changes: a short note in this spec + the README recording the decision; the existing
`default_config_has_seeding_and_inbound_on` test already asserts the default. No behavior
change.

---

## Testing

- **Registry / leases:** lease drop at `producers == 0` removes the entry; two leases on one
  infohash keep it alive until both drop; broadcast lease drop removes **both** keys; `get` on
  a removed key is `None`.
- **TTL reaper:** an idle **ownerless** (`producers == 0`) `Leech` entry past the TTL is
  force-evicted; an owned (`producers > 0`) entry and a `Broadcast` entry are **not** reaped even
  when idle; `last_active` bumps on `get`.
- **Disk (#36):** disk store instance dir is `<infohash_hex>-<generation>`; `Drop` removes its
  own dir; two same-infohash instances have distinct dirs and one's `Drop` never deletes the
  other's; startup root wipe clears pre-existing orphans; after `DELETE` + ingest-Arc release
  the dir is gone and a lingering `Arc` cannot resurrect it.
- **Coordinator:** unchoked set never exceeds `max_unchoked` (+1 optimistic); transitions on
  interest change / tick / peer-leave; `SeederSession` honors coordinator commands (serves only
  while unchoked); a peer choked mid-stream stops getting pieces.
- **Regression:** existing inbound seeder + broadcast + disk-cache tests still pass; the
  `enable_inbound` default test still asserts ON.

## Implementation phasing (single PR)

1. **Lifecycle + #36:** registry leases (A), TTL reaper (B), disk self-cleaning + unique dir +
   startup wipe (D), delete `remove_cache_dir`, `enable_inbound` note (E). Ships a bounded,
   self-cleaning registry with no behavior change to the serve policy.
2. **Serve coordinator (C):** the heaviest piece — cross-connection unchoke state, coordinator
   registration in `SeederSession::serve`, the rechoke ticker, and `max_unchoked` wiring.

## Out of scope / follow-ups

- **Leech re-request after a mid-stream unchoke (NEW — surfaced by the Task 10 review).** The
  coordinator can now `Choke` a peer that was previously unchoked (optimistic-slot rotation, or
  swarm churn changing the interested set). Our own leech logic (`ace-swarm/src/live.rs` one-shot
  `requested` flag; `driver.rs` `Scheduler.requested` never reset on `Choke`) requests each chunk
  exactly once and never re-requests after a later `Unchoke`, so a peer running this codebase that
  gets re-choked mid-download can stall on the chunks it requested during the choke window. The
  legacy serve path never re-choked, so this was previously unreachable. **File as a follow-up**
  (leech-side robustness; orthogonal to inbound serving). Mitigations: reset the leech's
  requested-set on `Choke` and re-request on `Unchoke`, or adopt a sticky-unchoke policy for live.
- #16 (richer `Choker` fairness beyond what this coordinator exercises) — this design wires the
  existing policy; #16 can extend it.
- #18 (seeder reject messages for choked/missing chunks) — same serve path, separate change.
- #37 (async disk I/O / `spawn_blocking`) — applies to the new `Drop` cleanup too.
- #38 (disk-mode RAM-fallback budget), #39 (consolidate infohash hex encoders — removing
  `remove_cache_dir` drops one duplicate caller but doesn't finish #39).
