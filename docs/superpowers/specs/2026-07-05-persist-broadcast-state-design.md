# Persist Broadcast State & Continuous Ingest Resume — Design (2026-07-05)

Closes #3.

## Problem

Broadcast origination (`PUT /broadcast/{name}`) works, but a minted broadcast's
identity lives only in process memory:

- `BroadcastRegistry` holds minted `Broadcast`s in an in-memory `BTreeMap`. A daemon
  restart generates a **fresh RSA key → a different infohash/content_id**, orphaning any
  peer, tracker entry, or consumer that knew the old identity.
- `BroadcastIngest::new` always builds `SigningChunker::new(..., start_piece = 0, ...)`, so
  every ingest task **restarts piece numbering at 0** (flagged as a KNOWN GAP in
  `http.rs`). A reconnecting source overwrites piece indices instead of continuing them.

This mirrors exactly what the official engine persists to disk (note 25, captured from
`--stream-source-node`): `test.sauth` (the signing private key, in the clear), `test.acelive`
(the transport file), and `test.restart` (plain ASCII decimal, the last piece number).
Starting each ingest at piece 0 is also a suspected reason the official *consumer* does not
treat an outpace source as an interesting live source (note 31).

## Goals (acceptance criteria from #3)

1. A named broadcast keeps the **same identity/content_id/infohash across daemon restarts**
   when persisted state exists.
2. Repeated ingest for the same name **continues the piece sequence** without restarting at 0.
3. Existing outpace-to-outpace broadcast playback still works.
4. The persistence format and storage location are documented (this doc + code comments).

## Non-goals (YAGNI)

- **No database.** Consistent with the existing `identity.seed` file convention and the
  disk-backed piece-cache design (file-per-piece, no DB). The dataset is a handful of named
  broadcasts written infrequently; a DB solves problems (relational queries, many concurrent
  writers) we do not have.
- **Piece *data* is not persisted.** Live piece bytes are ephemeral and go stale on restart
  (same stance as the disk-cache spec). We persist only *identity* + the *piece cursor*.
- **No `clean`/dirty shutdown flag.** Matching the engine's dead-simple `.restart`, reload
  always resumes at `cursor + margin`; we never try to resume at the exact last piece.

## Storage layout

One JSON file per broadcast, `data_dir/broadcasts/<name>.json`, mode `0600` (it holds the
RSA private key). Mirrors `config.rs::load_or_create_identity` / `write_private`.

```json
{
  "transport_b64": "<base64 of the minted transport bytes>",
  "key_pkcs1_pem": "-----BEGIN RSA PRIVATE KEY-----\n...\n-----END RSA PRIVATE KEY-----\n",
  "next_piece": 51234
}
```

- `transport_b64` is the **identity source of truth**. infohash (`infohash_of_transport`),
  content_id (`transport_file_hash`), and geometry (`piece_length`/`chunk_length` via
  `decode_transport`) are all re-derived from it on load — no re-minting, so the infohash can
  never drift from a future `build_descriptor` change.
- `key_pkcs1_pem` restores the signing identity via `LiveSourceAuth::from_pkcs1_pem`. On load
  we sanity-check that its `pubkey_der()` matches the pubkey embedded in the decoded transport;
  a mismatch marks the record invalid.
- `next_piece` is the last-persisted piece cursor (see Continuity).

`serde` + `serde_json` are already engine dependencies; `base64` is added (small, widely used)
for the transport bytes.

### Name validation (path-traversal safety)

`{name}` from the URL path becomes a filename, so it must be validated **before** any registry
or filesystem use, on both `PUT` and `DELETE`:

- Allowed charset: `[A-Za-z0-9._-]`, length 1..=64, and not `.` or `..`.
- Invalid names return `400 Bad Request` and touch neither the registry nor disk.

This closes a real traversal vector (`name = "../../etc/whatever"`) and also hardens the
existing unvalidated `PUT` path.

### Atomic writes

Write `<name>.json.tmp` (created `0600`), `write_all`, then `rename` over `<name>.json` — the
same temp-then-rename technique `write_private` implies, extended with the rename so a crash
mid-write never leaves a torn file. Non-Unix falls back to a plain write (mirroring the
existing `#[cfg(unix)]` split).

## Modules

### `ace-swarm/src/listen.rs` (`SeedRegistry`)

Add the removal method the existing code comment already anticipates ("No `unregister`/TTL
exists yet; add one if this becomes a need"):

```rust
/// Stop serving `key` (an infohash or a metadata content_id): drops both the piece store
/// and any registered metadata under it. Idempotent.
pub fn remove(&self, key: &[u8; 20]);
```

### `broadcast_persist.rs` (new)

Owns all disk I/O under `data_dir/broadcasts/`. Pure filesystem, no knowledge of the registry.

```rust
pub struct BroadcastPersist { dir: PathBuf }

pub struct PersistedBroadcast {
    pub transport: Vec<u8>,
    pub key_pkcs1_pem: String,
    pub next_piece: u64,
}

impl BroadcastPersist {
    pub fn new(data_dir: &Path) -> Self;                       // dir = data_dir/broadcasts
    pub fn load_all(&self) -> Vec<(String, PersistedBroadcast)>; // skips invalid files w/ warn
    pub fn save(&self, name: &str, rec: &PersistedBroadcast) -> io::Result<()>; // atomic 0600
    pub fn delete(&self, name: &str) -> io::Result<()>;        // remove the file (ok if absent)
}
```

`load_all` reads every `*.json`, deserializes, and validates (pubkey match, decodable
transport); an invalid or unreadable file is logged and skipped, never aborting startup.

### `broadcast.rs` (`BroadcastRegistry`, `Broadcast`)

- `BroadcastRegistry` gains an `Option<BroadcastPersist>` handle:
  - `new()` — disk-less (existing tests keep working unchanged; persistence is a no-op).
  - `with_persist(data_dir)` — production.
- `Broadcast` gains a shared cursor, `cursor: Arc<BroadcastCursor>` (below).
- `start_or_resume(name, ...)`:
  1. In-memory hit → return it (`fresh = false`).
  2. Else, if persistence has a record for `name` → **reload** it (auth from PEM, identity from
     transport, cursor seeded from `next_piece + CURSOR_PERSIST_INTERVAL`), register store +
     metadata in `seed_registry`, insert, return (`fresh = false`).
  3. Else → mint fresh as today, persist the identity record (`next_piece = 0`), insert, return
     (`fresh = true`).
- `delete(name)`: remove the in-memory entry, mark its cursor removed (so late flushes no-op),
  delete the persisted file. (Dropping it from `seed_registry` is handled by the caller, which
  owns the registry handle — see DELETE handler.)

`BroadcastCursor` encapsulates continuity + throttled persistence:

```rust
pub struct BroadcastCursor { /* AtomicU64 next, AtomicBool removed, Option<sink> */ }
impl BroadcastCursor {
    fn start_piece(&self) -> u64;          // where a new ingest's chunker begins
    fn advance_to(&self, next_piece: u64); // bump; persist when crossing CURSOR_PERSIST_INTERVAL
    fn flush(&self);                        // force-persist current value (called on finish)
}
```

The sink holds the persist handle + name + the immutable record parts (`transport`, key PEM),
so a cursor flush rewrites the whole small record atomically with the new `next_piece`. When
`removed` is set (via `delete`), `advance_to`/`flush` no-op so a stale in-flight ingest can't
resurrect the file.

### `broadcast_ingest.rs`

- `BroadcastIngest::new(store, auth, cursor)` starts `SigningChunker` at `cursor.start_piece()`
  instead of `0`.
- As `OutChunk`s are produced, it calls `cursor.advance_to(out.piece + 1)` (monotonic max), so
  the cursor tracks the live edge and persists on the throttle.
- `finish()` calls `cursor.flush()`.

### `http.rs`

- `broadcast_ingest`: validate name (400 on bad); pass `bc.cursor` into `BroadcastIngest::new`.
  Remove the KNOWN GAP comment.
- New `DELETE /broadcast/:name` → `broadcast_delete`: validate name; if `broadcasts` disabled
  → 404; else look up the broadcast (to get its infohash/content_id), `registry.delete(name)`,
  `seed_registry.remove(infohash)` + `remove(content_id)`, and return `204 No Content` (still
  204 if it did not exist — idempotent).
- Route becomes `put(broadcast_ingest).get(broadcast_transport).delete(broadcast_delete)`.

### `runtime.rs`

- Build the registry with `BroadcastRegistry::with_persist(&config.data_dir)`.
- After constructing `BroadcastState`, **reload on startup**: for each `load_all()` record,
  `start_or_resume`-equivalent reconstruction registers it in `seed_registry` and starts the
  tracker/DHT announce loops. Factor the announce-launch currently inline in
  `http::broadcast_ingest` (and mirrored in `rtmp::announce_broadcast`) into one shared helper
  reused by fresh mint, reload, and RTMP.

## Continuity mechanics

- **Within a run:** the `Arc<BroadcastCursor>` is authoritative. Each `BroadcastIngest` starts
  at `start_piece()` and advances it; a repeated `PUT` continues exactly, no gap.
- **Across restart:** the cursor is persisted every `CURSOR_PERSIST_INTERVAL` pieces (proposal
  **128** ≈ 8 MiB ≈ ~8 s at the 8375 kbps default) and on `finish()`. On reload we resume at
  `persisted_next_piece + CURSOR_PERSIST_INTERVAL`. This guarantees a piece number is **never
  reused** (reuse corrupts followers) at the cost of a ≤128-piece forward gap, which the
  existing `Continuity::skip_evicted_gap` path already tolerates on the consumer side.

`CURSOR_PERSIST_INTERVAL` lives as a constant in `broadcast.rs` next to the other broadcast
geometry constants.

## Error handling

- **Mint-time identity persist failure:** log an error and proceed in-memory (streaming
  continues; durability degraded). We do not kill a live ingest over a transient disk error.
- **Cursor persist failure:** log at debug/warn and continue (best-effort continuity).
- **Corrupt/invalid record on reload:** logged and skipped; startup proceeds.
- **Invalid name:** `400`, no side effects.

## Testing

- `listen.rs`: `SeedRegistry::remove` drops the store (`serves` → false) and metadata
  (`metadata` → None); removing an absent key is a no-op.
- `broadcast_persist`: save→load round-trips a record; atomic write leaves no `.tmp` and no
  torn file; file is `0600`; `delete` removes it; `load_all` skips a corrupt file and keeps a
  good one.
- `broadcast.rs`: reload from a persisted record reconstructs an **identical**
  infohash/content_id and a working `LiveSourceAuth`; a fresh mint writes a record with
  `next_piece = 0`; `delete` → next `start_or_resume` mints a **different** infohash; cursor
  reload resumes at `persisted + CURSOR_PERSIST_INTERVAL`.
- `broadcast_ingest`: a second `BroadcastIngest` over the same cursor **continues** piece
  numbering rather than restarting at 0 (direct regression test for the KNOWN GAP); cursor
  persists on the interval and on `finish()`.
- `http.rs`: `DELETE` an existing broadcast → 204 and subsequent `GET` → 404; `DELETE` unknown
  → 204 (idempotent); invalid name on `PUT`/`DELETE` → 400.
- `runtime`: startup reload makes a persisted broadcast immediately servable and
  `GET /broadcast/{name}` returns the same transport bytes as before the restart (temp-dir
  `data_dir`, construct → persist → reconstruct).

## Files touched

- `crates/ace-swarm/src/listen.rs` — add `SeedRegistry::remove` (+ test).
- `crates/ace-engine/src/broadcast_persist.rs` — new.
- `crates/ace-engine/src/broadcast.rs` — persist handle, cursor, reload/delete.
- `crates/ace-engine/src/broadcast_ingest.rs` — start-from-cursor, throttle-persist.
- `crates/ace-engine/src/http.rs` — DELETE route/handler, name validation, cursor wiring.
- `crates/ace-engine/src/runtime.rs` — `with_persist`, startup reload, shared announce helper.
- `crates/ace-engine/src/rtmp.rs` — use the shared announce helper; pass the cursor through.
- `crates/ace-engine/Cargo.toml` — add `base64`.
