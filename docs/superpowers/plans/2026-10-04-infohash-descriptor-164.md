# Fail Closed on Infohash-Only Live Playback Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Opening a live stream by bare infohash must never stream with guessed geometry again. An infohash opens only with a transport descriptor that outpace has already verified, which carries the real geometry and the source pubkey. With no such descriptor, every entry point fails closed with a clear error that suggests the `cid:<id>` form.

**Architecture:**
- `AceProvider::resolve_live_info` becomes the single live resolver behind `open()`. The native `/streams` routes, compat `/ace/getstream` and `/ace/manifest.m3u8`, and `outpace play` all reach it.
- Every successful signed-catalog `cid:` resolution records its `StreamInfo` in a new `InfohashIndex`, keyed by the 20-byte infohash. BEP-9 and transport-URL resolutions do not (controller Rulings G and J, below).
  - The index is a separate type from `ResolveCache`, which is keyed by the content-id string.
  - Each entry is keyed by its own `info.infohash`. `stream_info_from_transport` computed that value from the same descriptor that supplied the geometry and pubkey, so the binding holds by construction.
- A bare 40-hex id is served from two places:
  - the index;
  - a broadcast this daemon originates, whose transport the shared `SeedRegistry` already holds.
- With neither, `open()` returns a new `ProviderError::Unresolvable(reason)`:
  - the native routes map it to `422` with the reason as a text body;
  - the compat routes reject it before minting a lease, using a new offline `StreamProvider::check_openable` pre-check;
  - the CLI prints it.
- `stream_info_from_infohash` and the `DEFAULT_PIECE_LENGTH` and `DEFAULT_SIG_LEN` guesses are deleted.

**Tech Stack:** Rust 1.99.0 (pinned), tokio, axum, the workspace crates `ace-swarm`, `ace-engine` and `ace-wire`.

**Spec:** GitHub issue #164, "Infohash-only playback guesses piece geometry and serves corrupted, unverified MPEG-TS" (`gh issue view 164`). The controller's rulings R1-R9 are in the planner brief. This plan follows them; see "Deviations" below.

## Global Constraints

- **Workspace.**
  - Work only in `/home/jamezrin/dev/outpace/.worktrees/fix/infohash-descriptor-164`, on branch `fix/infohash-descriptor-164`. It is based on `origin/main` @ `815d2d8`, plus the commit that adds this plan.
  - Do not create another worktree or branch, and do not touch `/home/jamezrin/dev/outpace`.
  - Do not push, open PRs or change GitHub settings.
- **Toolchain.** The shell exports `RUSTUP_TOOLCHAIN=1.96.0`, which overrides `rust-toolchain.toml` (`1.99.0`, the version CI uses).
  - Prefix **every** cargo, rustc and rustfmt command with `env -u RUSTUP_TOOLCHAIN`.
  - Before Task 1, confirm that `env -u RUSTUP_TOOLCHAIN rustc --version` prints `rustc 1.99.0`.
- **The gate.** Every task ends with this exact sequence, run from the worktree root. All four commands must exit 0:

  ```bash
  env -u RUSTUP_TOOLCHAIN cargo fmt --all --check
  env -u RUSTUP_TOOLCHAIN cargo clippy --workspace --all-targets --locked -- -D warnings
  env -u RUSTUP_TOOLCHAIN cargo test --workspace --locked
  python3 tools/hygiene/check_identifiers.py
  ```

- **Formatting.**
  - Never run `cargo fmt --all` without `--check`; it rewrites unrelated files.
  - If `--check` reports a file you touched, format only that file with `env -u RUSTUP_TOOLCHAIN rustfmt --edition 2021 <file>`, then re-run `--check`.
- **`Cargo.lock`.** No task changes dependencies. If a cargo command modifies `Cargo.lock`, restore it with `git checkout -- Cargo.lock` before committing.
- **AceStream identifier hygiene (absolute).**
  - No content id, infohash or stream name may appear in any file, test or commit message.
  - Where 40-hex is syntactically required, use the placeholder `0123456789abcdef0123456789abcdef01234567`.
  - Every non-placeholder infohash in a test must be computed at test time from a synthetic transport.
  - Never read `acestream-ids.txt` from code or tests.
  - `core.hooksPath` is `.githooks` in this worktree, so the pre-commit hook runs the hygiene gate. Never bypass it.
- **Logging (R8).**
  - `ace-swarm` code added here is library code. It returns values or errors and does not log.
  - `ace-engine` operational events use `crate::alog!("[ace] …")`. CLI output a human reads live uses `eprintln!("outpace play: …")`.
- **No guessed-geometry fallback (R2).** Do not add an env var, flag or code path that streams a bare infohash with default geometry.
- **Out of bounds.**
  - `docs/protocol/notes/*` and older `docs/superpowers/{plans,specs}/*` are dated historical records. Leave them unchanged.
  - Do not modify `tools/swarmtest`, `tools/memsoak`, CI workflows, `Cargo.toml` or `Cargo.lock`.
- **Commits.** One commit per task, with the subject given in the task, plus the trailer `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`. Task 10 does not commit.

## Design Notes (why, for the PR description)

- **Spike: no network descriptor source is usable today.** The spike ran live on 2026-10-04 against cid9, resolved from the registry, with its infohash derived at runtime.
  - **The signed catalog cannot be queried by infohash.** `/gettorrent` was signed with `infohash=<ih>`, `pid=<ih>` and `ih=<ih>`. Each returned `200` with an empty chunked body, which is identical to an unknown `pid`. The catalog ignores unknown parameters.
  - **BEP-9 by infohash does not work.**
    - Live data-swarm peers found by tracker and DHT: 14. 12 were unreachable and 2 closed during the BT handshake.
    - The official 3.2.11 engine, acting as a data-swarm peer while playing that infohash, advertises `m.ut_metadata` but **no `metadata_size`**. It closes the connection on a `ut_metadata` request for piece 0.
    - This is consistent with notes 12, 20 and 44.
  - **The official engine does play a cold infohash.** It produced about 39 MB of clean H.264 in 25 s, with 5 CC errors. A fresh engine maps the cold infohash to its content id in about 2.6 s through `get_content_id`. So a remote infohash-to-content-id lookup exists, but its request line was not captured. Identifying it is a follow-up; see "Out of scope".
  - **The catalog transport for cid9 has `piece_length = 524288`.** That confirms the issue's inferred 512 KiB, half the 1 MiB guess.
- **Why fail closed is safe for integrity.**
  - The swarm infohash is `SHA1(bencode([name, authmethod, pubkey, piece_length, chunk_length, bitrate]))` (`ace_wire::infohash::infohash_of_descriptor`). It therefore commits to geometry and pubkey.
  - Any descriptor recorded under an infohash was decoded by `stream_info_from_transport`, which computed that infohash from the same bytes. An index hit therefore cannot pair an infohash with another stream's geometry.
  - A non-empty `source_pubkey` enables per-piece RSA verification on the live path: `Continuity::fresh` calls `PieceReassembler::with_source_pubkey`.
- **Why a separate index, not `ResolveCache`.** Content ids and infohashes share the 40-hex space, and `ResolveCache` is keyed by a bare string. The new `InfohashIndex` is keyed by `[u8; 20]` and holds only descriptors keyed by their own infohash. A content id typed without `cid:` (#165) therefore never hits it.
- **Why no TTL.** A descriptor's infohash commits to its geometry and pubkey, so an entry never goes stale. The index is bounded at 256 entries by evicting the oldest.
- **Why the compat pre-check.** `/ace/getstream?format=json` hands out playback URLs without opening a stream. Without a pre-check, a refused infohash would get a token and then a bare `404` at playback. `check_openable` is offline and synchronous. Its default accepts every id, so fixture providers are unaffected.
- **Why `422`.** The id is well-formed, but the server will not process it. `422` with a text body distinguishes "change your input" from the existing bodiless `404`, which still covers bad ids, unknown networks and other start failures.
- **Test vector (R5).**
  - `tests/vectors/transport-01.bin` is not under `tests/vectors/transport/`. Note 45 says it is a real engine capture, so it cannot serve as a synthetic vector.
  - The 512 KiB vector is therefore built in-test, with outpace's own `encode_transport` and a freshly generated `LiveSourceAuth` key. Its infohash is computed at test time.
  - No `.bin` is committed: in-test construction is deterministic in geometry, self-describing, and avoids a binary fixture.
  - The broadcast fallback test also uses outpace's own minting code, which has 64 KiB pieces.

## Deviations From the Issue and Rulings (flagged for the controller)

1. **CLI `infohash=` and `magnet:` inputs always fail.** A one-shot `outpace play` process starts with an empty index and runs no broadcasts, so for the CLI, acceptance item 1 ("uses the descriptor's geometry") can only mean "fails closed". The docs say this plainly (Task 9).
2. **Acceptance item 1's literal "add it under `tests/vectors/transport/`"** is replaced by in-test construction; see Design Notes.
3. **Live check (R7)** uses the "no network source" variant (Task 10).
4. **Unbound descriptor fields (controller Rulings G and J, added during execution).** The infohash does not commit to `trackers` or `categories`, and the index is shared state.
   - Ruling G: transport-URL resolutions never feed the index (caller-supplied descriptors).
   - The final review found the same exposure through BEP-9 content-id resolutions, which bind a blob only to a content id the caller chose. A first fix replaced indexed trackers with the daemon's default tracker; a live A/B showed that breaks playback (0 tracker peers, no reachable upstream), so it was reverted.
   - Ruling J: only signed-catalog content-id resolutions and the daemon's own broadcasts (checked first) make an infohash openable, and the descriptor's own trackers are kept. BEP-9 results go to a separate cache that never feeds the index.
   - Residual risk, accepted and disclosed: the catalog is plain HTTP with a self-asserted checksum, so whoever can tamper with that fetch, or register a third-party transport if the catalog allows it, can plant trackers for a real infohash (IP disclosure and peer steering; no media injection, because the infohash binds the pubkey). The same attacker can already substitute a whole descriptor on any `cid:` open, which predates this plan.

## Review Focus

1. **Fail-closed coverage.**
   - Expected: no production path builds a `StreamInfo` except from a decoded descriptor.
   - Pinned by `rg -n 'stream_info_from_infohash|DEFAULT_PIECE_LENGTH|DEFAULT_SIG_LEN' crates tools` returning nothing (Task 4).
   - Also pinned by the native, compat and CLI refusal tests (Tasks 3, 6 and 8).
2. **Namespace separation.** A content id cached in `ResolveCache` must not make the same 40-hex string openable as an infohash. Pinned in Task 3 (`content_id_resolution_records_the_descriptor_under_its_infohash`).
3. **Index semantics.** Capacity is 256, there is no TTL, last-write-wins applies, and a `ResolveCache` hit re-records the entry. Pinned in Tasks 1 and 3.
4. **Status-code choice.** `422` with a text body for refusals; everything else keeps its bodiless `404`. Pinned in Task 6.
5. **Trait surface.** `StreamProvider` gains two defaulted methods, `check_openable` and `remember_live_descriptor`. Fixture providers in `http.rs` and the integration tests compile unchanged and behave the same. Pinned by the full test suite.

## Out of Scope

- **Issue fix-direction 3 (runtime geometry-mismatch detection) is deferred (R2).** With no guessed-geometry path left, every `StreamInfo` comes from a decoded descriptor, so the mismatch it would detect cannot arise. Revisit it if a network infohash source is ever added.
- **A cold-infohash network source.**
  - The official engine resolves a cold infohash in about 2.6 s, probably infohash to content id, then `/gettorrent?pid=`.
  - Identifying that request needs a human-approved capture; the spike could not read one.
  - Recommended follow-up issue: add the lookup gated by `infohash_of_descriptor(raw) == infohash`, which would restore cold infohash, magnet and CLI playback. `docs/protocol/compat-matrix.md` already lists reverse `get_content_id` as deferred.
- **Persisting the infohash index** across restarts, or sharing it with one-shot CLI processes.
- **Issue #165** (the native route's `cid:` ergonomics), beyond the `cid:<id>` hint in the refusal message. Issues #168 and #10 are not addressed here.

---

### Task 1: `InfohashIndex` in `ace-swarm`

**Suggested implementer tier:** cheap (complete code given).

**Files:**
- Modify: `crates/ace-swarm/src/resolve.rs`

**Interfaces:**
- Produces:
  - `pub struct ace_swarm::resolve::InfohashIndex`
  - `InfohashIndex::new(capacity: usize) -> Self`
  - `InfohashIndex::put(&self, info: StreamInfo)`
  - `InfohashIndex::get(&self, infohash: &[u8; 20]) -> Option<StreamInfo>`
  - Task 3 consumes all of these.

- [ ] **Step 1: Write the failing tests**

Append these tests inside the existing `#[cfg(test)] mod tests` in `crates/ace-swarm/src/resolve.rs`, after `cache_returns_stored_value_then_expires`:

```rust
    fn indexed_info(infohash: [u8; 20], piece_length: u64) -> StreamInfo {
        StreamInfo {
            infohash,
            piece_length,
            chunk_length: 16_384,
            trackers: vec![],
            metadata: StreamMetadata::default(),
            sig_len: 0,
            source_pubkey: vec![],
        }
    }

    #[test]
    fn infohash_index_returns_only_the_entry_for_that_infohash() {
        let index = InfohashIndex::new(4);
        assert_eq!(index.get(&[1; 20]), None);
        index.put(indexed_info([1; 20], 524_288));
        index.put(indexed_info([2; 20], 65_536));
        assert_eq!(index.get(&[1; 20]).unwrap().piece_length, 524_288);
        assert_eq!(index.get(&[2; 20]).unwrap().piece_length, 65_536);
        assert_eq!(index.get(&[3; 20]), None);
    }

    #[test]
    fn infohash_index_replaces_an_entry_and_evicts_the_oldest_when_full() {
        let index = InfohashIndex::new(2);
        index.put(indexed_info([1; 20], 1));
        index.put(indexed_info([2; 20], 2));
        // Re-storing [1] replaces it in place and makes it the newest entry.
        index.put(indexed_info([1; 20], 10));
        index.put(indexed_info([3; 20], 3));
        assert_eq!(index.get(&[2; 20]), None, "the oldest entry is evicted");
        assert_eq!(index.get(&[1; 20]).unwrap().piece_length, 10);
        assert_eq!(index.get(&[3; 20]).unwrap().piece_length, 3);
    }

    /// A synthetic live transport with 512 KiB pieces and a freshly generated RSA source key,
    /// built with outpace's own encoder (#164). Nothing here comes from a real stream.
    fn synthetic_512k_transport() -> (Vec<u8>, Vec<u8>) {
        use ace_wire::bencode::Bencode;
        let pubkey = ace_wire::live_auth::LiveSourceAuth::generate().pubkey_der();
        let mut d = std::collections::BTreeMap::new();
        d.insert(b"name".to_vec(), Bencode::Bytes(b"Synthetic Live".to_vec()));
        d.insert(b"piece_length".to_vec(), Bencode::Int(524_288));
        d.insert(b"chunk_length".to_vec(), Bencode::Int(16_384));
        d.insert(b"bitrate".to_vec(), Bencode::Int(1_000_000));
        d.insert(b"authmethod".to_vec(), Bencode::Bytes(b"RSA".to_vec()));
        d.insert(b"pubkey".to_vec(), Bencode::Bytes(pubkey.clone()));
        d.insert(
            b"trackers".to_vec(),
            Bencode::List(vec![Bencode::Bytes(b"udp://tracker.invalid:80".to_vec())]),
        );
        (
            ace_wire::transport::encode_transport(&Bencode::Dict(d)),
            pubkey,
        )
    }

    #[test]
    fn synthetic_512k_descriptor_keeps_geometry_and_pubkey_through_the_index() {
        let (transport, pubkey) = synthetic_512k_transport();
        let info = stream_info_from_transport(&transport).unwrap();
        // The infohash is computed at test time, never written down.
        assert_eq!(
            info.infohash,
            ace_wire::infohash::infohash_of_transport(&transport)
        );
        assert_eq!(info.piece_length, 524_288);
        assert_eq!(info.chunk_length, 16_384);
        assert_eq!(info.chunks_per_piece(), 32);
        assert_eq!(info.sig_len, 96, "768-bit source key => 96-byte signature tail");
        assert_eq!(info.source_pubkey, pubkey);

        let index = InfohashIndex::new(8);
        index.put(info.clone());
        assert_eq!(index.get(&info.infohash), Some(info));
    }
```

- [ ] **Step 2: Run the tests and confirm they fail to compile**

Run: `env -u RUSTUP_TOOLCHAIN cargo test -p ace-swarm --lib resolve::tests::infohash_index 2>&1 | tail -5`

Expected: a compile error, `cannot find type`/`failed to resolve: use of undeclared type InfohashIndex`.

- [ ] **Step 3: Implement `InfohashIndex`**

In `crates/ace-swarm/src/resolve.rs`:

1. Change `use std::collections::HashMap;` to `use std::collections::{HashMap, VecDeque};`.
2. Insert this block directly after the closing `}` of `impl ResolveCache { … }`:

```rust
/// Verified live descriptors keyed by their swarm infohash (issue #164).
///
/// Deliberately separate from [`ResolveCache`], which is keyed by the content-id *string*:
/// content ids and infohashes share the 40-hex space, so a shared map could hand one kind of
/// id's entry to a lookup of the other. Every entry here is keyed by its own `info.infohash`,
/// which [`stream_info_from_transport`] computed from the same descriptor that supplied the
/// geometry and pubkey. The swarm infohash commits to `piece_length`, `chunk_length` and
/// `pubkey` (see [`infohash_of_descriptor`]), so an entry can neither pair an infohash with
/// another stream's geometry nor go stale. Entries therefore carry no TTL; the index is bounded
/// by evicting the least recently stored entry once `capacity` is reached.
pub struct InfohashIndex {
    entries: Mutex<VecDeque<StreamInfo>>,
    capacity: usize,
}

impl InfohashIndex {
    /// An empty index holding at most `capacity` descriptors (at least one).
    pub fn new(capacity: usize) -> Self {
        InfohashIndex {
            entries: Mutex::new(VecDeque::new()),
            capacity: capacity.max(1),
        }
    }

    /// Record a verified descriptor under `info.infohash`, replacing any previous entry for that
    /// infohash and evicting the least recently stored entry when full.
    pub fn put(&self, info: StreamInfo) {
        let mut entries = self.entries.lock().unwrap();
        entries.retain(|e| e.infohash != info.infohash);
        if entries.len() >= self.capacity {
            entries.pop_front();
        }
        entries.push_back(info);
    }

    /// The verified descriptor stored for `infohash`, if any.
    pub fn get(&self, infohash: &[u8; 20]) -> Option<StreamInfo> {
        self.entries
            .lock()
            .unwrap()
            .iter()
            .find(|e| &e.infohash == infohash)
            .cloned()
    }
}
```

- [ ] **Step 4: Run the new tests and confirm they pass**

Run: `env -u RUSTUP_TOOLCHAIN cargo test -p ace-swarm --lib resolve::tests 2>&1 | tail -5`

Expected: `test result: ok.` with 0 failed. The three new tests are listed as `ok`.

- [ ] **Step 5: Run the gate** (Global Constraints → The gate). All four commands exit 0.

- [ ] **Step 6: Commit**

```bash
git add crates/ace-swarm/src/resolve.rs
git commit -m "feat(ace-swarm): add a verified infohash descriptor index (#164)" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 2: Provider seam: an `Unresolvable` error and an offline pre-check

**Suggested implementer tier:** cheap (complete code given).

**Files:**
- Modify: `crates/ace-engine/src/provider.rs`
- Modify: `crates/ace-engine/src/manager.rs`

**Interfaces:**
- Produces:
  - `ProviderError::Unresolvable(String)`, with `impl Display for ProviderError`.
  - The defaulted trait methods `StreamProvider::check_openable(&self, id: &str) -> Result<(), ProviderError>` and `StreamProvider::remember_live_descriptor(&self, info: &StreamInfo)`.
  - `StreamManager::check_openable(&self, network: &str, id: &str) -> Result<(), ProviderError>` and `StreamManager::remember_live_descriptor(&self, network: &str, info: &StreamInfo)`.
  - Consumed by Tasks 3, 6, 7 and 8.

- [ ] **Step 1: Write the failing tests**

In `crates/ace-engine/src/provider.rs`, append inside `mod tests` (after `registry_registers_and_looks_up`):

```rust
    #[test]
    fn provider_error_display_is_the_user_facing_message() {
        assert_eq!(
            ProviderError::Unresolvable("use cid:<id>".into()).to_string(),
            "use cid:<id>"
        );
        assert_eq!(ProviderError::Backend("no peers".into()).to_string(), "no peers");
        assert_eq!(ProviderError::NotFound.to_string(), "not found");
    }

    #[test]
    fn default_check_openable_accepts_any_id() {
        assert!(DummyProvider.check_openable("anything").is_ok());
    }
```

In `crates/ace-engine/src/manager.rs`, append inside `mod tests`:

```rust
    #[tokio::test]
    async fn check_openable_is_not_found_for_an_unregistered_network() {
        let manager = StreamManager::new(ProviderRegistry::new());
        assert!(matches!(
            manager.check_openable("nope", "x"),
            Err(ProviderError::NotFound)
        ));
    }

    #[tokio::test]
    async fn check_openable_delegates_to_the_provider_default() {
        let mut registry = ProviderRegistry::new();
        registry.register(Arc::new(TestProvider { chunks: 1 }));
        let manager = StreamManager::new(registry);
        assert!(manager.check_openable("test", "anything").is_ok());
    }
```

- [ ] **Step 2: Run them and confirm they fail to compile**

Run: `env -u RUSTUP_TOOLCHAIN cargo test -p ace-engine --lib -- provider::tests manager::tests::check_openable 2>&1 | tail -5`

Expected: compile errors (`no variant named Unresolvable`, `no method named check_openable`).

- [ ] **Step 3: Implement the seam in `provider.rs`**

1. Change `use ace_swarm::types::StreamMetadata;` to `use ace_swarm::types::{StreamInfo, StreamMetadata};`.
2. Add two defaulted methods at the end of `pub trait StreamProvider`, after `resolve_vod`:

```rust
    /// Offline check that [`open`](Self::open) would get past id resolution, for routes that
    /// hand out playback URLs before opening (compat `/ace/getstream?format=json`). Must not
    /// touch the network or start anything. Defaults to accepting every id.
    fn check_openable(&self, _id: &str) -> Result<(), ProviderError> {
        Ok(())
    }

    /// Record a live descriptor that was resolved outside [`open`](Self::open) (the compat
    /// routes resolve content ids for their JSON responses), so a later open by its infohash can
    /// use it (#164). Defaults to ignoring it.
    fn remember_live_descriptor(&self, _info: &StreamInfo) {}
```

3. Replace the `ProviderError` enum with the following, and add the `Display` impl under it:

```rust
#[derive(Debug)]
pub enum ProviderError {
    NotFound,
    Backend(String),
    /// The id is well-formed, but the provider holds no verified metadata to open it with and
    /// will not guess (#164: a bare infohash without a verified transport descriptor). The
    /// message is user-facing: it explains the refusal and how to open the stream instead, and
    /// is safe to return to HTTP clients and to print in the CLI.
    Unresolvable(String),
}

impl std::fmt::Display for ProviderError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProviderError::NotFound => f.write_str("not found"),
            ProviderError::Backend(msg) | ProviderError::Unresolvable(msg) => f.write_str(msg),
        }
    }
}
```

- [ ] **Step 4: Implement the manager passthroughs in `manager.rs`**

1. Change `use ace_swarm::types::StreamMetadata;` to `use ace_swarm::types::{StreamInfo, StreamMetadata};`.
2. Add these two methods to `impl StreamManager`, directly before `pub async fn get_or_start(`:

```rust
    /// Offline pre-check that `id` can be opened on `network`; see
    /// [`StreamProvider::check_openable`](crate::provider::StreamProvider::check_openable).
    /// `NotFound` if the network is unregistered.
    pub fn check_openable(&self, network: &str, id: &str) -> Result<(), ProviderError> {
        self.registry
            .get(network)
            .ok_or(ProviderError::NotFound)?
            .check_openable(id)
    }

    /// Hand a live descriptor resolved outside `open` to `network`'s provider; see
    /// [`StreamProvider::remember_live_descriptor`](crate::provider::StreamProvider::remember_live_descriptor).
    /// A no-op for an unregistered network.
    pub fn remember_live_descriptor(&self, network: &str, info: &StreamInfo) {
        if let Some(provider) = self.registry.get(network) {
            provider.remember_live_descriptor(info);
        }
    }
```

If `manager.rs` does not already have `StreamProvider` in scope for the method call, the call still resolves, because `registry.get` returns `Arc<dyn StreamProvider>`. If the compiler asks for the trait anyway, add `StreamProvider` to the existing `use crate::provider::{…}` line.

- [ ] **Step 5: Run the new tests and confirm they pass**

Run: `env -u RUSTUP_TOOLCHAIN cargo test -p ace-engine --lib -- provider::tests manager::tests::check_openable 2>&1 | tail -5`

Expected: `test result: ok.` with 0 failed.

- [ ] **Step 6: Run the gate.** All four commands exit 0.

- [ ] **Step 7: Commit**

```bash
git add crates/ace-engine/src/provider.rs crates/ace-engine/src/manager.rs
git commit -m "feat(ace-engine): add an unresolvable provider error and open pre-check (#164)" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 3: `AceProvider`: one live resolver that fails closed on unverified infohashes

**Suggested implementer tier:** standard. Complete code is given, but the edits sit in a 4,600-line file, so placement needs care.

**Files:**
- Modify: `crates/ace-engine/src/ace_provider.rs`

**Interfaces:**
- Consumes:
  - `InfohashIndex` (Task 1);
  - `ProviderError::Unresolvable` and the `check_openable` / `remember_live_descriptor` trait methods (Task 2).
- Produces:
  - `AceProvider::resolve_live_info(&self, id) -> Result<StreamInfo, ProviderError>` (private, async) and `AceProvider::verified_info_for_infohash(&self, id) -> Result<StreamInfo, ProviderError>` (private, sync). Task 5 extends the latter.
  - `#[cfg(test)] pub(crate) mod test_support` with `synthetic_live_transport(piece_length: i64) -> (Vec<u8>, Vec<u8>)`, used by Tasks 5 and 6.
  - `AceProvider::open` no longer calls `stream_info_from_infohash`. Task 4 deletes it.

- [ ] **Step 1: Write the failing tests**

Append inside the existing `#[cfg(test)] mod tests` in `crates/ace-engine/src/ace_provider.rs`, right after `unrecognized_id_shape_is_backend_error`:

```rust
    const PLACEHOLDER_ID: &str = "0123456789abcdef0123456789abcdef01234567";

    fn test_provider() -> AceProvider {
        AceProvider::new(Arc::new(Identity::generate()), 0)
    }

    #[tokio::test]
    async fn bare_infohash_without_a_verified_descriptor_fails_closed() {
        let p = test_provider();
        // No descriptor and no bootstrap peers: the refusal must come before any discovery, so
        // the timeout only trips if open() regressed into tracker/DHT I/O.
        let err = tokio::time::timeout(Duration::from_secs(2), p.open(PLACEHOLDER_ID))
            .await
            .expect("must fail before any network I/O")
            .err()
            .expect("a bare infohash with no verified descriptor must not stream");
        match err {
            ProviderError::Unresolvable(msg) => {
                assert!(msg.contains(&format!("cid:{PLACEHOLDER_ID}")), "{msg}");
            }
            other => panic!("expected Unresolvable, got {other:?}"),
        }
        assert!(matches!(
            p.check_openable(PLACEHOLDER_ID),
            Err(ProviderError::Unresolvable(_))
        ));
    }

    #[tokio::test]
    async fn bare_infohash_uses_the_verified_descriptors_geometry_and_pubkey() {
        let (transport, pubkey) = test_support::synthetic_live_transport(524_288);
        let verified = ace_swarm::resolve::stream_info_from_transport(&transport).unwrap();
        let p = test_provider();
        p.remember_live_descriptor(&verified);

        // Hex ids are case-insensitive: an upper-case infohash must hit the same entry.
        let id = infohash_hex(&verified.infohash).to_ascii_uppercase();
        assert!(p.check_openable(&id).is_ok());
        let info = p.resolve_live_info(&id).await.unwrap();
        assert_eq!(
            info.infohash,
            ace_wire::infohash::infohash_of_transport(&transport)
        );
        assert_eq!(info.piece_length, 524_288, "not the old 1 MiB guess");
        assert_eq!(info.chunk_length, 16_384);
        assert_eq!(info.sig_len, 96);
        assert_eq!(info.source_pubkey, pubkey, "pubkey enables RSA piece verification");
        assert_eq!(info.metadata.title.as_deref(), Some("Synthetic Live"));
    }

    #[tokio::test]
    async fn content_id_resolution_records_the_descriptor_under_its_infohash() {
        let (transport, _) = test_support::synthetic_live_transport(524_288);
        let verified = ace_swarm::resolve::stream_info_from_transport(&transport).unwrap();
        let p = test_provider();
        let ih = infohash_hex(&verified.infohash);
        assert!(p.check_openable(&ih).is_err());

        // A cached content-id resolution (no network) must also feed the infohash index.
        p.resolve_cache.put(PLACEHOLDER_ID, verified.clone());
        let via_cid = p
            .resolve_live_info(&format!("cid:{PLACEHOLDER_ID}"))
            .await
            .unwrap();
        assert_eq!(via_cid, verified);
        assert_eq!(p.resolve_live_info(&ih).await.unwrap(), verified);

        // The content id itself is not an infohash: the two namespaces stay separate (#165).
        assert!(matches!(
            p.check_openable(PLACEHOLDER_ID),
            Err(ProviderError::Unresolvable(_))
        ));
    }

    #[test]
    fn check_openable_leaves_non_infohash_ids_to_open() {
        let p = test_provider();
        assert!(p.check_openable(&format!("cid:{PLACEHOLDER_ID}")).is_ok());
        let turl =
            crate::transport_url::encode_transport_url("https://example.invalid/x.acelive")
                .unwrap();
        assert!(p.check_openable(&turl).is_ok());
    }
```

- [ ] **Step 2: Run them and confirm they fail to compile**

Run: `env -u RUSTUP_TOOLCHAIN cargo test -p ace-engine --lib ace_provider::tests 2>&1 | tail -5`

Expected: compile errors (`could not find test_support`, `no method named resolve_live_info`).

- [ ] **Step 3: Add the index field and constant**

1. In the `use ace_swarm::resolve::{…};` block at the top:
   - remove `stream_info_from_infohash`;
   - add `InfohashIndex`.

   The block becomes:

```rust
use ace_swarm::resolve::{
    catalog_transport_bytes, hex20, infohash_hex, resolve_via_catalog, resolve_via_peer,
    stream_info_from_transport_url, transport_bytes_from_url, transport_bytes_via_peer,
    vod_info_from_transport, InfohashIndex, ResolveCache, ResolveError,
};
```

2. After `const RESOLVE_CACHE_TTL: Duration = Duration::from_secs(300);` add:

```rust
/// How many verified live descriptors the infohash index keeps (#164). Each entry is a few KiB
/// at most (geometry, trackers, pubkey, metadata).
const INFOHASH_INDEX_CAPACITY: usize = 256;
```

3. In `pub struct AceProvider`, directly after the `resolve_cache: ResolveCache,` field, add:

```rust
    /// Verified live descriptors keyed by swarm infohash, filled by every successful `cid:`
    /// resolution (never transport-url; Ruling G) so a later bare-infohash open uses the real
    /// geometry and pubkey (#164). Separate from `resolve_cache`, keyed by the content-id string.
    infohash_index: InfohashIndex,
```

4. In `AceProvider::new`, directly after `resolve_cache: ResolveCache::new(RESOLVE_CACHE_TTL),`, add:

```rust
            infohash_index: InfohashIndex::new(INFOHASH_INDEX_CAPACITY),
```

- [ ] **Step 4: Record every content-id resolution in the index**

In `async fn resolve_content_id`, make three edits:

1. Replace the cache-hit early return:

```rust
        if let Some(info) = self.resolve_cache.get(content_id) {
            return Ok(info);
        }
```

   with:

```rust
        if let Some(info) = self.resolve_cache.get(content_id) {
            // Re-record: the bounded infohash index may have evicted it since (#164).
            self.infohash_index.put(info.clone());
            return Ok(info);
        }
```

2. In the catalog `Ok(info)` arm, after `self.resolve_cache.put(content_id, info.clone());`, add `self.infohash_index.put(info.clone());`.
3. In the peer `Ok(info)` arm, after `self.resolve_cache.put(content_id, info.clone());`, add `self.infohash_index.put(info.clone());`.

- [ ] **Step 5: Add the single resolver**

Inside `impl AceProvider { … }`, directly after the end of `async fn resolve_content_id`, insert:

```rust
    /// Resolve a live `id` to a verified [`StreamInfo`]: the single resolver behind every live
    /// entry point (native `/streams`, compat `/ace/getstream` + `/ace/manifest.m3u8`, and
    /// `outpace play`). See #164.
    ///
    /// - `cid:<40hex>`: signed catalog, then BEP-9 peers ([`Self::resolve_content_id`]).
    /// - a transport-url id: fetched under the SSRF guard.
    /// - a bare 40-hex infohash: only a descriptor this process has already verified
    ///   ([`Self::verified_info_for_infohash`]); otherwise [`ProviderError::Unresolvable`].
    ///
    /// Every descriptor resolved here is recorded in the infohash index, so the stream can later
    /// be opened by its infohash too. outpace never guesses live geometry.
    async fn resolve_live_info(&self, id: &str) -> Result<StreamInfo, ProviderError> {
        if let Some(content_id) = id.strip_prefix("cid:") {
            return self.resolve_content_id(content_id).await;
        }
        if let Some(url) = crate::transport_url::decode_transport_url(id) {
            let info = stream_info_from_transport_url(&url)
                .await
                .map_err(|e| ProviderError::Backend(format!("transport url: {e:?}")))?;
            self.infohash_index.put(info.clone());
            return Ok(info);
        }
        if is_bare_hex40(id) {
            return match self.verified_info_for_infohash(id) {
                Ok(info) => {
                    crate::alog!("[ace] open {id}: using a descriptor verified in this process");
                    Ok(info)
                }
                Err(e) => {
                    crate::alog!("[ace] open {id}: refused, no verified descriptor for it");
                    Err(e)
                }
            };
        }
        Err(ProviderError::Backend(
            "id must be a 40-hex infohash, cid:<40hex>, or a transport-url id".into(),
        ))
    }

    /// The verified live descriptor for a bare 40-hex infohash, or
    /// [`ProviderError::Unresolvable`] with a user-facing reason. Offline and synchronous, so
    /// the compat routes can pre-check an id before minting playback URLs
    /// ([`StreamProvider::check_openable`]).
    fn verified_info_for_infohash(&self, id: &str) -> Result<StreamInfo, ProviderError> {
        let infohash = hex20(id).map_err(|_| ProviderError::Backend("bad infohash".into()))?;
        if let Some(info) = self.infohash_index.get(&infohash) {
            return Ok(info);
        }
        Err(ProviderError::Unresolvable(unresolved_infohash_message(id)))
    }
```

Then add these two free functions directly above `fn derived_prefetch_pieces(`, at module level outside the `impl`:

```rust
/// Whether `id` is a bare 40-hex string: a swarm infohash, or a content id missing its `cid:`
/// prefix (#165). The two are indistinguishable by shape.
fn is_bare_hex40(id: &str) -> bool {
    id.len() == 40 && id.bytes().all(|b| b.is_ascii_hexdigit())
}

/// The user-facing reason a bare 40-hex id cannot be opened (#164), with the `cid:` hint for a
/// content id pasted without its prefix (#165).
fn unresolved_infohash_message(id: &str) -> String {
    format!(
        "no verified transport descriptor for infohash {id}: outpace does not guess live stream \
         geometry. Open the stream by content id (cid:<content-id>) first; its \
         infohash then works in this process. If {id} is a content id, use cid:{id}"
    )
}
```

- [ ] **Step 6: Route `open()` through the resolver and implement the trait methods**

In `impl StreamProvider for AceProvider`, replace the head of `async fn open` from the comment through the end of the `let info = … ;` statement. That is everything before `// Bootstrap peers are the proven/direct path`:

```rust
    async fn open(&self, id: &str) -> Result<Box<dyn TsSource>, ProviderError> {
        // One resolver for every live entry point. A bare infohash without a verified
        // descriptor fails closed here, before any discovery (#164).
        let info = self.resolve_live_info(id).await?;
```

Add these two methods to the same `impl StreamProvider for AceProvider` block, after `resolve_vod`:

```rust
    fn check_openable(&self, id: &str) -> Result<(), ProviderError> {
        if is_bare_hex40(id) {
            self.verified_info_for_infohash(id).map(|_| ())
        } else {
            Ok(())
        }
    }

    fn remember_live_descriptor(&self, info: &StreamInfo) {
        self.infohash_index.put(info.clone());
    }
```

- [ ] **Step 7: Add the shared test-support builder**

Directly above the existing `#[cfg(test)]` that precedes `mod tests {` in `ace_provider.rs`, insert:

```rust
/// Test-only builders shared by the provider and HTTP tests.
#[cfg(test)]
pub(crate) mod test_support {
    use ace_wire::bencode::Bencode;
    use std::collections::BTreeMap;

    /// A synthetic live `AceStreamTransport` built with outpace's own encoder and a freshly
    /// generated RSA source key (#164). Nothing is derived from a real stream; callers compute
    /// its infohash at test time. Returns `(transport_bytes, pubkey_der)`.
    pub(crate) fn synthetic_live_transport(piece_length: i64) -> (Vec<u8>, Vec<u8>) {
        let pubkey = ace_wire::live_auth::LiveSourceAuth::generate().pubkey_der();
        let mut d: BTreeMap<Vec<u8>, Bencode> = BTreeMap::new();
        d.insert(b"name".to_vec(), Bencode::Bytes(b"Synthetic Live".to_vec()));
        d.insert(b"piece_length".to_vec(), Bencode::Int(piece_length));
        d.insert(b"chunk_length".to_vec(), Bencode::Int(16_384));
        d.insert(b"bitrate".to_vec(), Bencode::Int(1_000_000));
        d.insert(b"authmethod".to_vec(), Bencode::Bytes(b"RSA".to_vec()));
        d.insert(b"pubkey".to_vec(), Bencode::Bytes(pubkey.clone()));
        d.insert(
            b"trackers".to_vec(),
            Bencode::List(vec![Bencode::Bytes(b"udp://tracker.invalid:80".to_vec())]),
        );
        (
            ace_wire::transport::encode_transport(&Bencode::Dict(d)),
            pubkey,
        )
    }
}
```

- [ ] **Step 8: Replace the last in-crate use of `stream_info_from_infohash`**

In `fn continuity_uses_configured_live_recovery_bounds` (`mod tests`), replace:

```rust
        let info =
            stream_info_from_infohash("0123456789abcdef0123456789abcdef01234567", vec![]).unwrap();
```

with this fixture. It keeps the old geometry, because the test is about scheduling bounds, not resolution:

```rust
        let info = StreamInfo {
            infohash: [0x01; 20],
            piece_length: 1_048_576,
            chunk_length: 16_384,
            trackers: vec![],
            metadata: StreamMetadata::default(),
            sig_len: 96,
            source_pubkey: vec![],
        };
```

- [ ] **Step 9: Update the stale comments in this file**

1. In the module doc at the top, replace `fallback (see [`ace_swarm::resolve`]); the infohash form works directly.` with:

```rust
//! fallback (see [`ace_swarm::resolve`]). A bare infohash opens only from a transport descriptor
//! this process has already verified, and otherwise fails closed (issue #164).
```

2. In `Continuity::fresh`, replace the comment sentence `A bare-infohash stream has no `source_pubkey`, so it only strips; `with_source_pubkey` is then a no-op.` with `A descriptor without a parseable pubkey yields `sig_len == 0` and an empty `source_pubkey`, so nothing is stripped or verified.`

- [ ] **Step 10: Run the provider tests and confirm they pass**

Run: `env -u RUSTUP_TOOLCHAIN cargo test -p ace-engine --lib ace_provider::tests 2>&1 | tail -5`

Expected: `test result: ok.` with 0 failed. The four new tests pass, as do `unrecognized_id_shape_is_backend_error` and `continuity_uses_configured_live_recovery_bounds`.

- [ ] **Step 11: Run the gate.** All four commands exit 0.

- [ ] **Step 12: Commit**

```bash
git add crates/ace-engine/src/ace_provider.rs
git commit -m "fix(ace-engine): fail closed on bare infohash without a verified descriptor (#164)" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 4: Delete the guessed-geometry path from `ace-swarm`

**Suggested implementer tier:** cheap (complete edits given).

**Files:**
- Modify: `crates/ace-swarm/src/resolve.rs`
- Modify: `crates/ace-swarm/src/types.rs`

**Interfaces:**
- Removes these public items, which have no remaining callers after Task 3:
  - `ace_swarm::resolve::stream_info_from_infohash`
  - `ace_swarm::resolve::DEFAULT_PIECE_LENGTH`
  - `ace_swarm::types::DEFAULT_SIG_LEN`
- Keeps `DEFAULT_CHUNK_LENGTH`, which `validate_geometry` uses.
- Decision (R4): delete rather than keep. No legitimate caller remains, and a public builder of guessed geometry invites this bug back.

- [ ] **Step 1: Confirm there are no callers left outside the items being deleted**

Run: `rg -n 'stream_info_from_infohash|DEFAULT_PIECE_LENGTH|DEFAULT_SIG_LEN' crates tools`

Expected: hits only in `crates/ace-swarm/src/resolve.rs` (the definitions, the `MAX_PIECE_LENGTH` doc, and tests `infohash_form_uses_default_geometry` / `bad_infohash_rejected`) and in `crates/ace-swarm/src/types.rs` (the const and its test). If anything else appears, stop and report it.

- [ ] **Step 2: Edit `resolve.rs`**

1. Replace the module doc (the first 10 `//!` lines) with:

```rust
//! Resolve a stream identifier to a downloadable [`StreamInfo`].
//!
//! Every live [`StreamInfo`] comes from a decoded `AceStreamTransport` descriptor
//! ([`stream_info_from_transport`]): the descriptor supplies the swarm infohash, geometry,
//! trackers and source pubkey. The descriptor is located by content id (the signed catalog,
//! then BEP-9 `ut_metadata` as a fallback) or fetched from a transport-file URL. A bare
//! infohash carries no geometry, so it is never turned into a `StreamInfo` by guessing
//! (issue #164); callers keep descriptors they have verified in an [`InfohashIndex`].
```

2. Replace the two lines:

```rust
/// Default live geometry when only an infohash is known: 1 MiB pieces / 16 KiB chunks.
pub const DEFAULT_PIECE_LENGTH: u64 = 1_048_576;
pub const DEFAULT_CHUNK_LENGTH: u64 = 16_384;
```

   with:

```rust
/// The fixed Acestream chunk size (16 KiB) that the wire codec and every descriptor use.
pub const DEFAULT_CHUNK_LENGTH: u64 = 16_384;
```

3. Replace the whole `MAX_PIECE_LENGTH` doc comment, which currently links `[`DEFAULT_PIECE_LENGTH`]`, so the const reads:

```rust
/// Upper bound on a transport descriptor's advertised `piece_length` (bytes).
///
/// The piece length is untrusted and sizes the [`PieceReassembler`](ace_wire::reassembly)
/// per-piece buffer (`vec![0u8; piece_length]`, allocated on the first block of every
/// in-flight piece) as well as the request fan-out. Real Acestream geometry is small: 64 KiB
/// source-node pieces (`broadcast::PIECE_LENGTH`) and 512 KiB-1 MiB public live pieces. This
/// ceiling leaves generous headroom for higher-bitrate sources while bounding the allocation a
/// hostile transport can force at stream start.
pub const MAX_PIECE_LENGTH: u64 = 8 * 1_048_576;
```
4. Delete the whole `stream_info_from_infohash` function, including its two `///` doc lines.
5. In `mod tests`, delete `fn infohash_form_uses_default_geometry` and replace `fn bad_infohash_rejected` with:

```rust
    #[test]
    fn bad_infohash_rejected() {
        assert_eq!(hex20("xyz"), Err(ResolveError::BadInfohash));
        assert_eq!(hex20(&"z".repeat(40)), Err(ResolveError::BadInfohash));
    }
```

- [ ] **Step 3: Edit `types.rs`**

1. Delete the `DEFAULT_SIG_LEN` const and its three-line doc comment.
2. In `chunks_per_piece_is_64_for_1mib_pieces`, change `sig_len: DEFAULT_SIG_LEN,` to `sig_len: 96,`.
3. In `StreamInfo`:
   - replace the `metadata` field doc `/// Human-readable descriptor metadata, empty for a bare infohash.` with `/// Human-readable descriptor metadata (empty when the descriptor carries none).`
   - in the `source_pubkey` doc, replace the last sentence (`Only a resolved transport descriptor carries this; a bare infohash has no source key, so this is empty and pieces are stripped but not verified.`) with `Empty when the descriptor has no parseable RSA pubkey; such a stream is treated as unsigned (nothing stripped, nothing verified).`

- [ ] **Step 4: Verify that nothing references the deleted items**

Run: `rg -n 'stream_info_from_infohash|DEFAULT_PIECE_LENGTH|DEFAULT_SIG_LEN' crates tools`

Expected: no output, exit 1.

- [ ] **Step 5: Run the gate.** All four commands exit 0.

- [ ] **Step 6: Commit**

```bash
git add crates/ace-swarm/src/resolve.rs crates/ace-swarm/src/types.rs
git commit -m "refactor(ace-swarm): remove guessed infohash geometry (#164)" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 5: Open the daemon's own broadcasts by infohash (R3b)

**Suggested implementer tier:** cheap (complete code given).

This is cheap and real. `PUT /broadcast/<name>` and `outpace broadcast` both print the infohash. Before this task, opening that infohash on the same daemon guessed 1 MiB pieces for a 64 KiB broadcast. The shared `SeedRegistry` already holds the broadcast transport, under its content id. The controller may drop this task without affecting Tasks 6-9.

**Files:**
- Modify: `crates/ace-swarm/src/listen.rs`
- Modify: `crates/ace-engine/src/ace_provider.rs`

**Interfaces:**
- Produces: `SeedRegistry::broadcast_transport_for_infohash(&self, infohash: &[u8; 20]) -> Option<Arc<Vec<u8>>>`.
- Extends `AceProvider::verified_info_for_infohash` (Task 3).

- [ ] **Step 1: Write the failing tests**

In `crates/ace-swarm/src/listen.rs`, append inside `mod tests` after `broadcast_lease_owns_both_infohash_and_content_id`:

```rust
    #[test]
    fn broadcast_transport_is_found_by_its_swarm_infohash_only() {
        use ace_wire::bencode::Bencode;
        let mut d = std::collections::BTreeMap::new();
        d.insert(b"name".to_vec(), Bencode::Bytes(b"Synthetic".to_vec()));
        d.insert(b"authmethod".to_vec(), Bencode::Bytes(b"RSA".to_vec()));
        d.insert(b"pubkey".to_vec(), Bencode::Bytes(b"k".to_vec()));
        d.insert(b"piece_length".to_vec(), Bencode::Int(65_536));
        d.insert(b"chunk_length".to_vec(), Bencode::Int(16_384));
        d.insert(b"bitrate".to_vec(), Bencode::Int(1));
        let transport = ace_wire::transport::encode_transport(&Bencode::Dict(d));
        let ih = ace_wire::infohash::infohash_of_transport(&transport);
        let cid = ace_wire::infohash::transport_file_hash(&transport);

        let reg = SeedRegistry::new();
        assert!(reg.broadcast_transport_for_infohash(&ih).is_none());
        let (_store, lease) =
            reg.lease_broadcast(ih, cid, transport.clone(), || PieceStore::new(4, 4, 1024));
        assert_eq!(
            reg.broadcast_transport_for_infohash(&ih).as_deref(),
            Some(&transport)
        );
        assert!(
            reg.broadcast_transport_for_infohash(&cid).is_none(),
            "a content id is not a swarm infohash"
        );
        drop(lease);
        assert!(reg.broadcast_transport_for_infohash(&ih).is_none());
        // Metadata registered outside a broadcast lease is not an originated broadcast.
        reg.register_metadata(cid, transport);
        assert!(reg.broadcast_transport_for_infohash(&ih).is_none());
    }
```

In `crates/ace-engine/src/ace_provider.rs`, append inside `mod tests` after the Task 3 tests:

```rust
    #[tokio::test]
    async fn own_broadcast_opens_by_infohash_with_its_minted_geometry() {
        let seed = SeedRegistry::new();
        let broadcasts = crate::broadcast::BroadcastRegistry::new();
        let (bc, _) = broadcasts
            .start_or_resume(
                "t164",
                "T164",
                &["udp://tracker.invalid:80".into()],
                &seed,
                1 << 20,
            )
            .await;
        let id = infohash_hex(&bc.infohash);

        // A provider that does not share the broadcast's registry still refuses it.
        assert!(matches!(
            test_provider().check_openable(&id),
            Err(ProviderError::Unresolvable(_))
        ));

        let p = test_provider().with_seed_registry(seed);
        assert!(p.check_openable(&id).is_ok());
        let info = p.resolve_live_info(&id).await.unwrap();
        assert_eq!(info.infohash, bc.infohash);
        assert_eq!(info.piece_length, crate::broadcast::PIECE_LENGTH);
        assert_eq!(info.source_pubkey, bc.auth.pubkey_der());
    }
```

- [ ] **Step 2: Run them and confirm they fail**

Run: `env -u RUSTUP_TOOLCHAIN cargo test -p ace-swarm --lib listen::tests::broadcast_transport 2>&1 | tail -5`

Expected: a compile error (`no method named broadcast_transport_for_infohash`).

- [ ] **Step 3: Implement the registry lookup**

In `crates/ace-swarm/src/listen.rs`, add this method to `impl SeedRegistry`, directly after `pub fn metadata(…)`:

```rust
    /// The transport descriptor of a broadcast this registry originates whose swarm infohash is
    /// `infohash`, if any (#164). Broadcast transports are registered under their content id
    /// (the BEP-9 metadata key), so this scans broadcast-owned metadata and keeps the descriptor
    /// that hashes to `infohash`. A daemon originates a handful of broadcasts at most, so the
    /// scan is cheap; hashing happens after the registry lock is released.
    pub fn broadcast_transport_for_infohash(&self, infohash: &[u8; 20]) -> Option<Arc<Vec<u8>>> {
        let candidates: Vec<SharedMetadata> = {
            let map = self.stores.lock().unwrap();
            map.values()
                .filter(|e| e.kind == OwnerKind::Broadcast)
                .filter_map(|e| e.metadata.clone())
                .collect()
        };
        candidates.into_iter().find(|meta| {
            ace_wire::infohash::try_infohash_of_transport(meta).is_ok_and(|ih| &ih == infohash)
        })
    }
```

- [ ] **Step 4: Use it in the provider**

In `crates/ace-engine/src/ace_provider.rs`:

1. Add `stream_info_from_transport` to the `use ace_swarm::resolve::{…}` list, keeping the list alphabetical, as rustfmt does.
2. In `fn verified_info_for_infohash`, insert this block between the index lookup and the final `Err(…)`:

```rust
        // A broadcast this daemon originates: the shared seed registry holds its transport,
        // which outpace minted itself. Decoding recomputes the infohash from those bytes.
        if let Some(transport) = self.seed_registry.broadcast_transport_for_infohash(&infohash) {
            if let Ok(info) = stream_info_from_transport(&transport) {
                if info.infohash == infohash {
                    self.infohash_index.put(info.clone());
                    return Ok(info);
                }
            }
        }
```

- [ ] **Step 5: Run the tests and confirm they pass**

```bash
env -u RUSTUP_TOOLCHAIN cargo test -p ace-swarm --lib listen::tests 2>&1 | tail -3
env -u RUSTUP_TOOLCHAIN cargo test -p ace-engine --lib ace_provider::tests::own_broadcast 2>&1 | tail -3
```

Expected: both runs end with `test result: ok.` and 0 failed.

- [ ] **Step 6: Run the gate.** All four commands exit 0.

- [ ] **Step 7: Commit**

```bash
git add crates/ace-swarm/src/listen.rs crates/ace-engine/src/ace_provider.rs
git commit -m "feat(ace-engine): open own broadcasts by infohash from their transport (#164)" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 6: HTTP: surface the refusal on native routes; pre-check compat selectors

**Suggested implementer tier:** standard. The code is given, but it touches two request flows.

**Files:**
- Modify: `crates/ace-engine/src/http.rs`

**Interfaces:**
- Consumes:
  - `ProviderError::Unresolvable` and `StreamManager::check_openable` (Task 2);
  - `AceProvider` refusals (Task 3);
  - `crate::ace_provider::test_support::synthetic_live_transport` (Task 3, tests only);
  - `StreamManager::remember_live_descriptor` (Task 2, tests only).
- Produces:
  - Native `GET /streams/<net>/<id>`, `.ts` and `.m3u8`: `422 text/plain` with the reason when the provider returns `Unresolvable`. All other start failures keep the bodiless `404`.
  - Compat `/ace/getstream` (direct and `format=json`) and `/ace/manifest.m3u8`: HTTP `200` with `{ "response": null, "error": <reason> }` for an `infohash=` or `magnet=` selector the provider refuses. No lease is minted.

- [ ] **Step 1: Write the failing tests**

Append inside `mod tests` in `crates/ace-engine/src/http.rs`:

```rust
    const UNRESOLVED_IH: &str = "0123456789abcdef0123456789abcdef01234567";

    /// An app whose `ace` network is a real `AceProvider` with no peers and an empty infohash
    /// index, so bare-infohash requests exercise the #164 refusal without network I/O.
    fn ace_provider_state() -> AppState {
        let mut registry = ProviderRegistry::new();
        registry.register(Arc::new(crate::ace_provider::AceProvider::new(
            Arc::new(ace_wire::identity::Identity::generate()),
            0,
        )));
        AppState {
            manager: StreamManager::new(registry),
            networks: vec!["ace".into()],
            resolve_content_ids_in_getstream: false,
            ace_sessions: Arc::new(AceSessionStore::default()),
            experimental_ace_compat: true,
            broadcasts: None,
        }
    }

    async fn body_text(resp: Response) -> String {
        let bytes = axum::body::to_bytes(resp.into_body(), 1 << 20)
            .await
            .unwrap();
        String::from_utf8(bytes.to_vec()).unwrap()
    }

    #[tokio::test]
    async fn native_routes_refuse_a_bare_infohash_without_a_descriptor() {
        for path in [
            format!("/streams/ace/{UNRESOLVED_IH}"),
            format!("/streams/ace/{UNRESOLVED_IH}.ts"),
            format!("/streams/ace/{UNRESOLVED_IH}.m3u8"),
        ] {
            let resp = router(ace_provider_state())
                .oneshot(Request::get(path.as_str()).body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(resp.status(), StatusCode::UNPROCESSABLE_ENTITY, "{path}");
            let body = body_text(resp).await;
            assert!(body.contains(&format!("cid:{UNRESOLVED_IH}")), "{path}: {body}");
        }
    }

    #[tokio::test]
    async fn compat_routes_refuse_a_bare_infohash_before_minting_a_lease() {
        let state = ace_provider_state();
        let sessions = state.ace_sessions.clone();
        let app = router(state);
        for query in [
            format!("/ace/getstream?format=json&infohash={UNRESOLVED_IH}"),
            format!("/ace/getstream?infohash={UNRESOLVED_IH}"),
            format!("/ace/getstream?format=json&magnet=magnet%3A%3Fxt%3Durn%3Abtih%3A{UNRESOLVED_IH}"),
            format!("/ace/manifest.m3u8?format=json&infohash={UNRESOLVED_IH}"),
        ] {
            let resp = app
                .clone()
                .oneshot(Request::get(query.as_str()).body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(resp.status(), StatusCode::OK, "{query}");
            let json: serde_json::Value = serde_json::from_str(&body_text(resp).await).unwrap();
            assert!(json["response"].is_null(), "{query}: {json}");
            let error = json["error"].as_str().unwrap_or_default();
            assert!(error.contains(&format!("cid:{UNRESOLVED_IH}")), "{query}: {error}");
        }
        assert_eq!(sessions.active_count(), 0, "a refused selector mints no lease");
    }

    #[tokio::test]
    async fn compat_getstream_mints_urls_for_an_infohash_with_a_verified_descriptor() {
        let state = ace_provider_state();
        let (transport, _) =
            crate::ace_provider::test_support::synthetic_live_transport(524_288);
        let info = ace_swarm::resolve::stream_info_from_transport(&transport).unwrap();
        state.manager.remember_live_descriptor("ace", &info);
        let ih = infohash_hex(&info.infohash);
        // JSON mode only mints URLs; it never opens the stream, so this stays offline.
        let resp = router(state)
            .oneshot(
                Request::get(format!("/ace/getstream?format=json&infohash={ih}").as_str())
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_str(&body_text(resp).await).unwrap();
        assert!(json["error"].is_null(), "{json}");
        assert_eq!(json["response"]["infohash"], ih);
    }
```

- [ ] **Step 2: Run them and confirm they fail**

Run: `env -u RUSTUP_TOOLCHAIN cargo test -p ace-engine --lib -- http::tests::native_routes_refuse http::tests::compat_ 2>&1 | tail -8`

The filters go after `--`, because libtest accepts several filters and cargo accepts only one.

Expected: `native_routes_refuse_…` fails with `left: 404, right: 422`, and `compat_routes_refuse_…` fails because `response` is non-null. `compat_getstream_mints_urls_…` may already pass, which is fine: it guards the positive path.

- [ ] **Step 3: Add the native error mapping**

In `http.rs`, directly above `/// `GET /streams/{network}/{id}` or `{id}.ts` …` (the doc comment of `async fn stream_file`), add:

```rust
/// Response for a failed stream start on the native routes. `Unresolvable` carries a user-facing
/// reason (a bare infohash without a verified descriptor, #164) and is returned as `422` with
/// that text; every other failure keeps the historical bodiless `404`.
fn stream_start_error(e: ProviderError) -> Response {
    match e {
        ProviderError::Unresolvable(reason) => {
            (StatusCode::UNPROCESSABLE_ENTITY, reason).into_response()
        }
        _ => StatusCode::NOT_FOUND.into_response(),
    }
}
```

In `async fn stream_file_with_hls_timeout`, make two edits:
- in the `.m3u8` branch, change `Err(_) => StatusCode::NOT_FOUND.into_response(),` to `Err(e) => stream_start_error(e),`;
- in the `get_or_start` match at the end, change `Err(_) => return StatusCode::NOT_FOUND.into_response(),` to `Err(e) => return stream_start_error(e),`.

- [ ] **Step 4: Pre-check compat selectors**

Replace the whole `async fn resolve_ace_selection` with:

```rust
async fn resolve_ace_selection(
    s: &AppState,
    params: &HashMap<String, String>,
) -> Result<AceStreamSelection, String> {
    let mut selection = ace_selected_stream(params)?;
    let network = ace_network(s);
    if s.resolve_content_ids_in_getstream {
        if let Some(content_id) = selection.content_id.as_deref() {
            match resolve_via_catalog(content_id).await {
                Ok(info) => {
                    selection =
                        selection.with_resolved_stream(infohash_hex(&info.infohash), info.metadata);
                }
                Err(e) => crate::alog!(
                    "[ace] content-id catalog resolution failed, falling back to cid: {e:?}"
                ),
            }
        }
    }
    // A bare infohash (`infohash=` or `magnet=`) carries no descriptor: refuse it before minting
    // playback URLs unless the provider already holds a verified one (#164).
    if let Some(network) = network.as_deref() {
        if let Err(ProviderError::Unresolvable(reason)) =
            s.manager.check_openable(network, &selection.session_key)
        {
            return Err(reason);
        }
    }
    Ok(selection)
}
```

The two callers (`ace_getstream`, `ace_manifest`) need no change, because `json!({ "error": error })` accepts a `String`. `ace_selected_stream` keeps returning `&'static str`; `?` converts it.

- [ ] **Step 5: Run the HTTP tests and confirm they pass**

Run: `env -u RUSTUP_TOOLCHAIN cargo test -p ace-engine --lib http::tests 2>&1 | tail -5`

Expected: `test result: ok.` with 0 failed. Every existing `?infohash=` test uses fixture providers on network `fix`, whose default `check_openable` accepts every id, so they are unchanged.

- [ ] **Step 6: Run the gate.** All four commands exit 0.

- [ ] **Step 7: Commit**

```bash
git add crates/ace-engine/src/http.rs
git commit -m "fix(ace-engine): surface infohash refusals on native and compat routes (#164)" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 7: Compat and `/server/api` content-id resolutions feed the index (completes R3a)

**Suggested implementer tier:** cheap (complete code given).

Three `http.rs` paths resolve a descriptor outside `AceProvider`, then hand the client an infohash:
- `/ace/getstream?content_id=` (JSON);
- `/server/api?method=analyze_content`;
- `/server/api?method=get_media_files`.

A client that later opens that infohash should succeed. These paths are gated by `resolve_content_ids_in_getstream`, which tests disable to stay offline. Offline coverage is therefore the Task 2 and Task 3 seam tests, plus Task 10's live check.

**Files:**
- Modify: `crates/ace-engine/src/http.rs`

- [ ] **Step 1: Record the descriptor in `resolve_ace_selection`**

In the `Ok(info) => { … }` arm written in Task 6, insert before `selection = …`:

```rust
                    // A client may open this infohash directly later (#164).
                    if let Some(network) = network.as_deref() {
                        s.manager.remember_live_descriptor(network, &info);
                    }
```

- [ ] **Step 2: Record descriptors in `resolve_server_api_selector`**

In `async fn resolve_server_api_selector`, make two edits:
- in the `Selector::ContentId(cid)` arm, change `Ok(info) => Ok(ResolvedContent {` into a block that records the descriptor first;
- leave the `Selector::Url(url)` arm unchanged: transport URLs are caller-supplied and must not feed the shared index (Ruling G).

The `ContentId` arm becomes:

```rust
        Selector::ContentId(cid) => {
            if !s.resolve_content_ids_in_getstream {
                return Err("content-id catalog resolution is disabled".to_string());
            }
            match resolve_via_catalog(&cid).await {
                Ok(info) => {
                    if let Some(network) = ace_network(s) {
                        s.manager.remember_live_descriptor(&network, &info);
                    }
                    Ok(ResolvedContent {
                        infohash: infohash_hex(&info.infohash),
                        content_id: Some(cid),
                        is_live: true,
                    })
                }
                Err(e) => Err(format!("content-id resolution failed: {e:?}")),
            }
        }
```

- [ ] **Step 3: Run the HTTP tests** with `env -u RUSTUP_TOOLCHAIN cargo test -p ace-engine --lib http::tests 2>&1 | tail -3`.

Expected: `test result: ok.`

- [ ] **Step 4: Run the gate.** All four commands exit 0.

- [ ] **Step 5: Commit**

```bash
git add crates/ace-engine/src/http.rs
git commit -m "fix(ace-engine): record compat content-id resolutions by infohash (#164)" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 8: CLI: readable refusal and accurate `play` help

**Suggested implementer tier:** cheap (complete code given).

**Files:**
- Modify: `crates/ace-engine/src/cli.rs`

- [ ] **Step 1: Write the failing test and update the help assertion**

In `mod tests`, make two edits. The existing `use super::{…}` line already imports `play_provider_from_config` and `PlaybackTarget`.

1. Add this test:

```rust
    #[tokio::test]
    async fn play_infohash_and_magnet_inputs_fail_closed_with_the_cid_hint() {
        use crate::provider::StreamProvider;
        let ih = "0123456789abcdef0123456789abcdef01234567";
        let provider = play_provider_from_config(
            std::sync::Arc::new(ace_wire::identity::Identity::generate()),
            &crate::config::Config::default(),
            vec![],
            ace_swarm::listen::SeedRegistry::new(),
        );
        for input in [
            format!("acestream:?infohash={ih}"),
            format!("magnet:?xt=urn:btih:{ih}"),
        ] {
            let target = PlaybackTarget::parse(&input).unwrap();
            let err = provider
                .open(&target.provider_id)
                .await
                .err()
                .expect("a one-shot play has no verified descriptor for a bare infohash");
            assert!(err.to_string().contains(&format!("cid:{ih}")), "{input}: {err}");
        }
    }
```

2. In `help_identifies_native_surface_and_describes_each_command`, replace `assert!(play.contains("Acestream URL, magnet URI, or HTTP(S) transport-file URL"));` with the lines below. Whitespace is normalized first, so clap line wrapping cannot split a phrase:

```rust
        let play_flat = play.split_whitespace().collect::<Vec<_>>().join(" ");
        assert!(play_flat.contains("acestream://<content-id> URL or HTTP(S) transport-file URL"));
        assert!(play_flat.contains("fail closed"));
```

- [ ] **Step 2: Run the CLI tests and confirm the help test fails**

Run: `env -u RUSTUP_TOOLCHAIN cargo test -p ace-engine --lib cli::tests 2>&1 | tail -8`

Expected: `help_identifies_native_surface_and_describes_each_command` fails. `play_infohash_and_magnet_inputs_fail_closed_with_the_cid_hint` already passes after Task 3, which pins the CLI path.

- [ ] **Step 3: Update the help text and error output**

1. Replace the doc comment on `PlayArgs::input`, which is `/// Acestream URL, magnet URI, or HTTP(S) transport-file URL.`, with:

```rust
    /// acestream://<content-id> URL or HTTP(S) transport-file URL. `acestream:?infohash=` and
    /// magnet inputs are accepted but fail closed: a one-shot play holds no verified transport
    /// descriptor for a bare infohash.
```

2. In `run_play`, make the two provider-error mappings user-readable and echo them for the human watching:
   - Replace the VOD branch's `.map_err(|e| std::io::Error::other(format!("{e:?}")))?;` after `.resolve_vod(&target.provider_id).await`.
   - Replace the live branch's `.map_err(|e| std::io::Error::other(format!("{e:?}")))?;` after `.open(&target.provider_id).await`.

   Each becomes:

```rust
            .map_err(|e| {
                eprintln!("outpace play: {e}");
                std::io::Error::other(e.to_string())
            })?;
```

   Indent the replacement to match its call site. Leave the `open_range` mapping unchanged.

- [ ] **Step 4: Run the CLI tests** with `env -u RUSTUP_TOOLCHAIN cargo test -p ace-engine --lib cli::tests 2>&1 | tail -3`.

Expected: `test result: ok.`

- [ ] **Step 5: Run the gate.** All four commands exit 0.

- [ ] **Step 6: Commit**

```bash
git add crates/ace-engine/src/cli.rs
git commit -m "fix(ace-engine): print play refusals and update play help (#164)" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 9: Docs: state what an infohash input guarantees (R6)

**Suggested implementer tier:** cheap (exact text given).

**Files:**
- Modify: `README.md`
- Modify: `docs/native-api.md`
- Modify: `docs/protocol/compat-matrix.md`

- [ ] **Step 1: `README.md`, `play` selectors**

1. Replace the bullet `- `acestream:?infohash=<40-hex>`;` with:

```markdown
- `acestream:?infohash=<40-hex>` - accepted, but `play` fails closed with an error: a one-shot
  process holds no verified transport descriptor for a bare infohash (see "Infohash inputs");
```

2. Replace the magnet bullet's text, `its infohash (v2 `urn:btmh:` magnets are rejected).`, with `its infohash (v2 `urn:btmh:` magnets are rejected). Like `infohash=`, it fails closed in `play`.`
3. Directly after the paragraph that ends `…playback works after a daemon restart without any server-side alias table.`, add:

```markdown
### Infohash inputs

A bare infohash identifies a swarm but carries no piece geometry or source key. outpace never
guesses them. An infohash input (the native `/streams/ace/<40-hex>` form, compat `infohash=` or
`magnet=`, CLI `acestream:?infohash=` or `magnet:`) plays only when the process already holds a
verified transport descriptor for it:

- the same daemon resolved the stream earlier by content id (`cid:<content-id>`,
  `acestream://`, `content_id=`/`id=`), or answered `analyze_content` / `get_media_files` for
  its content id; or
- the daemon originates that broadcast itself.

The descriptor's infohash commits to its piece length, chunk length and pubkey, so such a stream
uses the real geometry and verifies every piece's RSA signature. Otherwise the request fails
closed: the native routes return `422` with the reason, compat routes return an error envelope,
and `outpace play` exits with an error. The reason suggests `cid:<id>`, which also covers a content
id pasted without its prefix. Prefer content ids. Transport-URL streams reopen by their `turl-`
id, not by infohash: a caller-supplied transport never feeds the shared infohash index.
```

- [ ] **Step 2: `README.md`, compat paragraph**

Replace `resolution; only `infohash=` bypasses it as an explicit swarm key.` with `resolution; `infohash=` is an explicit swarm key and plays only under the rules in "Infohash inputs".`

- [ ] **Step 3: `docs/native-api.md`**

1. Replace the two lines `Examples use `http://127.0.0.1:6878` and the built-in `ace` network. An `<id>` may be a` / `40-character infohash or another provider identifier accepted by the configured network.` with:

```markdown
Examples use `http://127.0.0.1:6878` and the built-in `ace` network. An `<id>` is
`cid:<content-id>` (recommended), a transport-url id (`turl-…`), or a 40-character infohash.
An infohash plays only when this daemon already holds a verified transport descriptor for it
(see [Infohash inputs](#infohash-inputs)); otherwise the request fails closed.
```

2. In the `outpace play <input>` CLI row, replace `Accepts `acestream://`, `acestream:?`, `magnet:`, and HTTP(S) transport-file inputs.` with `Accepts `acestream://`, `acestream:?`, `magnet:`, and HTTP(S) transport-file inputs; infohash and magnet inputs fail closed (no verified descriptor in a one-shot process).`
3. In the `GET /streams/<network>/<id>` row, replace `Dotted ids, unknown networks, and invalid ids return `404`.` with `Dotted ids, unknown networks, and invalid ids return `404`. A bare infohash without a verified descriptor returns `422` with a plain-text reason.`
4. In the `GET /streams/<network>/<id>.m3u8` row, append ` The same `422` applies.` after `returns a sliding playlist.`
5. Replace `bare infohashes and descriptors without metadata return `null`, `null`, and `[]` respectively.` and the next two lines (`The descriptor title is authoritative …` / `invent a title for a bare infohash.`) with:

```markdown
descriptors without metadata return `null`, `null`, and `[]` respectively.
The descriptor title is authoritative for both `metadata.title` and `Icy-Name`; outpace does not
invent a title.
```

6. Add this section directly before `## Live startup buffering`:

```markdown
## Infohash inputs

A bare infohash carries no piece geometry or source key, and outpace never guesses them. An
infohash `<id>` plays only when this daemon already holds a verified transport descriptor for it:
the stream was resolved earlier in this process by `cid:<content-id>` (natively or through the
compatibility routes), or it is a broadcast this daemon originates. Such a stream
uses the descriptor's piece length and verifies each piece against the descriptor's pubkey.
Otherwise playback routes return `422 Unprocessable Content` with a plain-text reason that
suggests `cid:<id>`. Prefer `cid:<content-id>` ids in playlists.
```

- [ ] **Step 4: `docs/protocol/compat-matrix.md`**

1. In the `GET /ace/getstream` row, replace ``id` is a content-ID alias; `infohash` is the only explicit bare-swarm selector.` with ``id` is a content-ID alias; `infohash` is the only explicit bare-swarm selector, and it (like `magnet`) is accepted only when the daemon already holds a verified descriptor for that infohash. Otherwise the route returns an error envelope suggesting `cid:<id>`, and mints no lease.`
2. Directly after the paragraph that ends `…pending a broader official-engine error capture.`, add:

```markdown
An `infohash=` or `magnet=` selector is refused with an error envelope (HTTP 200,
`response: null`) unless the daemon holds a verified transport descriptor for that infohash.
That happens when it resolved the stream by content id earlier in this process,
or originates the broadcast. outpace does not guess live geometry for a bare infohash.
```

3. In the `analyze_content` row, append ` Resolving a `content_id` here also lets a later `infohash=` playback of the result succeed (a `url` does not); an offline `infohash`/`magnet` analysis does not make that infohash playable.` after `…content-id resolution.`
4. Replace the deferred bullet `- Reverse `get_content_id` (deriving a content id from an infohash/transport).` with `- Reverse `get_content_id` (deriving a content id from an infohash/transport). The official engine does this remotely, which is how it plays a cold infohash; adopting it would let outpace play cold infohash inputs.`

- [ ] **Step 5: Check the docs for leftovers**

Run: `rg -n -i 'infohash form works directly|bare-swarm selector\.|may be a$' README.md docs/native-api.md docs/protocol/compat-matrix.md`

Expected: no output.

- [ ] **Step 6: Run the gate.** All four commands exit 0. The hygiene check is the one that matters for docs.

- [ ] **Step 7: Commit**

```bash
git add README.md docs/native-api.md docs/protocol/compat-matrix.md
git commit -m "docs: state what an infohash input guarantees (#164)" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 10: Live verification (controller only; no commit)

**Owner:** the controller, not the implementer. This is the R7 "no network source" variant.

Before starting:
- run `pgrep -a outpace` and `docker ps`;
- never kill processes or containers you did not start;
- keep Cloudflare WARP off.

All real values stay in shell variables and the session scratch dir. Never put them in files under the repo.

- [ ] **Step 1: Build and set up**

```bash
cd /home/jamezrin/dev/outpace/.worktrees/fix/infohash-descriptor-164
env -u RUSTUP_TOOLCHAIN cargo build --release -p ace-engine --bin outpace
SCR=<session scratchpad>/164/live; mkdir -p "$SCR"
CID=<cid9, resolved from the registry by the operator; never written to a file>
BIN=$PWD/target/release/outpace
```

- [ ] **Step 2: Derive the infohash at runtime in a separate, throwaway process**

```bash
OUTPACE_DATA_DIR=$SCR/derive timeout 15 $BIN play "acestream://$CID" >/dev/null 2>$SCR/derive.log
IH=$(grep -oE 'infohash [0-9a-f]{40}' $SCR/derive.log | head -1 | cut -d' ' -f2)
```

- [ ] **Step 3: Cold daemon: everything fails closed**

Start a fresh daemon. Its index is empty because the derivation ran in another process.

```bash
export OUTPACE_BIND=127.0.0.1:16878 OUTPACE_RTMP_BIND=127.0.0.1:11935 \
  OUTPACE_PEER_LISTEN=0.0.0.0:18621 OUTPACE_DATA_DIR=$SCR/daemon OUTPACE_EXPERIMENTAL_ACE_COMPAT=1
$BIN serve 2>$SCR/daemon.log & DAEMON=$!
B=http://127.0.0.1:16878
curl -s -o $SCR/cold.txt -w '%{http_code}\n' $B/streams/ace/$IH.ts
grep -c "cid:$IH" $SCR/cold.txt
curl -s "$B/ace/getstream?format=json&infohash=$IH"
OUTPACE_DATA_DIR=$SCR/cli $BIN play "acestream:?infohash=$IH" >/dev/null; echo "exit=$?"
```

Expected:
- `422`, and the grep count is `1`;
- the compat JSON has `"response": null` and an error containing `cid:`;
- the CLI prints `outpace play: no verified transport descriptor …` and exits non-zero.

- [ ] **Step 4: Content-id path, then the same stream by infohash, run sequentially**

Run the two paths one after the other, never concurrently: concurrent sessions would share one PieceStore across two follow loops and confound the A/B.

```bash
curl -s -m 90 -o $SCR/a-cid.ts $B/streams/ace/cid:$CID.ts
curl -s -X DELETE $B/streams/ace/cid:$CID
curl -s -m 90 -o $SCR/b-ih.ts $B/streams/ace/$IH.ts
curl -s -X DELETE $B/streams/ace/$IH
grep -E "open ($IH|cid:)" $SCR/daemon.log
```

Expected log line: `[ace] open <ih>: using a descriptor verified in this process`.

- [ ] **Step 5: Compare**

For both files:
- run `ffprobe -v error -show_entries stream=codec_name -of compact`;
- run `ffmpeg -v error -i <f> -f null - 2>&1 | wc -l`;
- run a continuity-counter count (the spike's `cc.py` in `<scratchpad>/164/spike/` works, or use the #173 A/B harness).

Acceptance:
- `b-ih.ts` has about 0 sustained CC errors, no duplicate-video-packet bursts, and decode-error counts comparable to `a-cid.ts`;
- no `[mpegts] transport resync discarded boundary bytes` storm in `daemon.log` during the infohash session.

For byte equivalence with the official engine, run the #173 A/B harness with outpace given `/streams/ace/$IH` after Step 4.

- [ ] **Step 6: Compat warm path**

```bash
curl -s -m 30 -o $SCR/c-compat.ts "$B/ace/getstream?infohash=$IH"
```

Expected: an MPEG-TS body, not a JSON error, because the index still holds the descriptor.

- [ ] **Step 7: Tear down your own processes only**

Run `kill $DAEMON`. Record the results in the PR description: status codes, CC counts and the log lines. Leave out real ids.
