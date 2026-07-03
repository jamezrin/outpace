# Env-Var Configuration of Existing Tunables — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Expose the two hardcoded buffers that are not yet configurable — the playback
prefetch window (`PREFETCH_PIECES`) and the session fan-out channel depth
(`StreamManager.buffer`) — as environment variables, so operators can tune the playback
cushion and fan-out depth without recompiling.

**Architecture:** Copy the established `seed_store_bytes` pattern exactly — a `Config` field
with a `Default`, an env-parse arm in `config_from_env`, and either a builder (like
`AceProvider::with_seed_store_bytes`) or a constructor argument threaded through
`build_runtime`. No new subsystems, no behavior change at default values.

**Tech Stack:** Rust 2021, tokio, axum, serde. Crates `ace-engine`.

**Spec:** `docs/superpowers/specs/2026-07-03-cache-config-and-disk-backing-design.md`.

**Scope note:** Only `prefetch_pieces` and `session_buffer`. Peer-scheduling internals
(`MAX_PIECE_ADVANCE`, `MAX_PARALLEL_CONNECT`, `MAX_ACTIVE_UPSTREAMS`,
`BACKGROUND_DISCOVERY_PEER_TARGET` in `ace_provider.rs`) are intentionally excluded — they are
scheduler knobs, not the buffer/cache sizing this work targets. They can be exposed later via
the same pattern; Task A5 records this.

---

## File Structure

- Modify `crates/ace-engine/src/config.rs`: add `prefetch_pieces` + `session_buffer` fields and defaults.
- Modify `crates/ace-engine/src/runtime.rs`: parse `OUTPACE_PREFETCH_PIECES` + `OUTPACE_SESSION_BUFFER`; thread both into `build_runtime`.
- Modify `crates/ace-engine/src/ace_provider.rs`: `prefetch_pieces` field + `with_prefetch_pieces` builder; use it at the start-piece computation.
- Modify `crates/ace-engine/src/manager.rs`: `StreamManager::with_buffer` constructor.
- Modify `README.md`: document the two new env vars.

---

## Task A1: add `prefetch_pieces` + `session_buffer` to `Config`

**Files:**
- Modify: `crates/ace-engine/src/config.rs`

- [ ] **Step 1: Extend the default-config test to expect the new fields**

In `crates/ace-engine/src/config.rs`, add to `default_config_has_seeding_on_and_inbound_off`
(around config.rs:117):

```rust
        assert_eq!(c.prefetch_pieces, 8);
        assert_eq!(c.session_buffer, 256);
```

- [ ] **Step 2: Run the test, verify it fails to compile**

Run: `cargo test -p ace-engine config::tests::default_config_has_seeding_on_and_inbound_off`
Expected: FAIL — `no field prefetch_pieces on type Config`.

- [ ] **Step 3: Add the fields and defaults**

In the `Config` struct (after `seed_store_bytes`, config.rs:21):

```rust
    /// Pieces behind the live edge to start at, giving an immediate playback cushion.
    pub prefetch_pieces: u64,
    /// Depth of the per-session fan-out broadcast channel (messages buffered per client).
    /// Must be >= 1.
    pub session_buffer: usize,
```

In `impl Default for Config` (after `seed_store_bytes: 128 * 1024 * 1024,`, config.rs:51):

```rust
            prefetch_pieces: 8,
            session_buffer: 256,
```

- [ ] **Step 4: Run the test, verify it passes**

Run: `cargo test -p ace-engine config::tests::default_config_has_seeding_on_and_inbound_off`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/ace-engine/src/config.rs
git commit -m "ace-engine: add prefetch_pieces + session_buffer config fields"
```

---

## Task A2: parse the two env vars in `config_from_env`

**Files:**
- Modify: `crates/ace-engine/src/runtime.rs`

- [ ] **Step 1: Write a failing env-parse test**

Add a `#[cfg(test)]` module to `crates/ace-engine/src/runtime.rs` (env tests must be
serialized — env is process-global):

```rust
#[cfg(test)]
mod env_tests {
    use super::config_from_env;
    use std::sync::Mutex;

    // Serializes env mutation across tests in this module.
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn parses_prefetch_and_session_buffer() {
        let _g = ENV_LOCK.lock().unwrap();
        std::env::set_var("OUTPACE_PREFETCH_PIECES", "32");
        std::env::set_var("OUTPACE_SESSION_BUFFER", "512");
        let c = config_from_env().unwrap();
        assert_eq!(c.prefetch_pieces, 32);
        assert_eq!(c.session_buffer, 512);
        std::env::remove_var("OUTPACE_PREFETCH_PIECES");
        std::env::remove_var("OUTPACE_SESSION_BUFFER");
    }

    #[test]
    fn rejects_zero_session_buffer() {
        let _g = ENV_LOCK.lock().unwrap();
        std::env::set_var("OUTPACE_SESSION_BUFFER", "0");
        let err = config_from_env().err();
        std::env::remove_var("OUTPACE_SESSION_BUFFER");
        assert!(err.is_some(), "session_buffer=0 must be rejected");
    }
}
```

- [ ] **Step 2: Run the tests, verify they fail**

Run: `cargo test -p ace-engine env_tests`
Expected: FAIL — vars are ignored; `prefetch_pieces`/`session_buffer` stay at defaults, and
`0` is accepted.

- [ ] **Step 3: Add the parse arms**

In `config_from_env` (`runtime.rs`), after the `OUTPACE_SEED_STORE_BYTES` arm
(runtime.rs:40-42):

```rust
    if let Ok(v) = std::env::var("OUTPACE_PREFETCH_PIECES") {
        config.prefetch_pieces = v.parse()?;
    }
    if let Ok(v) = std::env::var("OUTPACE_SESSION_BUFFER") {
        let n: usize = v.parse()?;
        if n == 0 {
            return Err("OUTPACE_SESSION_BUFFER must be >= 1".into());
        }
        config.session_buffer = n;
    }
```

- [ ] **Step 4: Run the tests, verify they pass**

Run: `cargo test -p ace-engine env_tests`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/ace-engine/src/runtime.rs
git commit -m "ace-engine: parse OUTPACE_PREFETCH_PIECES + OUTPACE_SESSION_BUFFER"
```

---

## Task A3: thread `prefetch_pieces` into `AceProvider`

**Files:**
- Modify: `crates/ace-engine/src/ace_provider.rs`
- Modify: `crates/ace-engine/src/runtime.rs`

The `PREFETCH_PIECES` const (ace_provider.rs:43) is used at ace_provider.rs:883:
`let start = max_piece.saturating_sub(PREFETCH_PIECES).max(min_piece);`. The existing test
`ace_provider.rs:2440` asserts `start == 200 - PREFETCH_PIECES`. We make that value
configurable and carried down to the compute site via `SeedConfig`.

- [ ] **Step 1: Write a failing test for a custom prefetch value**

The start-piece computation must be reachable with an explicit prefetch argument. If it is
currently a free function, add a variant test next to the one at ace_provider.rs:2440. If it
reads a const, first extract the arithmetic into a small pure helper:

```rust
/// First piece to request given a peer window and a configured prefetch depth.
fn prefetch_start(min_piece: u64, max_piece: u64, prefetch: u64) -> u64 {
    max_piece.saturating_sub(prefetch).max(min_piece)
}
```

Then add the test:

```rust
    #[test]
    fn prefetch_start_honors_configured_depth() {
        // window 100..=200, prefetch 32 -> start 168 (not 200 - 8)
        assert_eq!(prefetch_start(100, 200, 32), 168);
        // clamps to min_piece when the window is shorter than prefetch
        assert_eq!(prefetch_start(195, 200, 32), 195);
    }
```

- [ ] **Step 2: Run the test, verify it fails**

Run: `cargo test -p ace-engine prefetch_start_honors_configured_depth`
Expected: FAIL — `prefetch_start` not defined.

- [ ] **Step 3: Add the field, builder, helper, and use it**

Add to the `AceProvider` struct (ace_provider.rs, near `seed_store_bytes`):

```rust
    prefetch_pieces: u64,
```

In `AceProvider::new`, initialize it: `prefetch_pieces: PREFETCH_PIECES,`. Add the builder
mirroring `with_seed_store_bytes` (ace_provider.rs:164):

```rust
    pub fn with_prefetch_pieces(mut self, pieces: u64) -> Self {
        self.prefetch_pieces = pieces;
        self
    }
```

Add `prefetch_pieces: u64` to `SeedConfig` (ace_provider.rs:~110) and populate it from
`self.prefetch_pieces` where `SeedConfig` is built (ace_provider.rs:~300). Introduce the
`prefetch_start` helper (Step 1) and replace the inline arithmetic at ace_provider.rs:883:

```rust
        let start = prefetch_start(min_piece, max_piece, seed.prefetch_pieces);
```

- [ ] **Step 4: Wire the config in `build_runtime`**

In `runtime.rs` (the `AceProvider::new(...)` builder chain at runtime.rs:81-84), add:

```rust
            .with_prefetch_pieces(config.prefetch_pieces)
```

- [ ] **Step 5: Run tests, verify they pass**

Run: `cargo test -p ace-engine` (includes the existing `start == 200 - PREFETCH_PIECES` test
and the new one).
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/ace-engine/src/ace_provider.rs crates/ace-engine/src/runtime.rs
git commit -m "ace-engine: make live prefetch depth configurable"
```

---

## Task A4: thread `session_buffer` into `StreamManager`

**Files:**
- Modify: `crates/ace-engine/src/manager.rs`
- Modify: `crates/ace-engine/src/runtime.rs`

`StreamManager::new(registry)` (manager.rs:26) hardcodes `buffer: 256` and is used by
`StreamSession::start(source, self.buffer)` at manager.rs:62.

- [ ] **Step 1: Write a failing test for a custom buffer**

Add to `manager.rs` tests (add a `#[cfg(test)]`-only accessor `pub(crate) fn buffer(&self)`
returning `self.buffer` if the field isn't otherwise observable):

```rust
    #[test]
    fn with_buffer_sets_the_fanout_depth() {
        let reg = ProviderRegistry::new();
        let mgr = StreamManager::with_buffer(reg, 4);
        assert_eq!(mgr.buffer(), 4);
    }
```

- [ ] **Step 2: Run the test, verify it fails**

Run: `cargo test -p ace-engine with_buffer_sets_the_fanout_depth`
Expected: FAIL — `with_buffer` not defined.

- [ ] **Step 3: Add `with_buffer`, make `new` delegate**

In `manager.rs`, replace the body of `new` and add `with_buffer`:

```rust
    pub fn new(registry: ProviderRegistry) -> Arc<StreamManager> {
        Self::with_buffer(registry, 256)
    }

    pub fn with_buffer(registry: ProviderRegistry, buffer: usize) -> Arc<StreamManager> {
        Arc::new(StreamManager {
            registry,
            sessions: Mutex::new(HashMap::new()),
            packagers: Mutex::new(HashMap::new()),
            start_lock: Mutex::new(()),
            buffer,
            grace: Duration::from_secs(30),
        })
    }

    #[cfg(test)]
    pub(crate) fn buffer(&self) -> usize {
        self.buffer
    }
```

- [ ] **Step 4: Wire the config in `build_runtime`**

In `runtime.rs:136`, replace:

```rust
    let manager = StreamManager::new(registry);
```

with:

```rust
    let manager = StreamManager::with_buffer(registry, config.session_buffer);
```

- [ ] **Step 5: Run tests, verify they pass**

Run: `cargo test -p ace-engine manager`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/ace-engine/src/manager.rs crates/ace-engine/src/runtime.rs
git commit -m "ace-engine: make session fan-out buffer configurable"
```

---

## Task A5: docs

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Document the new env vars**

Add to the config/env section of `README.md`:

```markdown
- `OUTPACE_PREFETCH_PIECES` (default `8`) — pieces behind the live edge to start at; a
  bigger value deepens the startup/stall cushion at the cost of extra initial latency.
- `OUTPACE_SESSION_BUFFER` (default `256`, must be >= 1) — per-session fan-out channel
  depth (messages buffered per client).

Not yet exposed (scheduler internals, same wiring pattern if needed later):
`MAX_PIECE_ADVANCE`, `MAX_PARALLEL_CONNECT`, `MAX_ACTIVE_UPSTREAMS`,
`BACKGROUND_DISCOVERY_PEER_TARGET` in `crates/ace-engine/src/ace_provider.rs`.
```

- [ ] **Step 2: Commit**

```bash
git add README.md
git commit -m "docs: document prefetch + session-buffer env vars"
```

---

## Verification

- `cargo test` — all green (live-network tests remain `#[ignore]`).
- `cargo clippy --all-targets -- -D warnings` — clean.
- Manual: `OUTPACE_PREFETCH_PIECES=32 OUTPACE_SESSION_BUFFER=512 cargo run -p ace-engine -- serve`
  boots and plays a stream; `OUTPACE_SESSION_BUFFER=0 ... serve` exits with the validation
  error; default (no vars) behavior is unchanged.
