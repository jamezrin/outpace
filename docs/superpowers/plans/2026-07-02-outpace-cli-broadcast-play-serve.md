# Outpace CLI Broadcast Play Serve Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add three primary commands, `outpace serve`, `outpace broadcast <name>`, and `outpace play <acestream-url>`, with broadcasts publishing a real playable `acestream://<content-id>` link.

**Architecture:** Extract the current daemon bootstrap into a reusable runtime builder, then put a small Clap CLI in front of it. A broadcast mints a transport, derives a content ID as the raw transport-file hash, registers the transport as BEP-9 metadata under that content ID, announces both content ID and stream infohash, and serves OBS ingest over the existing `/broadcast/{name}` route. Playback parses official Acestream URLs, opens the provider directly, and writes only MPEG-TS bytes to stdout while all diagnostics go to stderr.

**Tech Stack:** Rust 2021, Tokio, Axum, Clap 4, existing `ace-engine`, `ace-swarm`, `ace-wire`, BEP-9 `ut_metadata`, existing transport/infohash helpers.

---

## File Structure

- Modify `crates/ace-engine/Cargo.toml`: add `clap = { version = "4", features = ["derive"] }`.
- Create `crates/ace-engine/src/cli.rs`: command parsing, Acestream URL parsing, stdout/stderr command runners.
- Create `crates/ace-engine/src/runtime.rs`: shared config/env loading, provider setup, broadcast state setup, HTTP serving, inbound peer listener startup.
- Modify `crates/ace-engine/src/main.rs`: delegate to `cli::run`.
- Modify `crates/ace-engine/src/broadcast.rs`: add `content_id` to `Broadcast`; derive it from `transport_file_hash`; register metadata under content ID.
- Modify `crates/ace-engine/src/http.rs`: return `content_id` in `PUT /broadcast/{name}` JSON and self-announce the content ID as metadata.
- Modify `crates/ace-swarm/src/listen.rs`: let `SeedRegistry` hold both piece stores and metadata blobs.
- Modify `crates/ace-swarm/src/seed.rs`: advertise `metadata_size` and answer `ut_metadata` requests.
- Modify `crates/ace-wire/src/extended.rs`: add optional outgoing `metadata_size`.
- Modify `docs/RESUME.md` and add `docs/protocol/notes/51-cli-and-broadcast-content-id.md`: document the CLI and the content-id convention.

## Content ID Policy

For outpace-originated broadcasts, use:

```rust
content_id = ace_wire::infohash::transport_file_hash(&transport_bytes)
```

This is a real 20-byte Acestream metadata swarm key for our broadcast transport. It is not claimed to be Ace’s catalog-assigned public content-id derivation. The broadcaster makes the link honest by announcing this key and serving the minted `AceStreamTransport` bytes over BEP-9 `ut_metadata`; `outpace play acestream://<content-id>` then resolves metadata to the real stream infohash and geometry before playback.

## Task 1: Add CLI Parsing

**Files:**
- Modify: `crates/ace-engine/Cargo.toml`
- Create: `crates/ace-engine/src/cli.rs`
- Modify: `crates/ace-engine/src/lib.rs`

- [ ] **Step 1: Add the dependency**

Add to `crates/ace-engine/Cargo.toml`:

```toml
clap = { version = "4", features = ["derive"] }
```

- [ ] **Step 2: Write failing parser tests**

Create `crates/ace-engine/src/cli.rs` with the tests first:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_args_defaults_to_serve() {
        let cli = Cli::parse_from(["outpace"]);
        assert!(matches!(cli.command, Command::Serve(_)));
    }

    #[test]
    fn parses_serve() {
        let cli = Cli::parse_from(["outpace", "serve"]);
        assert!(matches!(cli.command, Command::Serve(_)));
    }

    #[test]
    fn parses_broadcast_name() {
        let cli = Cli::parse_from(["outpace", "broadcast", "sports"]);
        match cli.command {
            Command::Broadcast(args) => assert_eq!(args.name, "sports"),
            _ => panic!("expected broadcast"),
        }
    }

    #[test]
    fn parses_play_url() {
        let cli = Cli::parse_from([
            "outpace",
            "play",
            "acestream://0123456789abcdef0123456789abcdef01234567",
        ]);
        match cli.command {
            Command::Play(args) => {
                assert_eq!(
                    args.input,
                    "acestream://0123456789abcdef0123456789abcdef01234567"
                );
            }
            _ => panic!("expected play"),
        }
    }
}
```

- [ ] **Step 3: Run the parser test and verify it fails**

Run:

```bash
cargo test -p ace-engine cli::tests::no_args_defaults_to_serve
```

Expected: compile failure because `Cli`, `Command`, and the parser are not implemented.

- [ ] **Step 4: Implement minimal CLI types**

Add above the tests in `crates/ace-engine/src/cli.rs`:

```rust
use clap::{Args, Parser, Subcommand};

#[derive(Debug, Parser)]
#[command(name = "outpace")]
#[command(about = "Broadcast and play Acestream-compatible live streams")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

impl Cli {
    pub fn parse_cli() -> Self {
        let mut args = std::env::args_os().collect::<Vec<_>>();
        if args.len() == 1 {
            args.push("serve".into());
        }
        Cli::parse_from(args)
    }
}

#[derive(Debug, Subcommand)]
pub enum Command {
    Serve(ServeArgs),
    Broadcast(BroadcastArgs),
    Play(PlayArgs),
}

#[derive(Debug, Args)]
pub struct ServeArgs {}

#[derive(Debug, Args)]
pub struct BroadcastArgs {
    pub name: String,
}

#[derive(Debug, Args)]
pub struct PlayArgs {
    pub input: String,
    #[arg(long = "peer")]
    pub peers: Vec<std::net::SocketAddrV4>,
}
```

Expose it from `crates/ace-engine/src/lib.rs`:

```rust
pub mod cli;
```

- [ ] **Step 5: Run parser tests**

Run:

```bash
cargo test -p ace-engine cli::tests
```

Expected: all CLI parser tests pass.

- [ ] **Step 6: Commit**

```bash
git add crates/ace-engine/Cargo.toml crates/ace-engine/src/cli.rs crates/ace-engine/src/lib.rs Cargo.lock
git commit -m "ace-engine: add outpace cli parser"
```

## Task 2: Parse Official Acestream Inputs

**Files:**
- Modify: `crates/ace-engine/src/cli.rs`

- [ ] **Step 1: Write failing URL parser tests**

Add to the existing `cli.rs` test module:

```rust
#[test]
fn old_acestream_url_is_content_id() {
    let parsed = PlaybackTarget::parse(
        "acestream://0123456789abcdef0123456789abcdef01234567",
    )
    .unwrap();
    assert_eq!(
        parsed.provider_id,
        "cid:0123456789abcdef0123456789abcdef01234567"
    );
}

#[test]
fn query_content_id_is_content_id() {
    let parsed = PlaybackTarget::parse(
        "acestream:?content_id=0123456789abcdef0123456789abcdef01234567",
    )
    .unwrap();
    assert_eq!(
        parsed.provider_id,
        "cid:0123456789abcdef0123456789abcdef01234567"
    );
}

#[test]
fn query_infohash_is_direct_infohash() {
    let parsed = PlaybackTarget::parse(
        "acestream:?infohash=89abcdef0123456789abcdef0123456789abcdef",
    )
    .unwrap();
    assert_eq!(
        parsed.provider_id,
        "89abcdef0123456789abcdef0123456789abcdef"
    );
}

#[test]
fn invalid_playback_input_is_rejected() {
    assert!(PlaybackTarget::parse("acestream://nothex").is_err());
}
```

- [ ] **Step 2: Run parser tests and verify failure**

Run:

```bash
cargo test -p ace-engine cli::tests::old_acestream_url_is_content_id
```

Expected: compile failure because `PlaybackTarget` is not implemented.

- [ ] **Step 3: Implement `PlaybackTarget`**

Add to `crates/ace-engine/src/cli.rs`:

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlaybackTarget {
    pub provider_id: String,
}

impl PlaybackTarget {
    pub fn parse(input: &str) -> Result<Self, String> {
        let input = input.trim();
        if let Some(rest) = input.strip_prefix("acestream://") {
            let id = rest.split(['?', '#']).next().unwrap_or("");
            return content_id_target(id);
        }
        if let Some(query) = input.strip_prefix("acestream:?") {
            let params = parse_query(query);
            if let Some(id) = params.get("content_id") {
                return content_id_target(id);
            }
            if let Some(id) = params.get("infohash") {
                return infohash_target(id);
            }
            return Err("acestream URL must contain content_id or infohash".into());
        }
        Err("expected an acestream:// or acestream:? URL".into())
    }
}

fn content_id_target(id: &str) -> Result<PlaybackTarget, String> {
    let id = normalize_hex40(id)?;
    Ok(PlaybackTarget {
        provider_id: format!("cid:{id}"),
    })
}

fn infohash_target(id: &str) -> Result<PlaybackTarget, String> {
    Ok(PlaybackTarget {
        provider_id: normalize_hex40(id)?,
    })
}

fn normalize_hex40(id: &str) -> Result<String, String> {
    if id.len() == 40 && id.bytes().all(|b| b.is_ascii_hexdigit()) {
        Ok(id.to_ascii_lowercase())
    } else {
        Err("identifier must be 40 hex characters".into())
    }
}

fn parse_query(query: &str) -> std::collections::BTreeMap<String, String> {
    query
        .split('&')
        .filter_map(|pair| {
            let mut parts = pair.splitn(2, '=');
            let key = parts.next()?.trim();
            let value = parts.next().unwrap_or("").trim();
            if key.is_empty() {
                None
            } else {
                Some((key.to_string(), value.to_string()))
            }
        })
        .collect()
}
```

- [ ] **Step 4: Run URL parser tests**

Run:

```bash
cargo test -p ace-engine cli::tests
```

Expected: all CLI tests pass.

- [ ] **Step 5: Commit**

```bash
git add crates/ace-engine/src/cli.rs
git commit -m "ace-engine: parse acestream playback URLs"
```

## Task 3: Add Metadata Size to Outgoing Extended Handshakes

**Files:**
- Modify: `crates/ace-wire/src/extended.rs`

- [ ] **Step 1: Write failing wire test**

Add to the test module in `crates/ace-wire/src/extended.rs`:

```rust
#[test]
fn outgoing_handshake_can_advertise_metadata_size() {
    let hs = OutgoingExtendedHandshake {
        ace_metadata_version: 1,
        ut_metadata_id: 2,
        mi: None,
        node: NodeFields::default(),
        peer_ip: None,
        metadata_size: Some(420),
    };
    let payload = hs.encode_payload();
    let parsed = ExtendedHandshake::parse(&payload).unwrap();
    assert_eq!(parsed.metadata_size(), Some(420));
    assert_eq!(parsed.ut_metadata_id(), Some(2));
}
```

- [ ] **Step 2: Run the test and verify failure**

Run:

```bash
cargo test -p ace-wire outgoing_handshake_can_advertise_metadata_size
```

Expected: compile failure because `metadata_size` is not a field yet.

- [ ] **Step 3: Add the field and encoder behavior**

Modify `OutgoingExtendedHandshake`:

```rust
#[derive(Debug, Clone)]
pub struct OutgoingExtendedHandshake {
    pub ace_metadata_version: i64,
    pub ut_metadata_id: i64,
    pub mi: Option<LivePosition>,
    pub node: NodeFields,
    pub peer_ip: Option<[u8; 4]>,
    pub metadata_size: Option<i64>,
}
```

Modify `base_fields`:

```rust
if let Some(size) = self.metadata_size {
    root.insert(b"metadata_size".to_vec(), Bencode::Int(size));
}
```

Update all existing `OutgoingExtendedHandshake` literals by adding:

```rust
metadata_size: None,
```

- [ ] **Step 4: Run ace-wire tests**

Run:

```bash
cargo test -p ace-wire extended
```

Expected: extended-handshake tests pass.

- [ ] **Step 5: Commit**

```bash
git add crates/ace-wire/src/extended.rs
git commit -m "ace-wire: advertise ut_metadata size"
```

## Task 4: Serve Broadcast Metadata Over BEP-9

**Files:**
- Modify: `crates/ace-swarm/src/listen.rs`
- Modify: `crates/ace-swarm/src/seed.rs`

- [ ] **Step 1: Write failing registry test**

Add to `crates/ace-swarm/src/listen.rs` tests:

```rust
#[test]
fn registry_serves_metadata_only_keys() {
    let reg = SeedRegistry::new();
    let key = [9u8; 20];
    reg.register_metadata(key, vec![1, 2, 3, 4]);
    assert!(reg.serves(&key));
    assert_eq!(&*reg.metadata(&key).unwrap(), &[1, 2, 3, 4]);
    assert!(reg.get(&key).is_none());
}
```

- [ ] **Step 2: Run the registry test and verify failure**

Run:

```bash
cargo test -p ace-swarm registry_serves_metadata_only_keys
```

Expected: compile failure because metadata registry methods do not exist.

- [ ] **Step 3: Extend `SeedRegistry`**

Replace the internal map value in `listen.rs` with an entry that can hold a store, metadata, or both:

```rust
type SharedStore = Arc<Mutex<PieceStore>>;
type SharedMetadata = Arc<Vec<u8>>;

#[derive(Clone, Default)]
struct SeedEntry {
    store: Option<SharedStore>,
    metadata: Option<SharedMetadata>,
}

#[derive(Clone, Default)]
pub struct SeedRegistry {
    stores: Arc<StdMutex<HashMap<[u8; 20], SeedEntry>>>,
}
```

Update methods:

```rust
pub fn register(&self, infohash: [u8; 20], store: SharedStore) {
    self.stores
        .lock()
        .unwrap()
        .entry(infohash)
        .or_default()
        .store = Some(store);
}

pub fn register_metadata(&self, key: [u8; 20], metadata: Vec<u8>) {
    self.stores
        .lock()
        .unwrap()
        .entry(key)
        .or_default()
        .metadata = Some(Arc::new(metadata));
}

pub fn metadata(&self, key: &[u8; 20]) -> Option<Arc<Vec<u8>>> {
    self.stores.lock().unwrap().get(key)?.metadata.clone()
}

pub fn serves(&self, infohash: &[u8; 20]) -> bool {
    self.stores.lock().unwrap().contains_key(infohash)
}
```

Keep `get_or_create` returning the existing store if present, otherwise creating only the store field.

- [ ] **Step 4: Run registry tests**

Run:

```bash
cargo test -p ace-swarm listen::tests
```

Expected: listener registry tests pass.

- [ ] **Step 5: Write failing metadata serve test**

Add a test in `crates/ace-swarm/src/seed.rs` that connects a mock client, reads the extended handshake, and requests metadata piece 0:

```rust
#[tokio::test]
async fn serves_ut_metadata_piece_when_metadata_is_registered() {
    use ace_wire::ut_metadata::{MetadataMessage, request_piece};
    use tokio::io::duplex;

    let metadata = Arc::new(b"AceStreamTransport-metadata".to_vec());
    let (client, server) = duplex(4096);
    let identity = Arc::new(ace_wire::identity::Identity::from_seed([7u8; 32]));
    let mut server_session = PeerSession::new(server);
    let mut client_session = PeerSession::new(client);

    let server_task = tokio::spawn(async move {
        SeederSession::serve(
            &mut server_session,
            None,
            Some(metadata),
            [0u8; 8],
            &identity,
            [127, 0, 0, 1],
        )
        .await
    });

    let msg = client_session.read_message().await.unwrap();
    let PeerMessage::Extended { ext_id: 0, payload } = msg else {
        panic!("expected extended handshake");
    };
    let parsed = ExtendedHandshake::parse(&payload).unwrap();
    assert_eq!(parsed.metadata_size(), Some(27));

    client_session
        .send(&PeerMessage::Extended {
            ext_id: 2,
            payload: request_piece(0),
        })
        .await
        .unwrap();
    let msg = client_session.read_message().await.unwrap();
    let PeerMessage::Extended { ext_id: 2, payload } = msg else {
        panic!("expected ut_metadata data");
    };
    match MetadataMessage::parse(&payload).unwrap() {
        MetadataMessage::Data { piece, data, .. } => {
            assert_eq!(piece, 0);
            assert_eq!(data, b"AceStreamTransport-metadata");
        }
        other => panic!("expected data, got {other:?}"),
    }

    drop(client_session);
    let _ = server_task.await;
}
```

- [ ] **Step 6: Run the metadata serve test and verify failure**

Run:

```bash
cargo test -p ace-swarm serves_ut_metadata_piece_when_metadata_is_registered
```

Expected: compile failure because `SeederSession::serve` does not accept metadata yet.

- [ ] **Step 7: Implement metadata serving**

Change `SeederSession::serve` signature:

```rust
pub async fn serve<S: AsyncRead + AsyncWrite + Unpin>(
    session: &mut PeerSession<S>,
    store: Option<Arc<Mutex<PieceStore>>>,
    metadata: Option<Arc<Vec<u8>>>,
    piece_header: [u8; 8],
    identity: &Identity,
    peer_ip: [u8; 4],
) -> Result<()>
```

Build the handshake with:

```rust
metadata_size: metadata.as_ref().map(|m| m.len() as i64),
```

When `store` is `None`, advertise no live pieces:

```rust
let (min, max) = if let Some(store) = &store {
    let guard = store.lock().await;
    complete_piece_window(&guard).unwrap_or((0, 0))
} else {
    (0, 0)
};
```

Handle metadata requests:

```rust
PeerMessage::Extended { ext_id, payload } if ext_id == 2 => {
    if let Some(metadata) = &metadata {
        if let Some(ace_wire::ut_metadata::MetadataMessage::Request { piece }) =
            ace_wire::ut_metadata::MetadataMessage::parse(&payload)
        {
            let piece = piece.max(0) as usize;
            let start = piece * ace_wire::ut_metadata::METADATA_BLOCK_LEN;
            if start < metadata.len() {
                let end = (start + ace_wire::ut_metadata::METADATA_BLOCK_LEN).min(metadata.len());
                let payload = ace_wire::ut_metadata::data_piece(
                    piece as i64,
                    metadata.len() as i64,
                    &metadata[start..end],
                );
                session
                    .send(&PeerMessage::Extended { ext_id: 2, payload })
                    .await?;
            }
        }
    }
}
```

Guard all piece-store operations with `if let Some(store) = &store`.

Update `listen.rs` inbound handler to pass both:

```rust
let store = registry.get(&peer_hs.infohash);
let metadata = registry.metadata(&peer_hs.infohash);
if store.is_none() && metadata.is_none() {
    return Err(ace_peer::PeerError::InfohashMismatch);
}
SeederSession::serve(&mut session, store, metadata, piece_header, identity, peer_ip).await
```

Update any other `SeederSession::serve` call sites by wrapping the store in `Some(...)` and passing `None` for metadata.

- [ ] **Step 8: Run ace-swarm tests**

Run:

```bash
cargo test -p ace-swarm
```

Expected: all ace-swarm tests pass.

- [ ] **Step 9: Commit**

```bash
git add crates/ace-swarm/src/listen.rs crates/ace-swarm/src/seed.rs
git commit -m "ace-swarm: serve broadcast metadata"
```

## Task 5: Mint Broadcast Content IDs

**Files:**
- Modify: `crates/ace-engine/src/broadcast.rs`
- Modify: `crates/ace-engine/src/http.rs`

- [ ] **Step 1: Write failing broadcast content-id test**

Add to `crates/ace-engine/src/broadcast.rs` tests:

```rust
#[tokio::test]
async fn minted_broadcast_has_content_id_and_registers_metadata() {
    let (reg, seed) = registry();
    let (bc, fresh) = reg
        .start_or_resume(
            "chan",
            "Channel",
            &["udp://tracker.example:2710/announce".to_string()],
            &seed,
            1024 * 1024,
        )
        .await;
    assert!(fresh);
    assert_eq!(bc.content_id, ace_wire::infohash::transport_file_hash(&bc.transport_bytes));
    assert_eq!(
        seed.metadata(&bc.content_id).as_deref().map(Vec::as_slice),
        Some(bc.transport_bytes.as_slice())
    );
    assert!(seed.serves(&bc.infohash));
    assert!(seed.serves(&bc.content_id));
}
```

- [ ] **Step 2: Run test and verify failure**

Run:

```bash
cargo test -p ace-engine minted_broadcast_has_content_id_and_registers_metadata
```

Expected: compile failure because `Broadcast.content_id` is missing and metadata is not registered.

- [ ] **Step 3: Implement content ID minting**

Modify `Broadcast`:

```rust
pub struct Broadcast {
    pub infohash: [u8; 20],
    pub content_id: [u8; 20],
    pub transport_bytes: Arc<Vec<u8>>,
    pub store: Arc<Mutex<PieceStore>>,
    pub auth: Arc<LiveSourceAuth>,
}
```

Inside `BroadcastRegistry::start_or_resume`, after encoding transport bytes:

```rust
let transport_bytes = encode_transport(&descriptor);
let content_id = ace_wire::infohash::transport_file_hash(&transport_bytes);
seed_registry.register_metadata(content_id, transport_bytes.clone());
```

Then construct:

```rust
let broadcast = Broadcast {
    infohash,
    content_id,
    transport_bytes: Arc::new(transport_bytes),
    store,
    auth: Arc::new(auth),
};
```

- [ ] **Step 4: Return content ID in broadcast HTTP response**

In `broadcast_ingest`, compute:

```rust
let content_id_hex: String = bc.content_id.iter().map(|b| format!("{b:02x}")).collect();
```

Include it in the JSON response:

```rust
Json(json!({
    "name": name,
    "content_id": content_id_hex,
    "infohash": infohash_hex,
}))
```

Update existing HTTP tests that assert response shape.

- [ ] **Step 5: Announce content ID when broadcasts are minted**

In `broadcast_ingest`, when `freshly_minted` and inbound is enabled, spawn a second announce loop:

```rust
let content_key = bc.content_id;
tokio::spawn(crate::ace_provider::announce_infohash_periodically(
    trackers.clone(),
    content_key,
    port,
));
```

Keep the existing infohash announce loop.

- [ ] **Step 6: Run broadcast tests**

Run:

```bash
cargo test -p ace-engine broadcast
```

Expected: broadcast tests pass.

- [ ] **Step 7: Commit**

```bash
git add crates/ace-engine/src/broadcast.rs crates/ace-engine/src/http.rs
git commit -m "ace-engine: mint broadcast content ids"
```

## Task 6: Extract Reusable Runtime

**Files:**
- Create: `crates/ace-engine/src/runtime.rs`
- Modify: `crates/ace-engine/src/lib.rs`
- Modify: `crates/ace-engine/src/main.rs`

- [ ] **Step 1: Create runtime module with the current daemon behavior**

Move the current setup from `main.rs` into `crates/ace-engine/src/runtime.rs` with these public functions:

```rust
pub struct EngineRuntime {
    pub config: Config,
    pub networks: Vec<String>,
    pub manager: Arc<StreamManager>,
    pub seed_registry: ace_swarm::listen::SeedRegistry,
    pub broadcasts: BroadcastState,
}

pub fn config_from_env() -> Result<Config, Box<dyn std::error::Error>>;

pub async fn build_runtime(
    mut config: Config,
    bootstrap_peers: Vec<std::net::SocketAddrV4>,
) -> Result<EngineRuntime, Box<dyn std::error::Error>>;

pub async fn serve_http(runtime: EngineRuntime) -> Result<(), Box<dyn std::error::Error>>;
```

Keep the existing env vars and defaults unchanged. `serve_http` must start the inbound listener when `config.enable_inbound` is true, bind the native HTTP API, and print the same daemon status lines to stderr.

- [ ] **Step 2: Wire `main.rs` to call CLI later without behavior changes**

Temporarily keep `main.rs` equivalent to current daemon behavior:

```rust
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = ace_engine::runtime::config_from_env()?;
    let peers = ace_engine::runtime::bootstrap_peers_from_env();
    let runtime = ace_engine::runtime::build_runtime(config, peers).await?;
    ace_engine::runtime::serve_http(runtime).await
}
```

Expose from `lib.rs`:

```rust
pub mod runtime;
```

- [ ] **Step 3: Run daemon tests**

Run:

```bash
cargo test -p ace-engine
```

Expected: all ace-engine tests pass.

- [ ] **Step 4: Smoke current serve behavior**

Run:

```bash
tmpdir=$(mktemp -d)
OUTPACE_BIND=127.0.0.1:6978 OUTPACE_DATA_DIR="$tmpdir" \
  timeout 5 cargo run -p ace-engine --bin outpace
```

Expected: command times out after printing a listener line for `http://127.0.0.1:6978`; no panic before timeout.

- [ ] **Step 5: Commit**

```bash
git add crates/ace-engine/src/runtime.rs crates/ace-engine/src/main.rs crates/ace-engine/src/lib.rs
git commit -m "ace-engine: extract runtime startup"
```

## Task 7: Implement `outpace serve`

**Files:**
- Modify: `crates/ace-engine/src/cli.rs`
- Modify: `crates/ace-engine/src/main.rs`

- [ ] **Step 1: Add command runner**

Add to `cli.rs`:

```rust
pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    match Cli::parse_cli().command {
        Command::Serve(_) => run_serve().await,
        Command::Broadcast(args) => run_broadcast(args).await,
        Command::Play(args) => run_play(args).await,
    }
}

async fn run_serve() -> Result<(), Box<dyn std::error::Error>> {
    let config = crate::runtime::config_from_env()?;
    let peers = crate::runtime::bootstrap_peers_from_env();
    let runtime = crate::runtime::build_runtime(config, peers).await?;
    crate::runtime::serve_http(runtime).await
}

async fn run_broadcast(_args: BroadcastArgs) -> Result<(), Box<dyn std::error::Error>> {
    Err("broadcast command is not wired yet".into())
}

async fn run_play(_args: PlayArgs) -> Result<(), Box<dyn std::error::Error>> {
    Err("play command is not wired yet".into())
}
```

Change `main.rs`:

```rust
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    ace_engine::cli::run().await
}
```

- [ ] **Step 2: Run tests**

Run:

```bash
cargo test -p ace-engine cli::tests
cargo test -p ace-engine
```

Expected: all tests pass.

- [ ] **Step 3: Smoke no-arg alias and explicit serve**

Run both:

```bash
tmpdir=$(mktemp -d)
OUTPACE_BIND=127.0.0.1:6978 OUTPACE_DATA_DIR="$tmpdir" \
  timeout 5 cargo run -p ace-engine --bin outpace

tmpdir=$(mktemp -d)
OUTPACE_BIND=127.0.0.1:6979 OUTPACE_DATA_DIR="$tmpdir" \
  timeout 5 cargo run -p ace-engine --bin outpace -- serve
```

Expected: both commands start the native API and time out without panic.

- [ ] **Step 4: Commit**

```bash
git add crates/ace-engine/src/cli.rs crates/ace-engine/src/main.rs
git commit -m "ace-engine: wire outpace serve"
```

## Task 8: Implement `outpace broadcast <name>`

**Files:**
- Modify: `crates/ace-engine/src/cli.rs`
- Modify: `crates/ace-engine/src/runtime.rs`

- [ ] **Step 1: Add broadcast command options**

Extend `BroadcastArgs`:

```rust
#[derive(Debug, Args)]
pub struct BroadcastArgs {
    pub name: String,
    #[arg(long = "public-host")]
    pub public_host: Option<String>,
}
```

The `public_host` value is only for printed operator URLs. Peer discovery uses tracker/DHT announces on the peer listener port.

- [ ] **Step 2: Implement pre-mint helper**

Add to `runtime.rs`:

```rust
use crate::broadcast::Broadcast;

pub async fn mint_broadcast(runtime: &EngineRuntime, name: &str) -> Broadcast {
    let (bc, _) = runtime
        .broadcasts
        .registry
        .start_or_resume(
            name,
            name,
            &runtime.broadcasts.trackers,
            &runtime.broadcasts.seed_registry,
            runtime.broadcasts.store_bytes,
        )
        .await;
    bc
}
```

- [ ] **Step 3: Implement broadcast command**

Add to `cli.rs`:

```rust
async fn run_broadcast(args: BroadcastArgs) -> Result<(), Box<dyn std::error::Error>> {
    let mut config = crate::runtime::config_from_env()?;
    config.enable_inbound = true;
    let peers = crate::runtime::bootstrap_peers_from_env();
    let runtime = crate::runtime::build_runtime(config, peers).await?;
    let bc = crate::runtime::mint_broadcast(&runtime, &args.name).await;

    let bind = runtime.config.bind;
    let host = args
        .public_host
        .unwrap_or_else(|| bind.ip().to_string());
    let content_id = hex20(&bc.content_id);
    let infohash = hex20(&bc.infohash);

    eprintln!("outpace broadcast: {}", args.name);
    eprintln!("OBS ingest URL: http://{}:{}/broadcast/{}", bind.ip(), bind.port(), args.name);
    eprintln!("Content ID: {content_id}");
    eprintln!("Ace link: acestream://{content_id}");
    eprintln!("Infohash: {infohash}");
    eprintln!("Transport URL: http://{host}:{}/broadcast/{}", bind.port(), args.name);
    eprintln!("Peer listen: {}", runtime.config.peer_listen);

    crate::runtime::serve_http(runtime).await
}

fn hex20(bytes: &[u8; 20]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
```

- [ ] **Step 4: Ensure pre-minted broadcasts self-announce**

If pre-minting bypasses the self-announce currently inside `broadcast_ingest`, move announce spawning into a runtime helper:

```rust
pub fn announce_broadcast(runtime: &EngineRuntime, bc: &Broadcast) {
    if let Some(port) = runtime.broadcasts.inbound_peer_port {
        let trackers = runtime.broadcasts.trackers.clone();
        tokio::spawn(crate::ace_provider::announce_infohash_periodically(
            trackers.clone(),
            bc.infohash,
            port,
        ));
        tokio::spawn(crate::ace_provider::announce_infohash_periodically(
            trackers,
            bc.content_id,
            port,
        ));
    }
}
```

Call this from `run_broadcast` after minting. Keep the HTTP ingest path doing the same for broadcasts minted by API-only users.

- [ ] **Step 5: Run tests**

Run:

```bash
cargo test -p ace-engine cli::tests
cargo test -p ace-engine broadcast
```

Expected: tests pass.

- [ ] **Step 6: Smoke command startup**

Run:

```bash
tmpdir=$(mktemp -d)
OUTPACE_BIND=127.0.0.1:6980 OUTPACE_PEER_LISTEN=127.0.0.1:6981 OUTPACE_DATA_DIR="$tmpdir" \
  timeout 5 cargo run -p ace-engine --bin outpace -- broadcast testchan 2>"$tmpdir/log"
cat "$tmpdir/log"
```

Expected stderr contains `OBS ingest URL`, `Content ID`, `Ace link: acestream://`, `Infohash`, and `Peer listen`.

- [ ] **Step 7: Commit**

```bash
git add crates/ace-engine/src/cli.rs crates/ace-engine/src/runtime.rs
git commit -m "ace-engine: add broadcast command"
```

## Task 9: Implement `outpace play <acestream-url>`

**Files:**
- Modify: `crates/ace-engine/src/cli.rs`

- [ ] **Step 1: Implement stdout playback loop**

Add to `cli.rs`:

```rust
async fn run_play(args: PlayArgs) -> Result<(), Box<dyn std::error::Error>> {
    use crate::provider::{StreamProvider, TsSource};
    use tokio::io::AsyncWriteExt;

    let target = PlaybackTarget::parse(&args.input)?;
    let config = crate::runtime::config_from_env()?;
    let mut peers = crate::runtime::bootstrap_peers_from_env();
    peers.extend(args.peers);

    let identity = std::sync::Arc::new(crate::config::load_or_create_identity(&config.data_dir)?);
    let seed_registry = ace_swarm::listen::SeedRegistry::new();
    let provider = crate::ace_provider::AceProvider::new(identity, config.bind.port())
        .with_bootstrap_peers(peers)
        .with_seed_registry(seed_registry)
        .with_seed_store_bytes(config.seed_store_bytes)
        .with_seeding_enabled(config.enable_seeding);

    eprintln!("outpace play: {}", args.input);
    eprintln!("outpace play: provider id {}", target.provider_id);

    let mut source = provider.open(&target.provider_id).await?;
    let mut stdout = tokio::io::stdout();
    while let Some(chunk) = source.next().await {
        stdout.write_all(&chunk).await?;
        stdout.flush().await?;
    }
    Ok(())
}
```

Keep all diagnostics on stderr. Do not print anything to stdout except MPEG-TS bytes.

- [ ] **Step 2: Run tests**

Run:

```bash
cargo test -p ace-engine cli::tests
cargo test -p ace-engine
```

Expected: tests pass.

- [ ] **Step 3: Commit**

```bash
git add crates/ace-engine/src/cli.rs
git commit -m "ace-engine: add stdout play command"
```

## Task 10: End-to-End Broadcast Content-ID Smoke

**Files:**
- No production file changes expected.
- Add note: `docs/protocol/notes/51-cli-and-broadcast-content-id.md`
- Modify: `docs/RESUME.md`

- [ ] **Step 1: Build binary**

Run:

```bash
cargo build -p ace-engine --bin outpace
```

Expected: build succeeds.

- [ ] **Step 2: Start broadcaster**

Run in terminal A:

```bash
tmpdir=$(mktemp -d)
OUTPACE_BIND=127.0.0.1:6990 \
OUTPACE_PEER_LISTEN=127.0.0.1:6991 \
OUTPACE_DATA_DIR="$tmpdir/broadcast-data" \
cargo run -p ace-engine --bin outpace -- broadcast smoke
```

Expected stderr prints `Ace link: acestream://<40hex>`.

- [ ] **Step 3: Feed OBS-shaped MPEG-TS over HTTP**

Run in terminal B:

```bash
curl -X PUT --data-binary @tests/vectors/media/h264-keyframes.ts \
  http://127.0.0.1:6990/broadcast/smoke
```

Expected JSON includes `content_id` and `infohash`.

- [ ] **Step 4: Play through content ID**

Run in terminal C, replacing the ID with the printed value:

```bash
OUTPACE_BIND=127.0.0.1:6992 \
OUTPACE_ACE_PEERS=127.0.0.1:6991 \
timeout 30 cargo run -p ace-engine --bin outpace -- \
  play acestream://<content-id> > /tmp/outpace-smoke.ts
```

Expected: `/tmp/outpace-smoke.ts` exists and has a size greater than 0. Stderr shows provider resolution and peer discovery; stdout file contains only media bytes.

- [ ] **Step 5: Verify MPEG-TS alignment**

Run:

```bash
python3 - <<'PY'
from pathlib import Path
p = Path("/tmp/outpace-smoke.ts")
b = p.read_bytes()
assert len(b) > 0, "empty capture"
assert len(b) % 188 == 0, f"not 188-aligned: {len(b)}"
assert b[0] == 0x47, "first byte is not MPEG-TS sync"
print(len(b))
PY
```

Expected: prints byte count and exits 0.

- [ ] **Step 6: Document the result**

Create `docs/protocol/notes/51-cli-and-broadcast-content-id.md`:

```markdown
# 51 - CLI broadcast/play and outpace broadcast content IDs

Date: 2026-07-02

`outpace` now has three primary commands:

- `outpace serve`
- `outpace broadcast <name>`
- `outpace play <acestream-url>`

For outpace-originated broadcasts, `content_id` is the raw transport-file hash.
The broadcaster announces that key and serves the minted `AceStreamTransport` over
BEP-9 `ut_metadata`, so `outpace play acestream://<content_id>` resolves metadata
before joining the actual broadcast infohash.

Smoke result:

- broadcaster bind:
- peer listen:
- content id:
- infohash:
- captured bytes:
- MPEG-TS alignment:
```

Fill the six smoke-result values from the commands above.

- [ ] **Step 7: Update RESUME**

In `docs/RESUME.md`, add a current-backlog/result bullet saying the three-command CLI is implemented and that `acestream://<content_id>` for outpace broadcasts is backed by BEP-9 metadata serving, not a guessed official catalog algorithm.

- [ ] **Step 8: Commit**

```bash
git add docs/protocol/notes/51-cli-and-broadcast-content-id.md docs/RESUME.md
git commit -m "docs: record outpace cli content id smoke"
```

## Task 11: Final Verification

**Files:**
- No edits expected.

- [ ] **Step 1: Run package tests**

Run:

```bash
cargo test -p ace-wire
cargo test -p ace-swarm
cargo test -p ace-engine
```

Expected: all tests pass.

- [ ] **Step 2: Run formatting checks**

Run:

```bash
cargo fmt --check
git diff --check
```

Expected: both exit 0.

- [ ] **Step 3: Run CLI help checks**

Run:

```bash
cargo run -p ace-engine --bin outpace -- --help
cargo run -p ace-engine --bin outpace -- serve --help
cargo run -p ace-engine --bin outpace -- broadcast --help
cargo run -p ace-engine --bin outpace -- play --help
```

Expected: each command prints help and exits 0.

- [ ] **Step 4: Inspect commit history**

Run:

```bash
git log --oneline -8
git status --short
```

Expected: recent commits match the task commits and worktree is clean.
