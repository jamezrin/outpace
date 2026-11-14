use std::net::SocketAddrV4;

use ace_swarm::resolve::infohash_hex;
use clap::{Args, Parser, Subcommand};

#[derive(Debug, Parser)]
#[command(
    name = "outpace",
    version,
    about = "Stream and broadcast public Acestream content",
    long_about = "Stream and broadcast public Acestream content.\n\nThe native CLI and /streams API are the primary interface. The supported Ace HTTP subset is disabled by default; enable it with OUTPACE_ACE_COMPAT=1."
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

impl Cli {
    pub fn parse_cli() -> Self {
        Self::parse_cli_from(std::env::args_os())
    }

    fn parse_cli_from<I, T>(args: I) -> Self
    where
        I: IntoIterator<Item = T>,
        T: Into<std::ffi::OsString>,
    {
        let mut args = args.into_iter().map(Into::into).collect::<Vec<_>>();
        if args.len() == 1 {
            args.push("serve".into());
        }

        Self::parse_from(args)
    }
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Run the HTTP API, RTMP ingest listener, and enabled peer services.
    Serve(ServeArgs),
    /// Create or resume a named broadcast and run its ingest server.
    Broadcast(BroadcastArgs),
    /// Write a live MPEG-TS stream (or verified VOD with --vod) to stdout.
    Play(PlayArgs),
    /// Check that a running daemon answers /healthz; exit 0 if healthy, 1 otherwise.
    Healthcheck(HealthcheckArgs),
    /// Install, remove, or run outpace as a Windows service.
    #[cfg(windows)]
    #[command(subcommand)]
    Service(ServiceCommand),
}

#[derive(Debug, Args)]
pub struct HealthcheckArgs {
    /// Address of the HTTP API to probe. Defaults to OUTPACE_BIND (a wildcard bind is probed on
    /// loopback), else 127.0.0.1:6878.
    #[arg(long)]
    pub addr: Option<std::net::SocketAddr>,
    /// Seconds to wait for the connection and the response.
    #[arg(long, default_value_t = 3)]
    pub timeout: u64,
}

#[cfg(windows)]
#[derive(Debug, Subcommand)]
pub enum ServiceCommand {
    /// Register outpace as an automatic-start service running as the NT SERVICE\outpace
    /// virtual account, with a private data directory (needs an elevated prompt). It is not
    /// started.
    Install,
    /// Stop the service if it is running, then remove it (needs an elevated prompt).
    Uninstall,
    /// Entry point used by the Service Control Manager; not meant to be run by hand.
    Run,
}

#[derive(Debug, Args)]
pub struct ServeArgs {}

#[derive(Debug, Args)]
pub struct BroadcastArgs {
    /// Broadcast name (1-64 ASCII letters, digits, '.', '_' or '-').
    #[arg(value_parser = parse_broadcast_name)]
    pub name: String,
    /// Host advertised in printed ingest and transport URLs.
    #[arg(long = "public-host")]
    pub public_host: Option<String>,
}

fn parse_broadcast_name(value: &str) -> Result<String, String> {
    crate::broadcast::valid_broadcast_name(value)
        .then(|| value.to_string())
        .ok_or_else(|| {
            "broadcast name must be 1-64 ASCII letters, digits, '.', '_' or '-' and cannot be '.' or '..'"
                .to_string()
        })
}

#[derive(Debug, Args)]
pub struct PlayArgs {
    // Clap uses this text verbatim for CLI help; preserve the angle-bracket placeholder.
    #[allow(rustdoc::invalid_html_tags)]
    /// acestream://<content-id> URL or HTTP(S) transport-file URL. `acestream:?infohash=` and
    /// magnet inputs resolve live descriptors through the authenticated catalog, requiring
    /// the descriptor to match the requested infohash. Remote Outpace broadcast Ace links require
    /// OUTPACE_BEP9_CONTENT_ID_FALLBACK=1; transport-file URLs are also supported.
    pub input: String,
    /// Bootstrap peer to try in addition to configured discovery (repeatable).
    #[arg(long = "peer")]
    pub peers: Vec<SocketAddrV4>,
    /// Treat the target as a single-file VOD: download it, verify each piece against the
    /// transport's SHA-1 hashes, and write the verified bytes to stdout (instead of a live TS).
    #[arg(long)]
    pub vod: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlaybackTarget {
    pub provider_id: String,
}

impl PlaybackTarget {
    pub fn parse(input: &str) -> Result<Self, String> {
        let input = input.trim();
        if let Some(alias) = input.strip_prefix("local:") {
            return crate::local_descriptors::alias_valid(alias)
                .then(|| Self {
                    provider_id: input.to_owned(),
                })
                .ok_or_else(|| "invalid local descriptor alias".into());
        }
        if let Some(rest) = input.strip_prefix("acestream://") {
            let rest = rest.split('#').next().unwrap_or("");
            let (id, query) = rest.split_once('?').unwrap_or((rest, ""));
            validate_uri_options(&parse_query(query)?)?;
            return content_id_target(id);
        }
        if input.starts_with("magnet:") {
            return magnet_target(input);
        }
        // A bare transport-file URL is unambiguous — accept it directly so a URL carrying its own
        // `&`-joined query string never has to be squeezed through the `acestream:?` form.
        if input.starts_with("http://") || input.starts_with("https://") {
            return url_target(input);
        }
        if let Some(query) = input.strip_prefix("acestream:?") {
            // `parse_query` percent-decodes values, so a `url=`/`magnet=` whose own query string
            // is percent-encoded (`%26` for `&`) survives — matching the HTTP query contract. A
            // URL with literal `&` should use the bare-`http(s)://` form above instead.
            let params = parse_query(query.split('#').next().unwrap_or(""))?;
            validate_uri_options(&params)?;
            if let Some(id) = params.get("content_id") {
                return content_id_target(id);
            }
            if let Some(id) = params.get("infohash") {
                return infohash_target(id);
            }
            if let Some(url) = params.get("url") {
                return url_target(url);
            }
            if let Some(magnet) = params.get("magnet") {
                return magnet_target(magnet);
            }
            return Err("acestream URL must contain content_id, infohash, url, or magnet".into());
        }
        Err("expected an acestream://, acestream:?, magnet:, or http(s):// URL".into())
    }
}

pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    match Cli::parse_cli().command {
        Command::Serve(_) => run_serve().await,
        Command::Broadcast(args) => run_broadcast(args).await,
        Command::Play(args) => run_play(args).await,
        Command::Healthcheck(args) => run_healthcheck(args),
        #[cfg(windows)]
        Command::Service(command) => tokio::task::block_in_place(|| match command {
            ServiceCommand::Install => crate::win_service::install(),
            ServiceCommand::Uninstall => crate::win_service::uninstall(),
            ServiceCommand::Run => crate::win_service::run(),
        }),
    }
}

fn run_healthcheck(args: HealthcheckArgs) -> Result<(), Box<dyn std::error::Error>> {
    let bind_env = std::env::var("OUTPACE_BIND").ok();
    let addr = crate::healthcheck::target(
        args.addr,
        bind_env.as_deref(),
        crate::config::Config::default().bind,
    )?;
    if let Err(error) =
        crate::healthcheck::probe(addr, std::time::Duration::from_secs(args.timeout.max(1)))
    {
        eprintln!("outpace healthcheck: {error}");
        std::process::exit(1);
    }
    eprintln!("outpace healthcheck: {addr} healthy");
    Ok(())
}

async fn run_serve() -> Result<(), Box<dyn std::error::Error>> {
    let config = crate::runtime::config_from_env()?;
    let peers = crate::runtime::bootstrap_peers_from_env();
    let runtime = crate::runtime::build_runtime(config, peers).await?;
    crate::runtime::serve_http(runtime).await
}

fn broadcast_config(
    mut config: crate::config::Config,
) -> Result<crate::config::Config, &'static str> {
    if config.peer_policy.is_private() {
        return Err("private_mode_broadcast_cli_requires_inbound_peers");
    }
    config.enable_inbound = true;
    Ok(config)
}

async fn run_broadcast(args: BroadcastArgs) -> Result<(), Box<dyn std::error::Error>> {
    let BroadcastArgs { name, public_host } = args;
    let config = broadcast_config(crate::runtime::config_from_env()?)?;
    let peers = crate::runtime::bootstrap_peers_from_env();
    let runtime = crate::runtime::build_runtime(config, peers).await?;
    let bc = match crate::runtime::mint_broadcast(&runtime, &name).await {
        Ok(bc) => bc,
        Err(error) => {
            let _ = runtime.broadcasts.shutdown().await;
            runtime.manager.shutdown().await;
            return Err(Box::new(error));
        }
    };
    // The serving runtime starts the same entry-owned announcers after listener startup.

    let bind = runtime.config.bind;
    let urls = crate::runtime::broadcast_ingest_urls(
        runtime.config.bind,
        runtime.config.rtmp_bind,
        public_host.clone(),
        &name,
    );
    let transport_host = public_host.unwrap_or_else(|| bind.ip().to_string());
    let content_id = infohash_hex(&bc.content_id);
    let infohash = infohash_hex(&bc.infohash);

    eprint!(
        "{}",
        broadcast_output(
            &name,
            &urls,
            &content_id,
            &infohash,
            &transport_host,
            bind.port(),
            runtime.config.peer_listen,
        )
    );

    crate::runtime::serve_http(runtime).await
}

fn broadcast_output(
    name: &str,
    urls: &crate::runtime::BroadcastIngestUrls,
    content_id: &str,
    infohash: &str,
    transport_host: &str,
    transport_port: u16,
    peer_listen: std::net::SocketAddr,
) -> String {
    format!(
        concat!(
            "outpace broadcast: {name}\n",
            "RAW Ingest URL: {raw} (MPEG-TS)\n",
            "RTMP Ingest URL: {rtmp}\n",
            "Content ID: {content_id}\n",
            "Ace link: acestream://{content_id}\n",
            "Remote Outpace consumers need OUTPACE_BEP9_CONTENT_ID_FALLBACK=1 for this raw-hash Ace link.\n",
            "The Transport URL also supports remote playback.\n",
            "Infohash: {infohash}\n",
            "Transport URL: http://{transport_host}:{transport_port}/broadcast/{name}\n",
            "Peer listen: {peer_listen}\n"
        ),
        name = name,
        raw = urls.raw.as_str(),
        rtmp = urls.rtmp.as_str(),
        content_id = content_id,
        infohash = infohash,
        transport_host = transport_host,
        transport_port = transport_port,
        peer_listen = peer_listen,
    )
}

fn play_provider_from_config(
    identity: std::sync::Arc<ace_wire::identity::Identity>,
    config: &crate::config::Config,
    peers: Vec<SocketAddrV4>,
    seed_registry: ace_swarm::listen::SeedRegistry,
) -> crate::ace_provider::AceProvider {
    // One-shot leech to stdout: it runs no inbound listener, so it discovers on the peer port
    // (never the HTTP port — issue #21) and does not self-announce as a dial-able seeder.
    crate::ace_provider::AceProvider::new(identity, config.peer_listen.port())
        .with_peer_policy(config.peer_policy.clone())
        .with_bootstrap_peers(peers)
        .with_seed_registry(seed_registry)
        .with_seed_store_bytes(config.seed_store_bytes)
        .with_seed_store_retention(std::time::Duration::from_secs(config.seed_retention_secs))
        .with_cache(config.cache_type, config.cache_dir.clone())
        .with_vod_disk_reserve(config.vod_disk_reserve_bytes)
        .with_seeding_enabled(config.enable_seeding)
        .with_bep9_content_id_fallback(config.bep9_content_id_fallback)
        .with_prefetch_pieces(config.prefetch_pieces)
        .with_startup_buffer(config.startup_buffer)
        .with_live_recovery(config.live_recovery)
}

async fn run_play(args: PlayArgs) -> Result<(), Box<dyn std::error::Error>> {
    use crate::provider::StreamProvider;
    use tokio::io::AsyncWriteExt;

    let target = PlaybackTarget::parse(&args.input)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;
    let config = crate::runtime::config_from_env()?;
    let mut peers = crate::runtime::bootstrap_peers_from_env();
    peers.extend(args.peers);
    crate::private_network::validate_private_config(&config, &peers)?;
    let local_descriptors = config.private_descriptors.load()?;

    let identity = std::sync::Arc::new(crate::config::load_or_create_identity(&config.data_dir)?);
    ace_swarm::engine_profile::set_listen_port(config.peer_listen.port());
    crate::engine_identity::apply(&config)?;
    let provider = play_provider_from_config(
        identity,
        &config,
        peers,
        ace_swarm::listen::SeedRegistry::new(),
    )
    .with_local_descriptors(local_descriptors);

    // User-facing CLI progress: plain `eprintln!` on purpose (a human is watching this run,
    // so no `alog!` timestamp/`[tag]` prefix). See the `ace_log` crate docs.
    eprintln!("outpace play: {}", args.input);
    eprintln!("outpace play: provider id {}", target.provider_id);

    if args.vod {
        eprintln!("outpace play: VOD download (verified) to stdout");
        let vod = provider
            .resolve_vod(&target.provider_id)
            .await
            .map_err(|e| {
                eprintln!("outpace play: {e}");
                std::io::Error::other(e.to_string())
            })?;
        let total = vod.content_length();
        if total == 0 {
            return Ok(());
        }
        let mut source = vod
            .open_range(0, total - 1)
            .await
            .map_err(|e| std::io::Error::other(format!("{e:?}")))?;
        let mut stdout = tokio::io::stdout();
        write_vod_output(source.as_mut(), total, &mut stdout).await?;
        return Ok(());
    }

    let mut source = provider.open(&target.provider_id).await.map_err(|e| {
        eprintln!("outpace play: {e}");
        std::io::Error::other(e.to_string())
    })?;
    let mut stdout = tokio::io::stdout();
    while let Some(chunk) = source.next().await {
        stdout.write_all(&chunk).await?;
        stdout.flush().await?;
    }
    Ok(())
}

async fn write_vod_output(
    source: &mut dyn crate::provider::VodByteSource,
    total: u64,
    output: &mut (impl tokio::io::AsyncWrite + Unpin),
) -> std::io::Result<()> {
    use tokio::io::AsyncWriteExt;

    let mut emitted = 0u64;
    while let Some(chunk) = source.next().await {
        let next = emitted.checked_add(chunk.len() as u64).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "VOD output length overflow",
            )
        })?;
        if next > total {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "VOD output exceeds its declared length",
            ));
        }
        output.write_all(&chunk).await?;
        emitted = next;
        output.flush().await?;
    }
    if emitted < total {
        return Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            format!("VOD output ended after {emitted} of {total} bytes"),
        ));
    }
    Ok(())
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

fn url_target(url: &str) -> Result<PlaybackTarget, String> {
    Ok(PlaybackTarget {
        provider_id: crate::transport_url::encode_transport_url(url)?,
    })
}

fn magnet_target(magnet: &str) -> Result<PlaybackTarget, String> {
    let hex = crate::magnet::parse_magnet_infohash(magnet)?;
    infohash_target(&hex)
}

fn normalize_hex40(id: &str) -> Result<String, String> {
    if id.len() == 40 && id.bytes().all(|b| b.is_ascii_hexdigit()) {
        Ok(id.to_ascii_lowercase())
    } else {
        Err("identifier must be 40 hex characters".into())
    }
}

/// Duplicate keys are rejected rather than resolved by position: a selector or file index
/// must not silently change meaning when a link is reordered.
fn parse_query(query: &str) -> Result<std::collections::BTreeMap<String, String>, String> {
    let mut params = std::collections::BTreeMap::new();
    for pair in query.split('&').filter(|pair| !pair.trim().is_empty()) {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        let key = percent_decode(key.trim());
        if key.is_empty() || params.insert(key, percent_decode(value.trim())).is_some() {
            return Err("empty or duplicate query parameter".into());
        }
    }
    Ok(params)
}

fn validate_uri_options(params: &std::collections::BTreeMap<String, String>) -> Result<(), String> {
    crate::source_options::validate(params.iter().map(|(k, v)| (k.as_str(), v.as_str())), true)
        .map_err(Into::into)
}

/// Percent-decode a query value (`%XX` -> byte, `+` left as-is). Invalid escapes are kept
/// literally. Lets an `acestream:?url=`/`magnet=` value carry a percent-encoded query string
/// (`%26` for `&`) so it is not split apart by the `&` param separator.
fn percent_decode(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            let hi = (bytes[i + 1] as char).to_digit(16);
            let lo = (bytes[i + 2] as char).to_digit(16);
            if let (Some(hi), Some(lo)) = (hi, lo) {
                out.push((hi * 16 + lo) as u8);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

#[cfg(test)]
mod tests {
    use super::{
        broadcast_output, play_provider_from_config, write_vod_output, Cli, Command, PlaybackTarget,
    };
    use bytes::Bytes;
    use clap::{CommandFactory, Parser};

    struct Chunks {
        length: u64,
        chunks: std::collections::VecDeque<Bytes>,
    }

    #[async_trait::async_trait]
    impl crate::provider::VodByteSource for Chunks {
        fn content_length(&self) -> u64 {
            self.length
        }

        async fn next(&mut self) -> Option<Bytes> {
            self.chunks.pop_front()
        }
    }

    fn chunks(length: u64, parts: &[&[u8]]) -> Chunks {
        Chunks {
            length,
            chunks: parts
                .iter()
                .map(|part| Bytes::copy_from_slice(part))
                .collect(),
        }
    }

    #[tokio::test]
    async fn vod_output_returns_unexpected_eof_when_source_is_short() {
        let mut source = chunks(3, &[b"ab"]);
        let mut output = Vec::new();

        let error = write_vod_output(&mut source, 3, &mut output)
            .await
            .expect_err("short VOD input must fail instead of succeeding with a truncated file");

        assert_eq!(error.kind(), std::io::ErrorKind::UnexpectedEof);
        assert_eq!(output, b"ab");
    }

    #[tokio::test]
    async fn vod_output_rejects_source_bytes_beyond_declared_total() {
        let mut source = chunks(2, &[b"abc"]);
        let mut output = Vec::new();

        let error = write_vod_output(&mut source, 2, &mut output)
            .await
            .expect_err("VOD output must not exceed the declared file length");

        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
        assert!(
            output.is_empty(),
            "reject an oversized chunk before writing it"
        );
    }

    #[tokio::test]
    async fn vod_output_writes_exact_declared_bytes_and_succeeds() {
        let mut source = chunks(5, &[b"ab", b"cde"]);
        let mut output = Vec::new();

        write_vod_output(&mut source, 5, &mut output)
            .await
            .expect("complete VOD source should write successfully");

        assert_eq!(output, b"abcde");
    }

    #[test]
    fn play_provider_receives_live_playback_policy_from_runtime_config() {
        let mut config = crate::config::Config::default();
        config.prefetch_pieces = Some(17);
        config.startup_buffer = crate::config::StartupBufferConfig {
            target_ms: 12_000,
            max_bytes: 33_554_432,
            timeout_ms: 9_000,
        };
        config.live_recovery.max_active_upstreams = 7;

        let provider = play_provider_from_config(
            std::sync::Arc::new(ace_wire::identity::Identity::generate()),
            &config,
            vec![],
            ace_swarm::listen::SeedRegistry::new(),
        );

        assert_eq!(provider.prefetch_policy(), Some(17));
        assert_eq!(provider.startup_buffer_config(), config.startup_buffer);
        assert_eq!(provider.live_recovery_config(), config.live_recovery);
    }

    #[test]
    fn play_provider_receives_bep9_policy_from_config() {
        for enabled in [false, true] {
            let config = crate::config::Config {
                bep9_content_id_fallback: enabled,
                ..Default::default()
            };
            let provider = play_provider_from_config(
                std::sync::Arc::new(ace_wire::identity::Identity::generate()),
                &config,
                vec![],
                ace_swarm::listen::SeedRegistry::new(),
            );
            assert_eq!(
                crate::ace_provider::test_support::bep9_enabled(&provider),
                enabled
            );
        }
    }

    #[test]
    fn help_identifies_native_surface_and_describes_each_command() {
        let mut command = Cli::command();
        let help = command.render_long_help().to_string();
        assert!(help.contains("native CLI and /streams API are the primary interface"));
        assert!(help.contains("supported Ace HTTP subset is disabled by default"));
        assert!(help.contains("Run the HTTP API, RTMP ingest listener"));
        assert!(help.contains("Create or resume a named broadcast"));
        assert!(help.contains("Write a live MPEG-TS stream"));

        let play = command
            .find_subcommand_mut("play")
            .expect("play subcommand")
            .render_long_help()
            .to_string();
        let play_flat = play.split_whitespace().collect::<Vec<_>>().join(" ");
        assert!(play_flat.contains("acestream://<content-id> URL or HTTP(S) transport-file URL"));
        assert!(play_flat.contains("authenticated catalog"));
        assert!(play_flat.contains("match the requested infohash"));
        assert!(play.contains("verified VOD"));
    }

    #[tokio::test]
    async fn play_unavailable_infohash_and_magnet_inputs_fail_closed_with_the_cid_hint() {
        use crate::provider::StreamProvider;
        let ih = "0123456789abcdef0123456789abcdef01234567";
        let provider = play_provider_from_config(
            std::sync::Arc::new(ace_wire::identity::Identity::generate()),
            &crate::config::Config::default(),
            vec![],
            ace_swarm::listen::SeedRegistry::new(),
        );
        provider.cache_infohash_failure_for_test(ih);
        for input in [
            format!("acestream:?infohash={ih}"),
            format!("magnet:?xt=urn:btih:{ih}"),
        ] {
            let target = PlaybackTarget::parse(&input).unwrap();
            let err = provider
                .open(&target.provider_id)
                .await
                .err()
                .expect("unavailable catalog metadata must fail closed");
            assert!(
                err.to_string().contains(&format!("cid:{ih}")),
                "{input}: {err}"
            );
        }
    }

    #[test]
    fn broadcast_name_rejects_unsafe_persistence_names() {
        for name in [".", "..", "../escape", "has/slash", "café"] {
            assert!(
                Cli::try_parse_from(["outpace", "broadcast", name]).is_err(),
                "accepted unsafe broadcast name {name:?}"
            );
        }
        let too_long = "a".repeat(65);
        assert!(Cli::try_parse_from(["outpace", "broadcast", &too_long]).is_err());
        assert!(Cli::try_parse_from(["outpace", "broadcast", "sports-1_test.ok"]).is_ok());
    }

    #[test]
    fn no_args_defaults_to_serve() {
        let cli = Cli::parse_cli_from(["outpace"]);

        assert!(matches!(cli.command, Command::Serve(_)));
    }

    #[test]
    fn parses_serve() {
        let cli = Cli::parse_from(["outpace", "serve"]);

        assert!(matches!(cli.command, Command::Serve(_)));
    }

    #[test]
    fn parses_healthcheck_defaults_and_flags() {
        match Cli::parse_from(["outpace", "healthcheck"]).command {
            Command::Healthcheck(args) => {
                assert_eq!(args.addr, None);
                assert_eq!(args.timeout, 3);
            }
            other => panic!("expected healthcheck, got {other:?}"),
        }
        match Cli::parse_from([
            "outpace",
            "healthcheck",
            "--addr",
            "127.0.0.1:7000",
            "--timeout",
            "5",
        ])
        .command
        {
            Command::Healthcheck(args) => {
                assert_eq!(args.addr, Some("127.0.0.1:7000".parse().unwrap()));
                assert_eq!(args.timeout, 5);
            }
            other => panic!("expected healthcheck, got {other:?}"),
        }
    }

    #[test]
    fn play_defaults_to_live_and_accepts_vod_flag() {
        let live = Cli::parse_from([
            "outpace",
            "play",
            "acestream://0123456789abcdef0123456789abcdef01234567",
        ]);
        match live.command {
            Command::Play(args) => assert!(!args.vod, "play defaults to live"),
            other => panic!("expected play command, got {other:?}"),
        }
        let vod = Cli::parse_from([
            "outpace",
            "play",
            "--vod",
            "acestream://0123456789abcdef0123456789abcdef01234567",
        ]);
        match vod.command {
            Command::Play(args) => assert!(args.vod, "--vod selects the VOD path"),
            other => panic!("expected play command, got {other:?}"),
        }
    }

    #[test]
    fn parses_broadcast_name() {
        let cli = Cli::parse_from(["outpace", "broadcast", "sports"]);

        match cli.command {
            Command::Broadcast(args) => assert_eq!(args.name, "sports"),
            other => panic!("expected broadcast command, got {other:?}"),
        }
    }

    #[test]
    fn parses_broadcast_public_host() {
        let cli = Cli::parse_from([
            "outpace",
            "broadcast",
            "sports",
            "--public-host",
            "stream.example",
        ]);

        match cli.command {
            Command::Broadcast(args) => {
                assert_eq!(args.name, "sports");
                assert_eq!(args.public_host.as_deref(), Some("stream.example"));
            }
            other => panic!("expected broadcast command, got {other:?}"),
        }
    }

    #[test]
    fn broadcast_output_uses_raw_and_rtmp_ingest_labels() {
        let urls = crate::runtime::BroadcastIngestUrls {
            raw: "http://stream.example:6878/broadcast/sports".to_string(),
            rtmp: "rtmp://stream.example:1935/live/sports".to_string(),
        };
        let output = broadcast_output(
            "sports",
            &urls,
            "0123456789abcdef0123456789abcdef01234567",
            "89abcdef0123456789abcdef0123456789abcdef",
            "stream.example",
            6878,
            "127.0.0.1:8621".parse().unwrap(),
        );

        assert!(output
            .contains("RAW Ingest URL: http://stream.example:6878/broadcast/sports (MPEG-TS)"));
        assert!(output.contains("RTMP Ingest URL: rtmp://stream.example:1935/live/sports"));
        assert!(output.contains("(MPEG-TS)"));
        let old_label = ["OBS", " ingest URL"].concat();
        assert!(!output.contains(&old_label));
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
            other => panic!("expected play command, got {other:?}"),
        }
    }

    #[test]
    fn parses_bare_magnet_input() {
        let t =
            PlaybackTarget::parse("magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567")
                .unwrap();
        assert_eq!(t.provider_id, "0123456789abcdef0123456789abcdef01234567");
    }

    #[test]
    fn parses_transport_url_input() {
        let t = PlaybackTarget::parse("acestream:?url=https://example.com/x.acelive").unwrap();
        assert_eq!(
            crate::transport_url::decode_transport_url(&t.provider_id).as_deref(),
            Some("https://example.com/x.acelive")
        );
    }

    #[test]
    fn parses_bare_transport_url_with_query_string() {
        // A literal `&` in the URL's own query works via the bare form (no acestream:? wrapper).
        let url = "https://cdn.example.com/t.acelive?token=abc&sig=xyz";
        let t = PlaybackTarget::parse(url).unwrap();
        assert_eq!(
            crate::transport_url::decode_transport_url(&t.provider_id).as_deref(),
            Some(url)
        );
    }

    #[test]
    fn acestream_url_percent_encoded_query_survives() {
        // %3F=? %3D== %26=& — the url's own query is percent-encoded so the outer `&` separator
        // does not truncate it.
        let t = PlaybackTarget::parse("acestream:?url=https://h/x%3Fa%3D1%26b%3D2").unwrap();
        assert_eq!(
            crate::transport_url::decode_transport_url(&t.provider_id).as_deref(),
            Some("https://h/x?a=1&b=2")
        );
    }

    #[test]
    fn selector_precedence_content_id_over_infohash_over_url_over_magnet() {
        let ih = "0123456789abcdef0123456789abcdef01234567";
        let cid = "89abcdef0123456789abcdef0123456789abcdef";
        // content_id wins.
        let t = PlaybackTarget::parse(&format!(
            "acestream:?content_id={cid}&infohash={ih}&url=https://e/x&magnet=magnet:?xt=urn:btih:{ih}"
        ))
        .unwrap();
        assert_eq!(t.provider_id, format!("cid:{cid}"));
        // then infohash.
        let t = PlaybackTarget::parse(&format!(
            "acestream:?infohash={ih}&url=https://e/x&magnet=magnet:?xt=urn:btih:{ih}"
        ))
        .unwrap();
        assert_eq!(t.provider_id, ih);
        // then url.
        let t = PlaybackTarget::parse(&format!(
            "acestream:?url=https://e/x.acelive&magnet=magnet:?xt=urn:btih:{ih}"
        ))
        .unwrap();
        assert_eq!(
            crate::transport_url::decode_transport_url(&t.provider_id).as_deref(),
            Some("https://e/x.acelive")
        );
    }

    #[test]
    fn ace_uri_keeps_selected_file_intent_and_rejects_duplicates() {
        let id = "0123456789abcdef0123456789abcdef01234567";
        for prefix in [
            format!("acestream://{id}?"),
            format!("acestream:?content_id={id}&"),
        ] {
            assert!(PlaybackTarget::parse(&format!("{prefix}index=0")).is_ok());
            assert!(PlaybackTarget::parse(&format!("{prefix}index=0#fragment")).is_ok());
            for query in [
                "index=1",
                "index=-1",
                "index=",
                "index=0&%69ndex=1",
                "_idx=0",
                "stream_id=0",
                "stream_id=-1",
                "direct_url=https%3A%2F%2Fexample.test%2Fmedia",
            ] {
                assert!(
                    PlaybackTarget::parse(&format!("{prefix}{query}")).is_err(),
                    "{prefix}{query}"
                );
            }
        }
        assert!(
            PlaybackTarget::parse(&format!("acestream:?content_id={id}&content_id={id}")).is_err()
        );
    }

    #[test]
    fn rejects_non_http_transport_url() {
        assert!(PlaybackTarget::parse("acestream:?url=file:///etc/passwd").is_err());
    }

    #[test]
    fn old_acestream_url_is_content_id() {
        let parsed =
            PlaybackTarget::parse("acestream://0123456789abcdef0123456789abcdef01234567").unwrap();
        assert_eq!(
            parsed.provider_id,
            "cid:0123456789abcdef0123456789abcdef01234567"
        );
    }

    #[test]
    fn query_content_id_is_content_id() {
        let parsed =
            PlaybackTarget::parse("acestream:?content_id=0123456789abcdef0123456789abcdef01234567")
                .unwrap();
        assert_eq!(
            parsed.provider_id,
            "cid:0123456789abcdef0123456789abcdef01234567"
        );
    }

    #[test]
    fn query_infohash_is_direct_infohash() {
        let parsed =
            PlaybackTarget::parse("acestream:?infohash=89abcdef0123456789abcdef0123456789abcdef")
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
}

#[cfg(test)]
mod local_alias_tests {
    use super::PlaybackTarget;
    #[test]
    fn local_alias_is_a_selector_not_a_request_path() {
        assert_eq!(
            PlaybackTarget::parse("local:fixture").unwrap().provider_id,
            "local:fixture"
        );
        for value in [
            "local:",
            "local:../fixture",
            "local:/etc/passwd",
            "local:a/b",
            "local:a?x",
        ] {
            assert!(PlaybackTarget::parse(value).is_err());
        }
    }
}

#[cfg(test)]
mod private_broadcast_tests {
    use super::*;
    #[test]
    fn private_broadcast_cli_never_widens_inbound_policy() {
        let private = crate::config::Config {
            peer_policy: ace_swarm::peer_policy::PeerPolicy::from_private_peers(Some(
                "127.0.0.1:1234",
            ))
            .unwrap(),
            enable_inbound: false,
            ..Default::default()
        };
        assert!(matches!(
            broadcast_config(private),
            Err("private_mode_broadcast_cli_requires_inbound_peers")
        ));
        let public = crate::config::Config {
            enable_inbound: false,
            ..Default::default()
        };
        assert!(broadcast_config(public).unwrap().enable_inbound);
    }
}
