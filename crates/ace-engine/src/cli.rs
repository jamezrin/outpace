use std::net::SocketAddrV4;

use clap::{Args, Parser, Subcommand};

#[derive(Debug, Parser)]
#[command(
    name = "outpace",
    about = "Broadcast and play Acestream-compatible live streams"
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
    Serve(ServeArgs),
    Broadcast(BroadcastArgs),
    Play(PlayArgs),
}

#[derive(Debug, Args)]
pub struct ServeArgs {}

#[derive(Debug, Args)]
pub struct BroadcastArgs {
    pub name: String,
    #[arg(long = "public-host")]
    pub public_host: Option<String>,
}

#[derive(Debug, Args)]
pub struct PlayArgs {
    pub input: String,
    #[arg(long = "peer")]
    pub peers: Vec<SocketAddrV4>,
}

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

async fn run_broadcast(args: BroadcastArgs) -> Result<(), Box<dyn std::error::Error>> {
    let BroadcastArgs { name, public_host } = args;
    let mut config = crate::runtime::config_from_env()?;
    config.enable_inbound = true;
    let peers = crate::runtime::bootstrap_peers_from_env();
    let runtime = crate::runtime::build_runtime(config, peers).await?;
    let bc = crate::runtime::mint_broadcast(&runtime, &name).await;
    crate::runtime::announce_broadcast(&runtime, &bc);

    let bind = runtime.config.bind;
    let urls = crate::runtime::broadcast_ingest_urls(
        runtime.config.bind,
        runtime.config.rtmp_bind,
        public_host.clone(),
        &name,
    );
    let transport_host = public_host.unwrap_or_else(|| bind.ip().to_string());
    let content_id = hex20(&bc.content_id);
    let infohash = hex20(&bc.infohash);

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

async fn run_play(args: PlayArgs) -> Result<(), Box<dyn std::error::Error>> {
    use crate::provider::StreamProvider;
    use tokio::io::AsyncWriteExt;

    let target = PlaybackTarget::parse(&args.input)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;
    let config = crate::runtime::config_from_env()?;
    let mut peers = crate::runtime::bootstrap_peers_from_env();
    peers.extend(args.peers);

    let identity = std::sync::Arc::new(crate::config::load_or_create_identity(&config.data_dir)?);
    let seed_registry = ace_swarm::listen::SeedRegistry::new();
    // One-shot leech to stdout: it runs no inbound listener, so it discovers on the peer port
    // (never the HTTP port — issue #21) and does not self-announce as a dial-able seeder.
    let provider = crate::ace_provider::AceProvider::new(identity, config.peer_listen.port())
        .with_bootstrap_peers(peers)
        .with_seed_registry(seed_registry)
        .with_seed_store_bytes(config.seed_store_bytes)
        .with_cache(config.cache_type, config.cache_dir.clone())
        .with_seeding_enabled(config.enable_seeding);

    eprintln!("outpace play: {}", args.input);
    eprintln!("outpace play: provider id {}", target.provider_id);

    let mut source = provider
        .open(&target.provider_id)
        .await
        .map_err(|e| std::io::Error::other(format!("{e:?}")))?;
    let mut stdout = tokio::io::stdout();
    while let Some(chunk) = source.next().await {
        stdout.write_all(&chunk).await?;
        stdout.flush().await?;
    }
    Ok(())
}

fn hex20(bytes: &[u8; 20]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
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

#[cfg(test)]
mod tests {
    use super::{broadcast_output, Cli, Command, PlaybackTarget};
    use clap::Parser;

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
