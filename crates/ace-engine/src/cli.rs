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
}

#[derive(Debug, Args)]
pub struct PlayArgs {
    pub input: String,
    #[arg(long = "peer")]
    pub peers: Vec<SocketAddrV4>,
}

#[cfg(test)]
mod tests {
    use super::{Cli, Command};
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
}
