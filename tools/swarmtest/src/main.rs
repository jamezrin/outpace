//! `swarmtest` — interop test harness for the outpace AceStream reimplementation.
//!
//! Thin CLI over the `swarmtest` library: it resolves config, runs the docker preflight,
//! acquires the engine, stands up the host-side tracker + descriptor httpd, and drives
//! each scenario ([`swarmtest::scenario`]) to a report. On-demand and local-only; see
//! `docs/testing/interop-swarm.md`.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand, ValueEnum};

use swarmtest::config::{self, Config, Scenario};
use swarmtest::{engine, httpd, report, scenario, tracker};

#[derive(Parser)]
#[command(
    name = "swarmtest",
    about = "AceStream <-> outpace interop swarm test harness"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run one or more interop scenarios.
    Run(RunArgs),
    /// Download the engine tarball and print its SHA-256 (to pin ENGINE_SHA256).
    #[command(hide = true)]
    VerifyEngineHash(VerifyEngineHashArgs),
}

#[derive(clap::Args)]
struct RunArgs {
    /// Which scenario(s) to run.
    #[arg(long, value_enum, default_value_t = ScenarioArg::All)]
    scenario: ScenarioArg,
    /// Warmup window before assertions, in seconds.
    #[arg(long, default_value_t = config::DEFAULT_WARMUP_SECS)]
    warmup_secs: u64,
    /// Measurement window, in seconds.
    #[arg(long, default_value_t = config::DEFAULT_WINDOW_SECS)]
    window_secs: u64,
    /// Use engine binaries from this directory (must contain `acestreamengine`).
    #[arg(long)]
    engine_dir: Option<PathBuf>,
    /// Engine tarball download URL.
    #[arg(long, default_value = config::DEFAULT_ENGINE_URL)]
    engine_url: String,
    /// Keep the run directory / containers after completion.
    #[arg(long)]
    keep: bool,
    /// Capture packet traces (pcap) during the run.
    #[arg(long)]
    pcap: bool,
    /// Address the in-process UDP tracker binds to.
    #[arg(long, default_value = config::DEFAULT_TRACKER_ADDR)]
    tracker_addr: SocketAddr,
    /// Address the static descriptor HTTP server binds to.
    #[arg(long, default_value = config::DEFAULT_HTTPD_ADDR)]
    httpd_addr: SocketAddr,
}

#[derive(clap::Args)]
struct VerifyEngineHashArgs {
    /// Engine tarball download URL.
    #[arg(long, default_value = config::DEFAULT_ENGINE_URL)]
    engine_url: String,
}

#[derive(Copy, Clone, PartialEq, Eq, ValueEnum)]
enum ScenarioArg {
    Baseline,
    Mixed,
    OutpaceSource,
    All,
}

impl ScenarioArg {
    fn expand(self) -> Vec<Scenario> {
        match self {
            ScenarioArg::Baseline => vec![Scenario::Baseline],
            ScenarioArg::Mixed => vec![Scenario::Mixed],
            ScenarioArg::OutpaceSource => vec![Scenario::OutpaceSource],
            ScenarioArg::All => Scenario::all(),
        }
    }
}

/// Process exit code for a preflight skip (docker/engine missing): distinct from an
/// all-pass (0) or any-fail (1) so a CI/wrapper can tell "not run here" from "failed".
const EXIT_PREFLIGHT_SKIP: u8 = 2;

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    match cli.command {
        Command::Run(args) => match run(args).await {
            Ok(code) => ExitCode::from(code),
            Err(e) => {
                eprintln!("error: {e:#}");
                ExitCode::FAILURE
            }
        },
        Command::VerifyEngineHash(args) => {
            match engine::download_and_hash(&args.engine_url).await {
                Ok(hash) => {
                    println!("{hash}  {}", args.engine_url);
                    ExitCode::SUCCESS
                }
                Err(e) => {
                    eprintln!("error: {e:#}");
                    ExitCode::FAILURE
                }
            }
        }
    }
}

/// Run the selected scenarios. Returns the process exit code: `0` all-pass, `1`
/// any-fail, `2` preflight-skip (docker or engine unavailable).
async fn run(args: RunArgs) -> Result<u8> {
    let workspace_root = workspace_root()?;
    let run_dir = config::allocate_run_dir(&workspace_root)?;

    let config = Config {
        scenarios: args.scenario.expand(),
        warmup_secs: args.warmup_secs,
        window_secs: args.window_secs,
        engine_dir: args.engine_dir,
        engine_url: args.engine_url,
        keep: args.keep,
        pcap: args.pcap,
        run_dir,
        workspace_root,
        tracker_addr: args.tracker_addr,
        httpd_addr: args.httpd_addr,
    };

    if !config.run_dir.exists() {
        anyhow::bail!("run directory {} does not exist", config.run_dir.display());
    }

    // Preflight: docker must be present/usable, else skip cleanly (exit 2), never panic.
    if let Err(e) = scenario::check_docker_available() {
        eprintln!("preflight skip: {e:#}");
        return Ok(EXIT_PREFLIGHT_SKIP);
    }

    // Preflight: the proprietary engine must be acquirable (download/cache/override).
    let engine_bin = match engine::acquire_engine(config.engine_dir.as_deref(), &config.engine_url)
        .await
        .context("acquiring the AceStream engine")
    {
        Ok(bin) => bin,
        Err(e) => {
            eprintln!("preflight skip: {e:#}");
            return Ok(EXIT_PREFLIGHT_SKIP);
        }
    };
    println!("engine binary: {}", engine_bin.display());
    println!("run directory: {}", config.run_dir.display());
    println!(
        "scenarios: {}",
        config
            .scenarios
            .iter()
            .map(|s| s.as_str())
            .collect::<Vec<_>>()
            .join(", ")
    );

    // Host-side interop services reachable from containers via the bridge gateway.
    let tracker = tracker::start(config.tracker_addr)
        .await
        .with_context(|| format!("binding interop tracker on {}", config.tracker_addr))?;
    let httpd = httpd::start(config.httpd_addr)
        .await
        .with_context(|| format!("binding descriptor httpd on {}", config.httpd_addr))?;

    let mut reports = Vec::new();
    for scenario in &config.scenarios {
        println!("\n>>> running scenario: {}", scenario.as_str());
        let report =
            match scenario::run_scenario(&config, *scenario, &engine_bin, &tracker, &httpd).await {
                Ok(r) => r,
                Err(e) => {
                    eprintln!("[{}] scenario error: {e:#}", scenario.as_str());
                    report::ScenarioReport::errored(scenario.as_str(), e)
                }
            };
        reports.push(report);
    }

    report::render_table(&reports);
    if let Err(e) = report::write_json(&config.run_dir, &reports) {
        eprintln!("warning: could not write report.json: {e:#}");
    }
    println!("run directory: {}", config.run_dir.display());

    tracker.shutdown().await;
    httpd.shutdown().await;

    Ok(report::overall_exit_code(&reports) as u8)
}

/// Resolve the workspace root (the directory containing `target/`).
///
/// `CARGO_MANIFEST_DIR` points at `tools/swarmtest`; the workspace root is two
/// levels up. Falls back to the current directory when that is unavailable.
fn workspace_root() -> Result<PathBuf> {
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    Ok(manifest
        .ancestors()
        .nth(2)
        .map(Path::to_path_buf)
        .unwrap_or_else(|| manifest.to_path_buf()))
}
