//! Docker-driven orchestration for one interop scenario.
//!
//! This is the harness's imperative glue: it builds images, writes the generated
//! compose file, stages `docker compose up` behind readiness gates, patches + serves
//! the swarm descriptor, attaches consumers, samples them through a measurement window,
//! and tears down. It cannot be unit-tested in this environment (it needs docker and the
//! proprietary engine), so every judgement is delegated to the tested pure functions in
//! [`crate::assertions`]/[`crate::compose`]/[`crate::peers`] and the whole thing is
//! guarded by [`check_docker_available`]. It never fabricates success: with no docker it
//! fails loudly and early.

use std::net::{Ipv4Addr, SocketAddrV4};
use std::path::Path;
use std::process::Stdio;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use tokio::process::Command;

use crate::compose::{self, ComposeParams, HTTP_API_PORT};
use crate::config::{Config, Scenario};
use crate::peers::{self, EngineConsumer, OutpaceConsumer};
use crate::report::{PeerReport, ScenarioReport};
use crate::{httpd::HttpdHandle, tracker::TrackerHandle};

/// Image tag for the built engine sandbox.
pub const ENGINE_IMAGE_TAG: &str = "swarmtest-engine:latest";
/// Image tag for the built outpace binary.
pub const OUTPACE_IMAGE_TAG: &str = "swarmtest-outpace:latest";
/// The ace network segment used for outpace stream routes.
const ACE_NETWORK: &str = "ace";
/// Nominal media byte-rate (~1.4 Mb/s) used to score stream throughput.
const EXPECTED_BPS: u64 = 175_000;
/// How many head bytes to capture for the MPEG-TS contiguity check (~100 TS packets).
const HEAD_CAP: usize = 188 * 100;
/// Sampling cadence within the measurement window.
const SAMPLE_INTERVAL: Duration = Duration::from_secs(2);
/// How long to wait for containers/descriptors to become ready.
const READY_TIMEOUT: Duration = Duration::from_secs(120);

/// Verify docker (and the compose plugin) are installed and the daemon is reachable.
///
/// Returns an actionable error the CLI turns into a preflight-skip (exit 2) rather than
/// a panic, so running the harness on a machine without docker is a clean no-op.
pub fn check_docker_available() -> Result<()> {
    let out = std::process::Command::new("docker")
        .args(["compose", "version"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
    match out {
        Ok(status) if status.success() => {}
        Ok(status) => bail!(
            "`docker compose version` exited with {status}. Install Docker Engine with the \
             Compose v2 plugin and ensure the daemon is running (this harness needs rootful \
             docker on Linux)."
        ),
        Err(e) => bail!(
            "could not run `docker`: {e}. Install Docker Engine (Compose v2) and ensure it is on \
             PATH and the daemon is running. See docs/testing/interop-swarm.md."
        ),
    }
    // Confirm the daemon actually answers, not just that the client exists.
    let info = std::process::Command::new("docker")
        .args(["info", "--format", "{{.ServerVersion}}"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
    match info {
        Ok(status) if status.success() => Ok(()),
        Ok(_) => bail!(
            "the docker daemon is not reachable (`docker info` failed). Start it (e.g. \
             `systemctl start docker`) and re-run."
        ),
        Err(e) => bail!("could not query the docker daemon: {e}"),
    }
}

/// Orchestrate `scenario` end to end and return its report.
///
/// `engine_bin` locates the extracted engine tree (its parent is the image build
/// context); `tracker`/`httpd` are the already-running host-side interop services the
/// containers reach via the bridge gateway.
pub async fn run_scenario(
    config: &Config,
    scenario: Scenario,
    engine_bin: &Path,
    tracker: &TrackerHandle,
    httpd: &HttpdHandle,
) -> Result<ScenarioReport> {
    let engine_dir = engine_bin
        .parent()
        .ok_or_else(|| anyhow!("engine binary {} has no parent dir", engine_bin.display()))?;
    ensure_images(config, engine_dir).await?;

    // Per-scenario working directory under the run dir.
    let scen_dir = config.run_dir.join(scenario.as_str());
    let pub_dir = scen_dir.join("pub");
    std::fs::create_dir_all(&pub_dir).with_context(|| format!("creating {}", pub_dir.display()))?;

    // Resolve compose params: fixed bridge, our image tags, host-side tracker/httpd ports.
    let mut params = ComposeParams::for_scenario(scenario);
    params.engine_image = ENGINE_IMAGE_TAG.to_string();
    params.outpace_image = OUTPACE_IMAGE_TAG.to_string();
    params.tracker_port = tracker.local_addr.port();
    params.httpd_port = httpd.local_addr.port();
    params.source_pub_dir = pub_dir.to_string_lossy().into_owned();
    // Optional tcpdump sidecar: capture the source container's traffic into the run dir.
    params.pcap = config.pcap;
    if config.pcap {
        let caps_dir = scen_dir.join("caps");
        std::fs::create_dir_all(&caps_dir)
            .with_context(|| format!("creating {}", caps_dir.display()))?;
        params.caps_dir = caps_dir.to_string_lossy().into_owned();
    }

    let compose_path = scen_dir.join("docker-compose.yaml");
    std::fs::write(&compose_path, compose::compose_yaml(scenario, &params))
        .with_context(|| format!("writing {}", compose_path.display()))?;

    // Run the body, then always dump logs / journal on error-or-keep and tear down.
    let outcome = measure(
        config,
        scenario,
        &params,
        &compose_path,
        &pub_dir,
        tracker,
        httpd,
    )
    .await;

    if outcome.is_err() || config.keep {
        if let Err(e) = dump_logs(&compose_path, &scen_dir).await {
            eprintln!(
                "[{}] warning: could not dump compose logs: {e:#}",
                scenario.as_str()
            );
        }
    }
    write_journal(&scen_dir, tracker);

    if !config.keep {
        if let Err(e) = compose_down(&compose_path).await {
            eprintln!("[{}] warning: teardown failed: {e:#}", scenario.as_str());
        }
    }

    let (peers, mut notes) = outcome?;
    notes.push(format!(
        "tracker announces recorded: {}",
        tracker.journal_snapshot().len()
    ));
    if config.pcap {
        notes.push(format!(
            "pcap capture: {}",
            scen_dir
                .join("caps")
                .join(format!("{}.pcap", scenario.as_str()))
                .display()
        ));
    }
    Ok(ScenarioReport::new(scenario.as_str(), peers, notes))
}

/// The staged happy path: up -> readiness -> descriptor -> attach -> sample -> verdicts.
async fn measure(
    config: &Config,
    scenario: Scenario,
    params: &ComposeParams,
    compose_path: &Path,
    pub_dir: &Path,
    tracker: &TrackerHandle,
    httpd: &HttpdHandle,
) -> Result<(Vec<PeerReport>, Vec<String>)> {
    let mut notes = Vec::new();
    compose_up(compose_path).await?;

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .context("building http client")?;

    // --- source readiness + swarm descriptor.
    let descriptor = obtain_descriptor(scenario, params, pub_dir, &client).await?;
    let tracker_url = params.tracker_url();
    let patched = crate::transport::patch_trackers(&descriptor, std::slice::from_ref(&tracker_url))
        .context("patching descriptor trackers")?;
    if let Ok(infohash) = crate::transport::infohash_of(&patched) {
        notes.push(format!("swarm infohash: {infohash}"));
        // Deterministic rendezvous: the source may announce only to its own embedded
        // trackers, so seed our tracker with the source's known static IP + peer port.
        // Consumers announcing this infohash then always learn the source.
        match seed_source(tracker, &infohash, scenario, &params.source_ip) {
            Ok(peer) => notes.push(format!("seeded source peer {peer} for infohash")),
            Err(e) => notes.push(format!("warning: could not seed source peer: {e}")),
        }
    } else {
        notes.push("warning: could not compute swarm infohash; source not seeded".to_string());
    }
    let descriptor_path = format!("/{}.acelive", scenario.as_str());
    httpd.register(&descriptor_path, patched);
    let descriptor_url = params.descriptor_url(scenario);
    notes.push(format!("descriptor served at {descriptor_url}"));

    // --- build + ready the consumers.
    let mut engine_consumers = Vec::new();
    for (i, ip) in params.engine_consumer_ips.iter().enumerate() {
        let base = format!("http://{ip}:{HTTP_API_PORT}");
        let ready = format!("{base}/webui/api/service?method=get_version&format=json");
        peers::wait_for_json(&client, &ready, READY_TIMEOUT, |_| true)
            .await
            .with_context(|| format!("engine-consumer-{} readiness", i + 1))?;
        let mut c = EngineConsumer::new(format!("engine-consumer-{}", i + 1), base, client.clone());
        c.attach(&descriptor_url)
            .await
            .with_context(|| format!("attaching engine-consumer-{}", i + 1))?;
        engine_consumers.push(c);
    }

    let mut outpace_consumers = Vec::new();
    for (i, ip) in params.outpace_consumer_ips.iter().enumerate() {
        let base = format!("http://{ip}:{HTTP_API_PORT}");
        peers::wait_for_ok(&client, &format!("{base}/healthz"), READY_TIMEOUT)
            .await
            .with_context(|| format!("outpace-consumer-{} readiness", i + 1))?;
        let c = OutpaceConsumer::new(
            format!("outpace-consumer-{}", i + 1),
            base,
            ACE_NETWORK,
            &descriptor_url,
            client.clone(),
        )?;
        outpace_consumers.push(c);
    }

    // --- Consume playback continuously for warmup+window, discarding the warmup span in
    // the verdicts. The pull must start right after attach: the engine reaps an idle
    // getstream session, so a gap between attach and first read invalidates the session.
    notes.push(format!(
        "warmup {}s + window {}s (continuous pull)",
        config.warmup_secs, config.window_secs
    ));
    let warmup = Duration::from_secs(config.warmup_secs);
    let window = Duration::from_secs(config.window_secs);
    let reports = sample_window(
        &client,
        warmup,
        window,
        &mut engine_consumers,
        &outpace_consumers,
    )
    .await?;

    Ok((reports, notes))
}

/// Stream playback from every consumer for `warmup + window` while polling stats every
/// [`SAMPLE_INTERVAL`], then reduce each to a [`PeerReport`] with the warmup span excluded.
async fn sample_window(
    client: &reqwest::Client,
    warmup: Duration,
    window: Duration,
    engine_consumers: &mut [EngineConsumer],
    outpace_consumers: &[OutpaceConsumer],
) -> Result<Vec<PeerReport>> {
    let total = warmup + window;
    let warmup_secs = warmup.as_secs() as usize;
    // Number of leading stat samples that fall within the warmup span.
    let warmup_samples = (warmup.as_secs() / SAMPLE_INTERVAL.as_secs().max(1)) as usize;
    // Start one playback pull per consumer, each mirroring its byte total into a counter.
    struct Pull {
        counter: Arc<AtomicU64>,
        handle: tokio::task::JoinHandle<Result<peers::PlaybackPull>>,
    }
    let mut engine_pulls = Vec::new();
    for c in engine_consumers.iter() {
        let counter = Arc::new(AtomicU64::new(0));
        let url = c
            .playback_url()
            .ok_or_else(|| anyhow!("{} has no playback url", c.name))?
            .to_string();
        let client = client.clone();
        let counter2 = counter.clone();
        let handle = tokio::spawn(async move {
            peers::pull_playback(&client, &url, total, HEAD_CAP, counter2).await
        });
        engine_pulls.push(Pull { counter, handle });
    }
    let mut outpace_pulls = Vec::new();
    for c in outpace_consumers.iter() {
        let counter = Arc::new(AtomicU64::new(0));
        let url = c.playback_url();
        let client = client.clone();
        let counter2 = counter.clone();
        let handle = tokio::spawn(async move {
            peers::pull_playback(&client, &url, total, HEAD_CAP, counter2).await
        });
        outpace_pulls.push(Pull { counter, handle });
    }

    // Poll stats every SAMPLE_INTERVAL across warmup+window.
    let ticks = (total.as_secs() / SAMPLE_INTERVAL.as_secs()).max(1);
    let mut engine_series: Vec<Vec<crate::assertions::PeerStats>> =
        vec![Vec::new(); engine_consumers.len()];
    let mut outpace_series: Vec<Vec<crate::assertions::PeerStats>> =
        vec![Vec::new(); outpace_consumers.len()];
    for _ in 0..ticks {
        tokio::time::sleep(SAMPLE_INTERVAL).await;
        for (idx, c) in engine_consumers.iter().enumerate() {
            if let Ok(s) = c.poll_stats().await {
                engine_series[idx].push(s);
            }
        }
        for (idx, c) in outpace_consumers.iter().enumerate() {
            let downloaded = outpace_pulls[idx].counter.load(Ordering::Relaxed);
            if let Ok(s) = c.poll_stats(downloaded).await {
                outpace_series[idx].push(s);
            }
        }
    }

    // Join pulls and build reports.
    let mut reports = Vec::new();
    for (idx, (c, pull)) in engine_consumers.iter().zip(engine_pulls).enumerate() {
        let pull = join_pull(pull.handle).await?;
        reports.push(build_report(
            &c.name,
            "engine-consumer",
            &engine_series[idx],
            &pull,
            warmup_samples,
            warmup_secs,
        ));
    }
    for (idx, (c, pull)) in outpace_consumers.iter().zip(outpace_pulls).enumerate() {
        let pull = join_pull(pull.handle).await?;
        reports.push(build_report(
            &c.name,
            "outpace-consumer",
            &outpace_series[idx],
            &pull,
            warmup_samples,
            warmup_secs,
        ));
    }
    Ok(reports)
}

/// Await a spawned playback pull, flattening the join error into the result.
async fn join_pull(
    handle: tokio::task::JoinHandle<Result<peers::PlaybackPull>>,
) -> Result<peers::PlaybackPull> {
    match handle.await {
        Ok(inner) => inner,
        Err(e) => Err(anyhow!("playback task panicked: {e}")),
    }
}

/// Reduce one consumer's samples + playback pull to a [`PeerReport`] via the pure verdicts.
fn build_report(
    name: &str,
    role: &str,
    series: &[crate::assertions::PeerStats],
    pull: &peers::PlaybackPull,
    warmup_samples: usize,
    warmup_secs: usize,
) -> PeerReport {
    let health = crate::assertions::swarm_health(series, warmup_samples);
    // Only score throughput over the post-warmup buckets; the warmup span is where the
    // swarm is still forming and the playback session is stabilising.
    let post_warmup = pull.per_second.get(warmup_secs..).unwrap_or(&[]);
    let stability = crate::assertions::stream_stability(post_warmup, EXPECTED_BPS);
    let ts_ok = crate::assertions::ts_contiguity(&pull.head);
    let last_raw = series
        .last()
        .map(|s| s.raw.clone())
        .unwrap_or(serde_json::Value::Null);
    PeerReport::finalize(name.into(), role.into(), health, stability, ts_ok, last_raw)
}

/// Fetch the swarm transport descriptor for `scenario`: read the engine source's
/// bind-mounted `test.acelive`, or GET the outpace source's `/broadcast/test`.
async fn obtain_descriptor(
    scenario: Scenario,
    params: &ComposeParams,
    pub_dir: &Path,
    client: &reqwest::Client,
) -> Result<Vec<u8>> {
    let source_base = format!("http://{}:{}", params.source_ip, HTTP_API_PORT);
    if matches!(scenario, Scenario::OutpaceSource) {
        // outpace source: healthz, then the minted broadcast descriptor.
        peers::wait_for_ok(client, &format!("{source_base}/healthz"), READY_TIMEOUT)
            .await
            .context("outpace source readiness")?;
        let url = format!("{source_base}/broadcast/{}", compose::BROADCAST_NAME);
        let deadline = std::time::Instant::now() + READY_TIMEOUT;
        loop {
            if let Ok(resp) = client.get(&url).send().await {
                if resp.status().is_success() {
                    return Ok(resp
                        .bytes()
                        .await
                        .context("reading broadcast descriptor")?
                        .to_vec());
                }
            }
            if std::time::Instant::now() >= deadline {
                bail!("outpace source never minted {}", url);
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    } else {
        // engine source: the descriptor lands in the bind-mounted publish dir.
        let path = pub_dir.join(format!("{}.acelive", compose::BROADCAST_NAME));
        let deadline = std::time::Instant::now() + READY_TIMEOUT;
        loop {
            if let Ok(bytes) = std::fs::read(&path) {
                if !bytes.is_empty() {
                    return Ok(bytes);
                }
            }
            if std::time::Instant::now() >= deadline {
                bail!("engine source never produced {}", path.display());
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
}

/// Build the engine + outpace images once, keyed by tag (skip if already present).
async fn ensure_images(config: &Config, engine_dir: &Path) -> Result<()> {
    if !image_exists(ENGINE_IMAGE_TAG).await {
        let dockerfile = crate::asset_path("engine.Dockerfile");
        build_image(ENGINE_IMAGE_TAG, &dockerfile, engine_dir, &[])
            .await
            .context("building engine image")?;
    }
    if !image_exists(OUTPACE_IMAGE_TAG).await {
        let root = &config.workspace_root;
        let dockerfile = root.join("Dockerfile");
        build_image(OUTPACE_IMAGE_TAG, &dockerfile, root, &[])
            .await
            .context("building outpace image")?;
    }
    Ok(())
}

async fn image_exists(tag: &str) -> bool {
    Command::new("docker")
        .args(["image", "inspect", tag])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .await
        .map(|s| s.success())
        .unwrap_or(false)
}

async fn build_image(
    tag: &str,
    dockerfile: &Path,
    context_dir: &Path,
    build_args: &[(&str, &str)],
) -> Result<()> {
    let mut cmd = Command::new("docker");
    cmd.arg("build")
        .arg("-t")
        .arg(tag)
        .arg("-f")
        .arg(dockerfile);
    for (k, v) in build_args {
        cmd.arg("--build-arg").arg(format!("{k}={v}"));
    }
    cmd.arg(context_dir);
    let status = cmd.status().await.context("spawning docker build")?;
    if !status.success() {
        bail!("docker build for {tag} failed ({status})");
    }
    Ok(())
}

async fn compose_up(compose_path: &Path) -> Result<()> {
    let status = Command::new("docker")
        .args(["compose", "-f"])
        .arg(compose_path)
        .args(["up", "-d"])
        .status()
        .await
        .context("spawning docker compose up")?;
    if !status.success() {
        bail!("docker compose up failed ({status})");
    }
    Ok(())
}

async fn compose_down(compose_path: &Path) -> Result<()> {
    let status = Command::new("docker")
        .args(["compose", "-f"])
        .arg(compose_path)
        .args(["down", "-v"])
        .status()
        .await
        .context("spawning docker compose down")?;
    if !status.success() {
        bail!("docker compose down failed ({status})");
    }
    Ok(())
}

async fn dump_logs(compose_path: &Path, scen_dir: &Path) -> Result<()> {
    let out = Command::new("docker")
        .args(["compose", "-f"])
        .arg(compose_path)
        .args(["logs", "--no-color", "--timestamps"])
        .output()
        .await
        .context("spawning docker compose logs")?;
    let path = scen_dir.join("compose-logs.txt");
    std::fs::write(&path, out.stdout).with_context(|| format!("writing {}", path.display()))?;
    Ok(())
}

/// Pre-register the scenario's source peer in the tracker under `infohash_hex`, using the
/// source's known static IP and the peer-wire port for its kind (engine source node 7764,
/// outpace source 8621). Returns the seeded `ip:port`.
fn seed_source(
    tracker: &TrackerHandle,
    infohash_hex: &str,
    scenario: Scenario,
    source_ip: &str,
) -> Result<SocketAddrV4> {
    let bytes = hex::decode(infohash_hex).context("decoding infohash hex")?;
    let infohash: [u8; 20] = bytes
        .as_slice()
        .try_into()
        .map_err(|_| anyhow!("infohash is not 20 bytes: {infohash_hex}"))?;
    let ip: Ipv4Addr = source_ip
        .parse()
        .with_context(|| format!("parsing source ip {source_ip}"))?;
    let port = match scenario {
        Scenario::OutpaceSource => compose::OUTPACE_PEER_PORT,
        Scenario::Baseline | Scenario::Mixed => compose::ENGINE_SOURCE_PEER_PORT,
    };
    let peer = SocketAddrV4::new(ip, port);
    tracker.seed_peer(infohash, peer);
    Ok(peer)
}

fn write_journal(scen_dir: &Path, tracker: &TrackerHandle) {
    let journal = tracker.journal_snapshot();
    if let Ok(json) = serde_json::to_string_pretty(&journal) {
        let _ = std::fs::write(scen_dir.join("tracker-journal.json"), json);
    }
}
