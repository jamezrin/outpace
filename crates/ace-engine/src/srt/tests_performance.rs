//! Fixed local comparative profile, not a throughput/peak-memory/readiness gate.
use super::{
    config::SrtConfig,
    tests_loopback::{bounded_command, Artifacts, OwnedChild},
    SrtService,
};
use crate::{
    broadcast::{BroadcastRegistry, ProducerKind},
    http::BroadcastState,
};
use ace_swarm::{listen::SeedRegistry, peer_policy::PeerPolicy};
use bytes::Bytes;
use serde::Serialize;
use std::{
    fs::OpenOptions,
    process::{Command, Stdio},
    time::{Duration, Instant},
};
use tokio::sync::watch;

#[derive(Serialize)]
struct Sample {
    cpu_us: u64,
    rss_bytes: u64,
    threads: Vec<String>,
}
fn sample() -> Result<Sample, String> {
    let mut usage: libc::rusage = unsafe { std::mem::zeroed() };
    if unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut usage) } != 0 {
        return Err("getrusage failed".into());
    }
    let micros = |time: libc::timeval| -> Result<u64, String> {
        let seconds = u64::try_from(time.tv_sec).map_err(|_| "negative CPU seconds")?;
        let micros = u64::try_from(time.tv_usec).map_err(|_| "negative CPU micros")?;
        seconds
            .checked_mul(1_000_000)
            .and_then(|v| v.checked_add(micros))
            .ok_or("CPU overflow".into())
    };
    let cpu_us = micros(usage.ru_utime)?
        .checked_add(micros(usage.ru_stime)?)
        .ok_or("CPU overflow")?;
    let status = std::fs::read_to_string("/proc/self/status").map_err(|e| e.to_string())?;
    let rss = status
        .lines()
        .find_map(|line| line.strip_prefix("VmRSS:"))
        .ok_or("RSS unavailable")?;
    let kib = rss
        .split_whitespace()
        .next()
        .ok_or("RSS malformed")?
        .parse::<u64>()
        .map_err(|e| e.to_string())?;
    let rss_bytes = kib.checked_mul(1024).ok_or("RSS overflow")?;
    let mut threads = Vec::new();
    for entry in std::fs::read_dir("/proc/self/task")
        .map_err(|e| e.to_string())?
        .take(257)
    {
        if threads.len() >= 256 {
            return Err("thread observation limit".into());
        }
        let entry = entry.map_err(|e| e.to_string())?;
        match std::fs::read_to_string(entry.path().join("comm")) {
            Ok(name) => threads.push(name.trim().to_owned()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.to_string()),
        }
    }
    threads.sort();
    Ok(Sample {
        cpu_us,
        rss_bytes,
        threads,
    })
}
async fn generated_input(artifacts: &Artifacts) -> Result<Bytes, String> {
    let mut command = Command::new("ffmpeg");
    command
        .args([
            "-hide_banner",
            "-loglevel",
            "error",
            "-f",
            "lavfi",
            "-i",
            "testsrc=size=160x90:rate=25",
            "-f",
            "lavfi",
            "-i",
            "sine=frequency=440:sample_rate=48000",
            "-t",
            "3",
            "-c:v",
            "libx264",
            "-threads",
            "1",
            "-preset",
            "ultrafast",
            "-tune",
            "zerolatency",
            "-pix_fmt",
            "yuv420p",
            "-g",
            "12",
            "-c:a",
            "aac",
            "-b:a",
            "64k",
            "-muxrate",
            "1000000",
            "-f",
            "mpegts",
        ])
        .arg(artifacts.path("input.ts"))
        .stdout(Stdio::null())
        .stderr(artifacts.file("generate.log"));
    bounded_command(&mut command);
    let mut child = OwnedChild(command.spawn().map_err(|e| e.to_string())?);
    let status = tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            if let Some(status) = child.0.try_wait().map_err(|e| e.to_string())? {
                return Ok::<_, String>(status);
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .map_err(|_| "input generation deadline")??;
    if !status.success() {
        return Err(format!("input generation exited {status}"));
    }
    let input = std::fs::read(artifacts.path("input.ts")).map_err(|e| e.to_string())?;
    if input.len() < 3 * 65536 || input.len() > 1 << 20 || !input.len().is_multiple_of(188) {
        return Err("unexpected generated corpus size".into());
    }
    Ok(Bytes::from(input))
}

#[tokio::test]
#[ignore = "explicit one/four-stream release comparison; requires SRT FFmpeg opt-in and private report path"]
async fn fixed_one_or_four_stream_resource_profile() {
    use std::os::unix::fs::OpenOptionsExt;
    assert_eq!(std::env::var("OUTPACE_TEST_SRT_FFMPEG").as_deref(), Ok("1"));
    let mode = std::env::var("OUTPACE_TEST_SRT_PERF_MODE").unwrap();
    assert!(matches!(mode.as_str(), "core" | "srt"));
    let count = std::env::var("OUTPACE_TEST_SRT_PERF_COUNT")
        .unwrap()
        .parse::<usize>()
        .unwrap();
    assert!(matches!(count, 1 | 4));
    let output = std::env::var_os("OUTPACE_TEST_SRT_PERF_REPORT").unwrap();
    let artifacts = Artifacts::new();
    let input = generated_input(&artifacts).await.unwrap();
    std::io::Write::write_all(&mut artifacts.file("key"), b"synthetic-loopback-passphrase")
        .unwrap();
    let registry = BroadcastRegistry::new();
    let state = BroadcastState {
        peer_policy: PeerPolicy::from_private_peers(Some("127.0.0.1:1")).unwrap(),
        registry: registry.clone(),
        seed_registry: SeedRegistry::new(),
        trackers: vec![],
        store_bytes: 4 << 20,
        inbound_peer_port: watch::channel(None).1,
    };
    let before = sample().unwrap();
    let clock = Instant::now();
    let mut service = if mode == "srt" {
        let prepared = SrtConfig {
            bind: Some("127.0.0.1:0".parse().unwrap()),
            max_connections: count,
            passphrase_file: Some(artifacts.path("key")),
            ..SrtConfig::default()
        }
        .prepare(true)
        .unwrap();
        SrtService::start(prepared, state.clone()).await.unwrap()
    } else {
        None
    };
    let mut children = Vec::new();
    let mut core = tokio::task::JoinSet::new();
    let mut starts = Vec::new();
    let names = (0..count)
        .map(|i| format!("synthetic-perf-{i}"))
        .collect::<Vec<_>>();
    for (index, name) in names.iter().enumerate() {
        starts.push(Instant::now());
        if let Some(service) = &service {
            let url = format!("srt://{}?mode=caller&streamid={name}&passphrase=synthetic-loopback-passphrase&pkt_size=1316&connect_timeout=1000", service.local_addr().unwrap());
            let mut command = Command::new("ffmpeg");
            command
                .args([
                    "-hide_banner",
                    "-loglevel",
                    "error",
                    "-re",
                    "-protocol_whitelist",
                    "file,srt,udp",
                    "-i",
                ])
                .arg(artifacts.path("input.ts"))
                .args(["-c", "copy", "-muxrate", "1000000", "-f", "mpegts", &url])
                .stdout(Stdio::null())
                .stderr(artifacts.file(&format!("publisher-{index}.log")));
            bounded_command(&mut command);
            children.push(OwnedChild(command.spawn().unwrap()));
        } else {
            let state = state.clone();
            let input = input.clone();
            let name = name.clone();
            core.spawn(async move {
                let (_, mut producer) = state
                    .acquire_producer(&name, ProducerKind::RawPut)
                    .await
                    .map_err(|e| e.code().to_owned())?;
                let mut ingest = producer.open_ingest().map_err(|e| e.to_string())?;
                let start = tokio::time::Instant::now();
                for offset in (0..input.len()).step_by(1316) {
                    tokio::time::sleep_until(
                        start + Duration::from_secs_f64(offset as f64 / 125_000.0),
                    )
                    .await;
                    ingest
                        .push_bytes(&input[offset..(offset + 1316).min(input.len())])
                        .await
                        .map_err(|e| e.to_string())?;
                }
                Ok::<_, String>(())
            });
        }
    }
    let mut first_piece_ms = vec![None; count];
    let observed = tokio::time::timeout(Duration::from_secs(7), async {
        loop {
            let mut pieces = Vec::new();
            for (index, name) in names.iter().enumerate() {
                let Some(broadcast) = registry.get(name).await else {
                    pieces.push(0);
                    continue;
                };
                let store = broadcast.store.lock().await;
                if store.has_piece(0) && first_piece_ms[index].is_none() {
                    first_piece_ms[index] = Some(starts[index].elapsed().as_secs_f64() * 1000.0);
                }
                let completed = store
                    .window()
                    .map(|(first, last)| {
                        (first..=last)
                            .filter(|piece| store.has_piece(*piece))
                            .count()
                    })
                    .unwrap_or(0);
                pieces.push(completed);
            }
            if pieces.iter().all(|count| *count >= 3) {
                return (sample(), pieces, clock.elapsed().as_secs_f64());
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
    children.clear();
    core.abort_all();
    while core.join_next().await.is_some() {}
    let native_cleanup = if let Some(service) = &mut service {
        service.shutdown().await
    } else {
        Ok(())
    };
    let registry_cleanup = registry.shutdown().await;
    let cleanup_seconds = clock.elapsed().as_secs_f64();
    assert!(native_cleanup.is_ok() && registry_cleanup.is_ok());
    let (steady, pieces, elapsed) = observed.unwrap_or_else(|error| {
        panic!(
            "profile deadline {error}; artifacts={}",
            artifacts.0.display()
        )
    });
    let steady = steady.unwrap();
    let after = sample().unwrap();
    let report = serde_json::json!({
        "mode":mode,"publishers":count,"profile":if cfg!(debug_assertions) {"debug"} else {"release"},
        "scope":"library test process using System allocator; core baseline excludes HTTP transport; SRT latency includes FFmpeg launch; CPU excludes children; snapshots are not peaks",
        "input_bytes":input.len(),"nominal_input_bytes_per_second":125000,"poll_ms":5,
        "observed_seconds":elapsed,"cleanup_finished_seconds":cleanup_seconds,
        "launch_to_observed_first_piece_ms":first_piece_ms,"complete_piece_observations":pieces,
        "before":before,"steady":steady,"after_cleanup":after,
        "cleanup_confirmed":true
    });
    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(output)
        .unwrap();
    serde_json::to_writer_pretty(file, &report).unwrap();
}
