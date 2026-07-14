//! Scenario reporting: per-peer verdict rows, a stdout table, and a `report.json`.
//!
//! [`ScenarioReport`] aggregates the tested verdicts from [`crate::assertions`] for one
//! scenario; [`overall_exit_code`] maps a batch of reports to the process exit contract
//! (0 all-pass / 1 any-fail / 2 preflight-skip).

use std::path::Path;

use anyhow::{Context, Result};
use serde::Serialize;

use crate::assertions::{HealthVerdict, StabilityVerdict};

/// Verdict rows for a single peer in a scenario.
#[derive(Debug, Clone, Serialize)]
pub struct PeerReport {
    /// Service name, e.g. `engine-consumer-1`.
    pub name: String,
    /// Role label, e.g. `engine-consumer` / `outpace-consumer`.
    pub role: String,
    /// Swarm-health verdict over the sampled stats series.
    pub health: HealthVerdict,
    /// Stream-stability verdict over the per-second byte buckets.
    pub stability: StabilityVerdict,
    /// Whether the captured playback head was MPEG-TS packet-aligned.
    pub ts_contiguous: bool,
    /// The last raw stat JSON captured (for human audit).
    pub last_raw_stat: serde_json::Value,
    /// All checks passed for this peer.
    pub passed: bool,
}

impl PeerReport {
    /// Combine the three verdicts into a per-peer pass/fail.
    pub fn finalize(
        name: String,
        role: String,
        health: HealthVerdict,
        stability: StabilityVerdict,
        ts_contiguous: bool,
        last_raw_stat: serde_json::Value,
    ) -> Self {
        let passed = health.passed && stability.passed && ts_contiguous;
        Self {
            name,
            role,
            health,
            stability,
            ts_contiguous,
            last_raw_stat,
            passed,
        }
    }
}

/// The aggregated result of running one scenario.
#[derive(Debug, Clone, Serialize)]
pub struct ScenarioReport {
    /// Scenario key (`baseline` / `mixed` / `outpace-source`).
    pub scenario: String,
    /// Per-peer verdicts.
    pub peers: Vec<PeerReport>,
    /// Free-form notes (infra errors, log-dump locations, tracker journal size).
    pub notes: Vec<String>,
    /// Whether at least one peer uploaded (swarm-level seeding proof; see
    /// [`crate::assertions::swarm_reciprocates`]).
    pub reciprocation_ok: bool,
    /// Overall pass: at least one peer, every peer passed, and the swarm reciprocated.
    pub passed: bool,
}

impl ScenarioReport {
    /// Build a report and compute the overall verdict from the peer rows plus the
    /// swarm-level reciprocation check.
    pub fn new(scenario: &str, peers: Vec<PeerReport>, mut notes: Vec<String>) -> Self {
        let verdicts: Vec<_> = peers.iter().map(|p| p.health.clone()).collect();
        let reciprocation_ok = crate::assertions::swarm_reciprocates(&verdicts);
        let all_peers_pass = !peers.is_empty() && peers.iter().all(|p| p.passed);
        notes.push(format!(
            "swarm reciprocation (>=1 peer uploaded): {}",
            if reciprocation_ok { "yes" } else { "NO" }
        ));
        Self {
            scenario: scenario.to_string(),
            peers,
            notes,
            reciprocation_ok,
            passed: all_peers_pass && reciprocation_ok,
        }
    }

    /// A report for a scenario that failed to run at all (infra/orchestration error).
    pub fn errored(scenario: &str, error: impl std::fmt::Display) -> Self {
        Self {
            scenario: scenario.to_string(),
            peers: vec![],
            notes: vec![format!("scenario failed to run: {error}")],
            reciprocation_ok: false,
            passed: false,
        }
    }
}

/// Render a batch of reports as a readable table to stdout.
pub fn render_table(reports: &[ScenarioReport]) {
    for r in reports {
        println!();
        println!(
            "=== scenario {} : {} ===",
            r.scenario,
            if r.passed { "PASS" } else { "FAIL" }
        );
        if r.peers.is_empty() {
            println!("  (no peer results)");
        } else {
            println!(
                "  {:<20} {:<18} {:<6} {:<8} {:<7} {:<5} {:<5} verdict",
                "peer", "role", "health", "stable", "dl%", "ts", "up"
            );
            for p in &r.peers {
                println!(
                    "  {:<20} {:<18} {:<6} {:<8} {:<7} {:<5} {:<5} {}",
                    p.name,
                    p.role,
                    yn(p.health.passed),
                    yn(p.stability.passed),
                    format!("{:.0}", p.health.dl_ratio * 100.0),
                    yn(p.ts_contiguous),
                    // upload is informational (seeding is judged at the swarm level).
                    yn(p.health.upload_positive),
                    if p.passed { "PASS" } else { "FAIL" }
                );
            }
        }
        for note in &r.notes {
            println!("  note: {note}");
        }
    }
    println!();
    let passed = reports.iter().filter(|r| r.passed).count();
    println!("summary: {passed}/{} scenarios passed", reports.len());
}

fn yn(b: bool) -> &'static str {
    if b {
        "ok"
    } else {
        "FAIL"
    }
}

/// Write the reports as pretty JSON to `<dir>/report.json`.
pub fn write_json(dir: &Path, reports: &[ScenarioReport]) -> Result<()> {
    let path = dir.join("report.json");
    let json = serde_json::to_string_pretty(reports).context("serializing report.json")?;
    std::fs::write(&path, json).with_context(|| format!("writing {}", path.display()))?;
    Ok(())
}

/// Process exit code: 0 all-pass, 1 any-fail. Preflight-skip (exit 2) is handled by the
/// caller before scenarios run, not here.
pub fn overall_exit_code(reports: &[ScenarioReport]) -> i32 {
    if !reports.is_empty() && reports.iter().all(|r| r.passed) {
        0
    } else {
        1
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::assertions::{stream_stability, swarm_health, PeerStats};

    fn passing_peer(name: &str) -> PeerReport {
        let samples: Vec<_> = (0..8)
            .map(|i| PeerStats::new("dl", 2, 1000 * (i + 1), 500 * (i + 1)))
            .collect();
        let health = swarm_health(&samples, 2);
        let stability = stream_stability(&[175_000u64; 20], 175_000);
        PeerReport::finalize(
            name.into(),
            "engine-consumer".into(),
            health,
            stability,
            true,
            serde_json::json!({ "status": "dl" }),
        )
    }

    fn failing_peer(name: &str) -> PeerReport {
        let samples: Vec<_> = (0..8).map(|_| PeerStats::new("idle", 0, 10, 0)).collect();
        let health = swarm_health(&samples, 2);
        let stability = stream_stability(&[0u64; 20], 175_000);
        PeerReport::finalize(
            name.into(),
            "outpace-consumer".into(),
            health,
            stability,
            false,
            serde_json::Value::Null,
        )
    }

    #[test]
    fn all_pass_scenario_passes() {
        let r = ScenarioReport::new(
            "baseline",
            vec![passing_peer("e1"), passing_peer("e2")],
            vec![],
        );
        assert!(r.passed);
    }

    #[test]
    fn one_failing_peer_fails_scenario() {
        let r = ScenarioReport::new(
            "mixed",
            vec![passing_peer("e1"), failing_peer("o1")],
            vec![],
        );
        assert!(!r.passed);
    }

    #[test]
    fn empty_scenario_does_not_pass() {
        let r = ScenarioReport::new("baseline", vec![], vec![]);
        assert!(!r.passed);
    }

    #[test]
    fn errored_scenario_is_failed_with_note() {
        let r = ScenarioReport::errored("baseline", "docker exploded");
        assert!(!r.passed);
        assert!(r.notes[0].contains("docker exploded"));
    }

    #[test]
    fn exit_code_contract() {
        let all_pass = vec![ScenarioReport::new(
            "baseline",
            vec![passing_peer("e1")],
            vec![],
        )];
        assert_eq!(overall_exit_code(&all_pass), 0);
        let any_fail = vec![
            ScenarioReport::new("baseline", vec![passing_peer("e1")], vec![]),
            ScenarioReport::errored("mixed", "boom"),
        ];
        assert_eq!(overall_exit_code(&any_fail), 1);
        assert_eq!(overall_exit_code(&[]), 1);
    }

    #[test]
    fn write_json_emits_file() {
        let tmp = tempfile::tempdir().unwrap();
        let reports = vec![ScenarioReport::new(
            "baseline",
            vec![passing_peer("e1")],
            vec![],
        )];
        write_json(tmp.path(), &reports).unwrap();
        let written = std::fs::read_to_string(tmp.path().join("report.json")).unwrap();
        assert!(written.contains("\"scenario\": \"baseline\""));
        assert!(written.contains("\"passed\": true"));
    }
}
