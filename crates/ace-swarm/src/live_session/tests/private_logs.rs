//! Capture a bounded standalone real follower run; do not redirect process-global test stderr.
use std::{
    io::Read,
    process::{Command, Stdio},
    time::{Duration, Instant},
};

fn capture(case: &str) -> (bool, Vec<u8>, Vec<u8>) {
    fn drain(mut pipe: impl Read + Send + 'static) -> std::thread::JoinHandle<(Vec<u8>, usize)> {
        std::thread::spawn(move || {
            let mut kept = Vec::new();
            let mut total = 0usize;
            let mut block = [0; 4096];
            loop {
                let count = pipe.read(&mut block).unwrap();
                if count == 0 {
                    break;
                }
                total = total.saturating_add(count);
                kept.extend_from_slice(
                    &block[..count.min((64 * 1024usize).saturating_sub(kept.len()))],
                );
            }
            (kept, total)
        })
    }
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", case, "--nocapture", "--test-threads=1"])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let out = drain(child.stdout.take().unwrap());
    let err = drain(child.stderr.take().unwrap());
    let deadline = Instant::now() + Duration::from_secs(15);
    let (status, timed_out) = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break (status, false);
        }
        if Instant::now() >= deadline {
            child.kill().unwrap();
            break (child.wait().unwrap(), true);
        }
        std::thread::sleep(Duration::from_millis(5));
    };
    let (stdout, out_count) = out.join().unwrap();
    let (stderr, err_count) = err.join().unwrap();
    assert!(!timed_out, "bounded follower child timed out");
    assert_eq!(stdout.len(), out_count, "stdout capture truncated");
    assert_eq!(stderr.len(), err_count, "stderr capture truncated");
    (status.success(), stdout, stderr)
}

#[test]
fn private_follower_logs_hide_endpoints_while_public_control_remains_visible() {
    for (private, case) in [
        (true, "live_session::tests::support_lifecycle::private_follower_uses_allowlist_without_public_discovery_or_warm_cache"),
        (false, "live_session::tests::support_lifecycle::signed_support_reseeds_without_playback_and_releases_its_producer"),
    ] {
        let (ok, stdout, stderr) = capture(case);
        assert!(ok, "follower child failed: {} {}", String::from_utf8_lossy(&stdout), String::from_utf8_lossy(&stderr));
        let logs = String::from_utf8(stderr).unwrap();
        assert!(logs.contains("discovery stage=first-source-output"), "missing real output witness");
        if private { assert!(!logs.contains("127.0.0.1"), "private endpoint disclosed: {logs}"); }
        else { assert!(logs.contains("127.0.0.1"), "public endpoint control disappeared"); }
    }
}

#[test]
fn private_gap_log_fixture() {
    use super::*;
    let policy =
        crate::peer_policy::PeerPolicy::from_private_peers(Some("127.0.0.1:1234")).unwrap();
    let (mut continuity, start) = Continuity::fresh(
        &info(),
        10,
        20,
        PREFETCH_PIECES,
        LiveRecoveryConfig::default(),
    );
    assert!(start < 100);
    assert_eq!(
        continuity.resume_with_policy("127.0.0.1:1234".parse().unwrap(), 100, 120, &policy),
        100
    );
}

#[test]
fn private_reconnect_gap_logs_hide_endpoint() {
    let (ok, stdout, stderr) =
        capture("live_session::tests::private_logs::private_gap_log_fixture");
    assert!(ok, "child failed: {:?}", stdout);
    let logs = String::from_utf8(stderr).unwrap();
    assert!(logs.contains("reconnect gap"));
    assert!(
        !logs.contains("127.0.0.1"),
        "private endpoint disclosed: {logs}"
    );
}
