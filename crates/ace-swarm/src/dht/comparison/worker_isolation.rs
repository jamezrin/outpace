//! Local cfg(test) namespace boundary; conformance is qualified with tool workers.
use super::WorkerError;
use serde_json::Value;
use std::net::IpAddr;
const CAP: usize = 16 * 1024;
fn denied<T>() -> Result<T, WorkerError> {
    Err(WorkerError::Namespace)
}
fn loopback(value: &Value, family: Option<bool>) -> bool {
    value
        .as_str()
        .and_then(|s| s.parse::<IpAddr>().ok())
        .is_some_and(|ip| ip.is_loopback() && family.is_none_or(|v6| ip.is_ipv6() == v6))
}
fn destination(value: &Value, v6: bool) -> bool {
    let Some(text) = value.as_str() else {
        return false;
    };
    let (address, prefix) = text
        .split_once('/')
        .map_or((text, None), |(a, p)| (a, Some(p)));
    let Ok(ip) = address.parse::<IpAddr>() else {
        return false;
    };
    if !ip.is_loopback() || ip.is_ipv6() != v6 {
        return false;
    }
    prefix.is_none_or(|p| {
        p.parse::<u8>()
            .is_ok_and(|n| if v6 { n == 128 } else { (8..=32).contains(&n) })
    })
}
/// Validate four bounded `ip -j` replies: links, addresses, IPv4/IPv6 ALL routes.
/// This pure parser is not namespace attestation; the actual gate also requires PID 1.
pub fn validate_snapshot(parts: [&[u8]; 4]) -> Result<(), WorkerError> {
    let mut values = Vec::with_capacity(4);
    for bytes in parts {
        if bytes.len() > CAP {
            return denied();
        }
        let value: Value = serde_json::from_slice(bytes).map_err(|_| WorkerError::Namespace)?;
        if !value.is_array() {
            return denied();
        }
        values.push(value);
    }
    let links = values[0].as_array().ok_or(WorkerError::Namespace)?;
    if links.len() != 1 {
        return denied();
    }
    let link = &links[0];
    let index = link["ifindex"]
        .as_u64()
        .filter(|n| *n > 0)
        .ok_or(WorkerError::Namespace)?;
    if link["ifname"] != "lo" || link["link_type"] != "loopback" || link.get("master").is_some() {
        return denied();
    }
    let flags = link["flags"].as_array().ok_or(WorkerError::Namespace)?;
    if !flags.contains(&Value::from("UP")) || !flags.contains(&Value::from("LOOPBACK")) {
        return denied();
    }
    let addresses = values[1].as_array().ok_or(WorkerError::Namespace)?;
    if addresses.len() != 1
        || addresses[0]["ifname"] != "lo"
        || addresses[0]["ifindex"].as_u64() != Some(index)
    {
        return denied();
    }
    let mut v4 = false;
    for address in addresses[0]["addr_info"]
        .as_array()
        .ok_or(WorkerError::Namespace)?
    {
        let v6 = match address["family"].as_str() {
            Some("inet") => false,
            Some("inet6") => true,
            _ => return denied(),
        };
        if !loopback(&address["local"], Some(v6))
            || address["scope"] != "host"
            || !address["prefixlen"].as_u64().is_some_and(|p| {
                if v6 {
                    p == 128
                } else {
                    (8..=32).contains(&p)
                }
            })
            || address.get("peer").is_some()
            || address
                .get("address")
                .is_some_and(|a| !loopback(a, Some(v6)))
        {
            return denied();
        }
        v4 |= !v6;
    }
    if !v4 {
        return denied();
    }
    for (routes, v6) in [(&values[2], false), (&values[3], true)] {
        for route in routes.as_array().ok_or(WorkerError::Namespace)? {
            let object = route.as_object().ok_or(WorkerError::Namespace)?;
            if object.keys().any(|key| {
                !matches!(
                    key.as_str(),
                    "type"
                        | "dst"
                        | "dev"
                        | "table"
                        | "protocol"
                        | "scope"
                        | "prefsrc"
                        | "src"
                        | "metric"
                        | "flags"
                        | "pref"
                )
            }) || route["dev"] != "lo"
                || !destination(&route["dst"], v6)
                || route
                    .get("type")
                    .is_some_and(|t| !matches!(t.as_str(), Some("local" | "broadcast" | "unicast")))
                || ["prefsrc", "src"]
                    .iter()
                    .any(|k| route.get(k).is_some_and(|a| !loopback(a, Some(v6))))
            {
                return denied();
            }
        }
    }
    Ok(())
}

/// Admit only namespace PID 1, then inspect both address families and ALL tables.
/// The parent namespace owner supplies the outer process deadline/containment.
pub fn attest_offline() -> Result<(), WorkerError> {
    if std::process::id() != 1 {
        return denied();
    }
    #[cfg(target_os = "linux")]
    {
        use std::{
            process::Command,
            time::{Duration, Instant},
        };
        let deadline = Instant::now() + Duration::from_secs(2);
        let mut parts = Vec::with_capacity(4);
        for args in [
            vec!["-j", "link", "show"],
            vec!["-j", "address", "show"],
            vec!["-j", "-4", "route", "show", "table", "all"],
            vec!["-j", "-6", "route", "show", "table", "all"],
        ] {
            let mut command = Command::new("/usr/bin/ip");
            command.env_clear().env("LC_ALL", "C").args(args);
            parts.push(capture(command, deadline)?);
        }
        validate_snapshot([&parts[0], &parts[1], &parts[2], &parts[3]])
    }
    #[cfg(not(target_os = "linux"))]
    denied()
}

#[cfg(target_os = "linux")]
fn capture(
    mut command: std::process::Command,
    deadline: std::time::Instant,
) -> Result<Vec<u8>, WorkerError> {
    use std::{
        io::{ErrorKind, Read},
        os::fd::AsRawFd,
        process::{Child, Stdio},
        time::{Duration, Instant},
    };
    struct Owned(Child);
    impl Drop for Owned {
        fn drop(&mut self) {
            // `ip` is a fixed trusted direct child, not a caller-selected program.
            // Kill and reap even on setup/read failures; the parent bounds OS stalls.
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    if Instant::now() >= deadline {
        return denied();
    }
    let mut child = Owned(
        command
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|_| WorkerError::Namespace)?,
    );
    let mut stdout = child.0.stdout.take().ok_or(WorkerError::Namespace)?;
    let mut stderr = child.0.stderr.take().ok_or(WorkerError::Namespace)?;
    for fd in [stdout.as_raw_fd(), stderr.as_raw_fd()] {
        // SAFETY: both descriptors remain owned/open here; F_GETFL/F_SETFL do not
        // access memory. Preserve existing flags while adding nonblocking reads.
        let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
        if flags < 0 || unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) } < 0 {
            return denied();
        }
    }
    let mut outputs = [Vec::new(), Vec::new()];
    let mut eof = [false; 2];
    loop {
        if Instant::now() >= deadline {
            return denied();
        }
        for (index, pipe) in [&mut stdout as &mut dyn Read, &mut stderr as &mut dyn Read]
            .into_iter()
            .enumerate()
        {
            if eof[index] {
                continue;
            }
            let mut buffer = [0; 4096];
            match pipe.read(&mut buffer) {
                Ok(0) => eof[index] = true,
                Ok(n) => {
                    if outputs[index].len() + n > CAP {
                        return denied();
                    }
                    outputs[index].extend_from_slice(&buffer[..n]);
                }
                Err(error)
                    if matches!(error.kind(), ErrorKind::WouldBlock | ErrorKind::Interrupted) => {}
                Err(_) => return denied(),
            }
        }
        if let Some(status) = child.0.try_wait().map_err(|_| WorkerError::Namespace)? {
            if !status.success() {
                return denied();
            }
            if eof == [true, true] {
                return Ok(std::mem::take(&mut outputs[0]));
            }
        }
        std::thread::sleep(Duration::from_millis(1));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        process::Command,
        time::{Duration, Instant},
    };
    #[test]
    fn inspector_capture_bounds_both_pipes_exit_and_deadline() {
        for script in [
            "import sys;sys.stdout.write('x'*16385)",
            "import sys;sys.stderr.write('x'*16385)",
            "raise SystemExit(4)",
            "import time;time.sleep(30)",
        ] {
            let start = Instant::now();
            let mut command = Command::new("/usr/bin/python3");
            command.args(["-I", "-c", script]);
            assert!(capture(command, start + Duration::from_millis(100)).is_err());
            assert!(start.elapsed() < Duration::from_secs(2));
        }
        let mut command = Command::new("/usr/bin/python3");
        command.args([
            "-I",
            "-c",
            "import sys;sys.stdout.write('[]');sys.stderr.write('note')",
        ]);
        assert_eq!(
            capture(command, Instant::now() + Duration::from_secs(2)).unwrap(),
            b"[]"
        );
    }
    #[test]
    fn local_snapshot_rejects_external_policy_tables_and_missing_link() {
        let links =
            br#"[{"ifindex":1,"ifname":"lo","link_type":"loopback","flags":["UP","LOOPBACK"]}]"#;
        let addresses=br#"[{"ifindex":1,"ifname":"lo","addr_info":[{"family":"inet","local":"127.0.0.1","prefixlen":8,"scope":"host"}]}]"#;
        assert!(validate_snapshot([links, addresses, b"[]", b"[]"]).is_ok());
        assert!(validate_snapshot([b"[]", addresses, b"[]", b"[]"]).is_err());
        for route in [
            br#"[{"dst":"default","dev":"lo","table":123}]"#.as_slice(),
            br#"[{"dst":"192.0.2.0/24","dev":"lo","table":123}]"#.as_slice(),
            br#"[{"dst":"2001:db8::/64","dev":"lo","table":123}]"#.as_slice(),
            br#"[{"dst":"127.0.0.0/8","dev":"lo","gateway":"127.0.0.2"}]"#.as_slice(),
        ] {
            assert!(validate_snapshot([links, addresses, route, b"[]"]).is_err());
            assert!(validate_snapshot([links, addresses, b"[]", route]).is_err());
        }
    }
    #[test]
    fn actual_gate_denies_unconfined_process() {
        assert_ne!(std::process::id(), 1);
        assert_eq!(attest_offline(), Err(WorkerError::Namespace));
    }
}
