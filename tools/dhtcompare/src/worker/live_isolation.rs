//! Fail-closed live-worker gate over per-namespace procfs only.
//!
//! The supervisor attests the inner nftables policy and enforces destinations in
//! its relay. This gate denies execution unless the process is unprivileged inside
//! the fixed veth topology, so an unconfined invocation cannot reach the network.
use super::WorkerError;
use std::net::Ipv4Addr;

const CAP: usize = 16 * 1024;
/// Mirrors `live_policy.DENIED_V4`; a Python test keeps both lists identical.
pub const DENIED_V4: [&str; 15] = [
    "0.0.0.0/8",
    "10.0.0.0/8",
    "100.64.0.0/10",
    "127.0.0.0/8",
    "169.254.0.0/16",
    "172.16.0.0/12",
    "192.0.0.0/24",
    "192.0.2.0/24",
    "192.88.99.0/24",
    "192.168.0.0/16",
    "198.18.0.0/15",
    "198.51.100.0/24",
    "203.0.113.0/24",
    "224.0.0.0/4",
    "240.0.0.0/4",
];
const ROUTES: [(&str, &str, &str, &str); 2] = [
    ("dhtw0", "00000000", "0100C80A", "00000000"),
    ("dhtw0", "0000C80A", "00000000", "FCFFFFFF"),
];

/// Whether an IPv4 address lies outside every denied special-purpose prefix.
pub fn permitted(ip: Ipv4Addr) -> bool {
    let value = u32::from(ip);
    !DENIED_V4.iter().any(|prefix| {
        let (base, length) = prefix.split_once('/').expect("fixed prefix");
        let base = u32::from(base.parse::<Ipv4Addr>().expect("fixed prefix"));
        let mask = u32::MAX.checked_shl(32 - length.parse::<u32>().expect("fixed prefix"));
        value & mask.unwrap_or(0) == base
    })
}

fn denied<T>() -> Result<T, WorkerError> {
    Err(WorkerError::Namespace)
}

/// Validate bounded procfs views: process status, interfaces, IPv4 main routes,
/// IPv6 addresses and the IPv6 disable switch. Pure; reads nothing itself.
/// PID-namespace ownership is verified by the supervisor, not from inside.
pub fn validate_live(
    status: &str,
    dev: &str,
    route: &str,
    inet6: &str,
    ipv6_disabled: &str,
) -> Result<(), WorkerError> {
    let mut seen = 0;
    for line in status.lines() {
        let Some((key, value)) = line.split_once(':') else {
            continue;
        };
        let value = value.trim();
        match key {
            "CapInh" | "CapPrm" | "CapEff" | "CapBnd" | "CapAmb" => {
                if value != "0000000000000000" {
                    return denied();
                }
                seen += 1;
            }
            "NoNewPrivs" => {
                if value != "1" {
                    return denied();
                }
                seen += 1;
            }
            _ => {}
        }
    }
    if seen != 6 {
        return denied();
    }
    let mut names: Vec<&str> = dev
        .lines()
        .skip(2)
        .map(|line| line.split(':').next().unwrap_or("").trim())
        .collect();
    names.sort_unstable();
    if names != ["dhtr0", "dhtw0", "lo"] {
        return denied();
    }
    let mut routes: Vec<(&str, &str, &str, &str)> = Vec::new();
    for line in route.lines().skip(1) {
        let fields: Vec<&str> = line.split_whitespace().collect();
        if fields.len() != 11 {
            return denied();
        }
        routes.push((fields[0], fields[1], fields[2], fields[7]));
    }
    routes.sort_unstable();
    if routes != ROUTES || !inet6.trim().is_empty() || ipv6_disabled.trim() != "1" {
        return denied();
    }
    Ok(())
}

/// Read the current process's namespace views and apply [`validate_live`].
pub fn attest_live() -> Result<(), WorkerError> {
    let mut parts = Vec::with_capacity(5);
    for path in [
        "/proc/self/status",
        "/proc/self/net/dev",
        "/proc/self/net/route",
        "/proc/self/net/if_inet6",
        "/proc/sys/net/ipv6/conf/all/disable_ipv6",
    ] {
        let text = match std::fs::read_to_string(path) {
            Ok(text) => text,
            Err(error)
                if path.ends_with("if_inet6") && error.kind() == std::io::ErrorKind::NotFound =>
            {
                String::new()
            }
            Err(_) => return denied(),
        };
        if text.len() > CAP {
            return denied();
        }
        parts.push(text);
    }
    validate_live(&parts[0], &parts[1], &parts[2], &parts[3], &parts[4])
}

#[cfg(test)]
mod tests {
    use super::*;
    const STATUS: &str = "Name:\tworker\nNSpid:\t812\t2\nCapInh:\t0000000000000000\nCapPrm:\t0000000000000000\nCapEff:\t0000000000000000\nCapBnd:\t0000000000000000\nCapAmb:\t0000000000000000\nNoNewPrivs:\t1\n";
    const DEV: &str = "Inter-|   Receive\n face |bytes\n    lo: 0 0\n dhtr0: 0 0\n dhtw0: 0 0\n";
    const ROUTE: &str = "Iface\tDestination\tGateway \tFlags\tRefCnt\tUse\tMetric\tMask\t\tMTU\tWindow\tIRTT\ndhtw0\t00000000\t0100C80A\t0003\t0\t0\t0\t00000000\t0\t0\t0\ndhtw0\t0000C80A\t00000000\t0001\t0\t0\t0\tFCFFFFFF\t0\t0\t0\n";

    #[test]
    fn accepts_only_the_fixed_unprivileged_topology() {
        assert!(validate_live(STATUS, DEV, ROUTE, "", "1\n").is_ok());
        let cases = [
            (
                STATUS.replace("CapEff:\t0000000000000000", "CapEff:\t000001ffffffffff"),
                DEV.to_owned(),
                ROUTE.to_owned(),
                "",
                "1",
            ),
            (
                STATUS.replace("CapBnd:\t0000000000000000", "CapBnd:\t0000000000000400"),
                DEV.to_owned(),
                ROUTE.to_owned(),
                "",
                "1",
            ),
            (
                STATUS.replace("NoNewPrivs:\t1", "NoNewPrivs:\t0"),
                DEV.to_owned(),
                ROUTE.to_owned(),
                "",
                "1",
            ),
            (
                STATUS.replace("CapAmb:\t0000000000000000\n", ""),
                DEV.to_owned(),
                ROUTE.to_owned(),
                "",
                "1",
            ),
            (
                STATUS.to_owned(),
                format!("{DEV}  eth0: 0 0\n"),
                ROUTE.to_owned(),
                "",
                "1",
            ),
            (
                STATUS.to_owned(),
                DEV.replace(" dhtr0: 0 0\n", ""),
                ROUTE.to_owned(),
                "",
                "1",
            ),
            (
                STATUS.to_owned(),
                DEV.to_owned(),
                ROUTE.replace("0100C80A", "0101A8C0"),
                "",
                "1",
            ),
            (
                STATUS.to_owned(),
                DEV.to_owned(),
                format!("{ROUTE}eth0\t0001A8C0\t00000000\t0001\t0\t0\t0\t00FFFFFF\t0\t0\t0\n"),
                "",
                "1",
            ),
            (
                STATUS.to_owned(),
                DEV.to_owned(),
                ROUTE.to_owned(),
                "00000000000000000000000000000001 01 80 10 80       lo",
                "1",
            ),
            (STATUS.to_owned(), DEV.to_owned(), ROUTE.to_owned(), "", "0"),
        ];
        for (status, dev, route, inet6, disabled) in cases {
            assert_eq!(
                validate_live(&status, &dev, &route, inet6, disabled),
                Err(WorkerError::Namespace)
            );
        }
    }

    #[test]
    fn permits_only_global_unicast() {
        for ip in ["11.0.0.2", "1.1.1.1", "45.1.2.3"] {
            assert!(permitted(ip.parse().unwrap()), "{ip}");
        }
        for ip in [
            "10.0.0.5",
            "127.0.0.1",
            "192.168.1.9",
            "100.64.0.1",
            "169.254.0.1",
            "172.31.0.1",
            "224.0.0.1",
            "255.255.255.255",
            "0.1.2.3",
            "198.19.0.1",
        ] {
            assert!(!permitted(ip.parse().unwrap()), "{ip}");
        }
    }

    #[test]
    fn actual_gate_denies_unconfined_process() {
        assert_eq!(attest_live(), Err(WorkerError::Namespace));
    }
}
