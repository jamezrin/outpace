//! `outpace healthcheck`: probe a running daemon's `/healthz` without external tools.
//!
//! Container images (in particular the `scratch`-based minimal image) have no `curl`, so the
//! binary checks itself. The probe is a single blocking HTTP/1.1 request over std TCP: no
//! runtime, no TLS, and no configuration parsing beyond `OUTPACE_BIND`, so a misconfigured
//! environment cannot make the check itself fail differently from the daemon.

use std::io::{Read, Write};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, TcpStream};
use std::time::Duration;

/// The address to probe: an explicit `--addr`, else `OUTPACE_BIND`, else the default bind.
/// A wildcard bind (`0.0.0.0` / `::`) is probed on the loopback address of the same family.
pub fn target(
    explicit: Option<SocketAddr>,
    bind_env: Option<&str>,
    default_bind: SocketAddr,
) -> Result<SocketAddr, String> {
    let addr = match (explicit, bind_env) {
        (Some(addr), _) => addr,
        (None, Some(raw)) => raw
            .parse()
            .map_err(|_| format!("invalid OUTPACE_BIND: {raw}"))?,
        (None, None) => default_bind,
    };
    Ok(match addr.ip() {
        IpAddr::V4(ip) if ip.is_unspecified() => {
            SocketAddr::new(Ipv4Addr::LOCALHOST.into(), addr.port())
        }
        IpAddr::V6(ip) if ip.is_unspecified() => {
            SocketAddr::new(Ipv6Addr::LOCALHOST.into(), addr.port())
        }
        _ => addr,
    })
}

/// GET `/healthz` on `addr`; succeed only on an HTTP 200 status line.
pub fn probe(addr: SocketAddr, timeout: Duration) -> Result<(), String> {
    let mut stream =
        TcpStream::connect_timeout(&addr, timeout).map_err(|e| format!("connect {addr}: {e}"))?;
    stream
        .set_read_timeout(Some(timeout))
        .and_then(|()| stream.set_write_timeout(Some(timeout)))
        .map_err(|e| format!("socket {addr}: {e}"))?;
    let request = format!("GET /healthz HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n");
    stream
        .write_all(request.as_bytes())
        .map_err(|e| format!("send {addr}: {e}"))?;
    // The status line is all that matters; a short bounded read is enough for it.
    let mut head = [0u8; 64];
    let mut len = 0;
    while len < head.len() && !head[..len].contains(&b'\n') {
        match stream.read(&mut head[len..]) {
            Ok(0) => break,
            Ok(n) => len += n,
            Err(e) => return Err(format!("read {addr}: {e}")),
        }
    }
    let line = String::from_utf8_lossy(&head[..len]);
    let status = line.lines().next().unwrap_or_default();
    match status.split_whitespace().nth(1) {
        Some("200") if status.starts_with("HTTP/1.") => Ok(()),
        _ => Err(format!("unhealthy response from {addr}: {status:?}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::TcpListener;

    const DEFAULT: &str = "127.0.0.1:6878";

    fn default_bind() -> SocketAddr {
        DEFAULT.parse().unwrap()
    }

    #[test]
    fn target_prefers_explicit_then_env_then_default() {
        let explicit: SocketAddr = "127.0.0.2:1".parse().unwrap();
        assert_eq!(
            target(Some(explicit), Some("127.0.0.3:2"), default_bind()),
            Ok(explicit)
        );
        assert_eq!(
            target(None, Some("127.0.0.3:2"), default_bind()),
            Ok("127.0.0.3:2".parse().unwrap())
        );
        assert_eq!(target(None, None, default_bind()), Ok(default_bind()));
    }

    #[test]
    fn wildcard_bind_probes_loopback_of_same_family() {
        assert_eq!(
            target(None, Some("0.0.0.0:6878"), default_bind()),
            Ok("127.0.0.1:6878".parse().unwrap())
        );
        assert_eq!(
            target(None, Some("[::]:6878"), default_bind()),
            Ok("[::1]:6878".parse().unwrap())
        );
    }

    #[test]
    fn invalid_env_bind_is_an_error() {
        assert!(target(None, Some("not-an-address"), default_bind()).is_err());
    }

    fn serve_once(response: &'static [u8]) -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = [0u8; 256];
            let _ = stream.read(&mut request);
            let _ = stream.write_all(response);
        });
        addr
    }

    #[test]
    fn probe_accepts_200() {
        let addr = serve_once(b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nok");
        assert_eq!(probe(addr, Duration::from_secs(2)), Ok(()));
    }

    #[test]
    fn probe_rejects_non_200() {
        let addr = serve_once(b"HTTP/1.1 503 Service Unavailable\r\n\r\n");
        assert!(probe(addr, Duration::from_secs(2)).is_err());
    }

    #[test]
    fn probe_rejects_closed_port() {
        let addr = TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap();
        assert!(probe(addr, Duration::from_secs(2)).is_err());
    }
}
