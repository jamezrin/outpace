//! One bounded SSDP `M-SEARCH` for media renderers.
//!
//! The search runs only when a caller asks for targets: one UDP socket, two copies of the
//! request (UDP is lossy), a fixed listening window, and caps on replies read and distinct
//! description URLs kept. A reply is kept only when its `LOCATION` names the very address the
//! datagram came from, and that address is private or link-local IPv4.

use super::url::{is_lan_ipv4, LanUrl};
use super::CastError;
use std::net::{IpAddr, Ipv4Addr, SocketAddr, SocketAddrV4};
use std::time::Duration;

pub const MEDIA_RENDERER: &str = "urn:schemas-upnp-org:device:MediaRenderer:1";
const SSDP_ADDR: SocketAddrV4 = SocketAddrV4::new(Ipv4Addr::new(239, 255, 255, 250), 1900);
/// Datagrams are read into this buffer; anything longer is truncated by the OS and rejected.
const MAX_DATAGRAM: usize = 2048;

/// Bounds for one search.
#[derive(Clone, Copy, Debug)]
pub struct SearchLimits {
    pub window: Duration,
    pub max_replies: usize,
    pub max_locations: usize,
}

/// The `M-SEARCH` request for `target` with an `MX` of `mx` seconds.
pub fn m_search(target: &str, mx: u8) -> String {
    format!(
        "M-SEARCH * HTTP/1.1\r\nHOST: 239.255.255.250:1900\r\nMAN: \"ssdp:discover\"\r\nMX: {mx}\r\nST: {target}\r\nUSER-AGENT: outpace/{} UPnP/1.1 DLNADOC/1.50\r\n\r\n",
        env!("CARGO_PKG_VERSION")
    )
}

/// Validate one search reply from `source`, returning its description URL.
pub fn parse_reply(datagram: &[u8], source: SocketAddr) -> Result<LanUrl, CastError> {
    if datagram.len() >= MAX_DATAGRAM {
        return Err(CastError::TooLarge);
    }
    let IpAddr::V4(source_ip) = source.ip() else {
        return Err(CastError::Forbidden("ssdp reply is not ipv4"));
    };
    if !is_lan_ipv4(source_ip) {
        return Err(CastError::Forbidden("ssdp reply from a non-lan address"));
    }
    let text = std::str::from_utf8(datagram).map_err(|_| CastError::Malformed("bad ssdp"))?;
    let mut lines = text.split("\r\n");
    let status = lines.next().unwrap_or_default();
    let mut status_parts = status.split_whitespace();
    if !status_parts
        .next()
        .is_some_and(|v| v.eq_ignore_ascii_case("HTTP/1.1") || v.eq_ignore_ascii_case("HTTP/1.0"))
        || status_parts.next() != Some("200")
    {
        return Err(CastError::Malformed("ssdp reply is not 200"));
    }
    let mut location = None;
    for line in lines.take(32) {
        if let Some((name, value)) = line.split_once(':') {
            if name.trim().eq_ignore_ascii_case("location") {
                if location.is_some() {
                    return Err(CastError::Malformed("duplicate ssdp location"));
                }
                location = Some(value.trim());
            }
        }
    }
    let url = LanUrl::parse(location.ok_or(CastError::Malformed("ssdp reply has no location"))?)?;
    if url.ip() != source_ip {
        return Err(CastError::Forbidden(
            "ssdp location host differs from replier",
        ));
    }
    Ok(url)
}

/// Send the search from `bind_ip` and collect distinct description URLs until the window ends.
pub async fn search(bind_ip: Ipv4Addr, limits: SearchLimits) -> Result<Vec<LanUrl>, CastError> {
    let socket = tokio::net::UdpSocket::bind(SocketAddrV4::new(bind_ip, 0))
        .await
        .map_err(|_| CastError::Discovery)?;
    let _ = socket.set_multicast_ttl_v4(2);
    let mx = limits.window.as_secs().clamp(1, 5) as u8;
    let request = m_search(MEDIA_RENDERER, mx);
    for _ in 0..2 {
        socket
            .send_to(request.as_bytes(), SSDP_ADDR)
            .await
            .map_err(|_| CastError::Discovery)?;
    }
    let deadline = tokio::time::Instant::now() + limits.window;
    let mut found: Vec<LanUrl> = Vec::new();
    let mut buf = [0u8; MAX_DATAGRAM];
    let mut replies = 0usize;
    while replies < limits.max_replies && found.len() < limits.max_locations {
        let received = tokio::time::timeout_at(deadline, socket.recv_from(&mut buf)).await;
        let Ok(Ok((n, source))) = received else {
            break;
        };
        replies += 1;
        match parse_reply(&buf[..n], source) {
            Ok(url) if !found.contains(&url) => found.push(url),
            Ok(_) => {}
            Err(error) => crate::alog!("[dlna] ignored ssdp reply from {source}: {error}"),
        }
    }
    Ok(found)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vector(name: &str) -> Vec<u8> {
        let path = format!(
            "{}/../../tests/vectors/dlna/{name}",
            env!("CARGO_MANIFEST_DIR")
        );
        std::fs::read(path).unwrap()
    }

    fn from(ip: &str) -> SocketAddr {
        SocketAddr::new(ip.parse().unwrap(), 1900)
    }

    #[test]
    fn m_search_targets_media_renderers() {
        let request = m_search(MEDIA_RENDERER, 2);
        assert!(request.starts_with("M-SEARCH * HTTP/1.1\r\n"));
        assert!(request.contains("MAN: \"ssdp:discover\"\r\n"));
        assert!(request.contains("ST: urn:schemas-upnp-org:device:MediaRenderer:1\r\n"));
        assert!(request.contains("MX: 2\r\n"));
        assert!(request.ends_with("\r\n\r\n"));
    }

    #[test]
    fn accepts_a_reply_whose_location_is_the_replier() {
        let url = parse_reply(&vector("ssdp-reply.txt"), from("192.168.1.40")).unwrap();
        assert_eq!(url.addr, "192.168.1.40:49152".parse().unwrap());
        assert_eq!(url.path, "/description.xml");
    }

    #[test]
    fn rejects_location_pointing_elsewhere() {
        let reply = vector("ssdp-reply-foreign-location.txt");
        assert_eq!(
            parse_reply(&reply, from("192.168.1.40")),
            Err(CastError::Forbidden(
                "ssdp location host differs from replier"
            ))
        );
    }

    #[test]
    fn rejects_non_lan_repliers_and_garbage() {
        let reply = vector("ssdp-reply.txt");
        for source in ["8.8.8.8", "127.0.0.1", "100.64.1.1"] {
            assert!(parse_reply(&reply, from(source)).is_err(), "{source}");
        }
        assert!(parse_reply(&reply, SocketAddr::new("fe80::1".parse().unwrap(), 1900)).is_err());
        assert!(parse_reply(b"NOTIFY * HTTP/1.1\r\n\r\n", from("192.168.1.40")).is_err());
        assert!(parse_reply(b"HTTP/1.1 200 OK\r\nST: x\r\n\r\n", from("192.168.1.40")).is_err());
        let duplicate = b"HTTP/1.1 200 OK\r\nLOCATION: http://192.168.1.40/a\r\nLocation: http://192.168.1.40/b\r\n\r\n";
        assert!(parse_reply(duplicate, from("192.168.1.40")).is_err());
        assert_eq!(
            parse_reply(&[b'a'; MAX_DATAGRAM], from("192.168.1.40")),
            Err(CastError::TooLarge)
        );
    }
}
