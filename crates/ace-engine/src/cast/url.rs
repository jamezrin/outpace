//! A deliberately narrow `http://` URL for renderer endpoints.
//!
//! Renderer URLs arrive in untrusted SSDP replies and device descriptions, so only the shape a
//! LAN device needs is accepted: plain `http`, an IPv4 literal host (no DNS, so no rebinding),
//! an optional port, and a printable path. Userinfo, fragments, hostnames and IPv6 are refused.

use super::CastError;
use std::fmt;
use std::net::{Ipv4Addr, SocketAddrV4};

/// Longest URL accepted from a renderer.
const MAX_URL: usize = 1024;

/// Private (RFC 1918) or link-local IPv4: the only hosts a renderer URL may name.
pub fn is_lan_ipv4(ip: Ipv4Addr) -> bool {
    ip.is_private() || ip.is_link_local()
}

/// A parsed renderer URL: socket address plus origin-form request target.
#[derive(Clone, PartialEq, Eq)]
pub struct LanUrl {
    pub addr: SocketAddrV4,
    /// Request target, always starting with `/` (path and optional query).
    pub path: String,
}

impl fmt::Debug for LanUrl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "LanUrl({}{})", self.addr, self.path)
    }
}

impl LanUrl {
    /// Parse an absolute `http://a.b.c.d[:port]/path` URL. The host is not policy-checked
    /// here; callers compare it against the replying address.
    pub fn parse(raw: &str) -> Result<Self, CastError> {
        let raw = raw.trim();
        if raw.len() > MAX_URL {
            return Err(CastError::Malformed("url too long"));
        }
        let rest = strip_prefix_ignore_case(raw, "http://")
            .ok_or(CastError::Malformed("url is not http"))?;
        let (authority, path) = match rest.find(['/', '?', '#']) {
            Some(at) => rest.split_at(at),
            None => (rest, "/"),
        };
        if authority.is_empty() || authority.contains('@') {
            return Err(CastError::Malformed("bad url authority"));
        }
        let (host, port) = match authority.rsplit_once(':') {
            Some((host, port)) => {
                if port.is_empty() || port.len() > 5 || !port.bytes().all(|b| b.is_ascii_digit()) {
                    return Err(CastError::Malformed("bad url port"));
                }
                let port: u16 = port
                    .parse()
                    .map_err(|_| CastError::Malformed("bad url port"))?;
                (host, port)
            }
            None => (authority, 80),
        };
        if port == 0 {
            return Err(CastError::Malformed("bad url port"));
        }
        // `Ipv4Addr::from_str` rejects hostnames, IPv6, and leading-zero octets.
        let ip: Ipv4Addr = host
            .parse()
            .map_err(|_| CastError::Malformed("url host is not an IPv4 address"))?;
        let path = if path.starts_with(['?']) {
            format!("/{path}")
        } else {
            path.to_string()
        };
        Self::with_path(SocketAddrV4::new(ip, port), &path)
    }

    fn with_path(addr: SocketAddrV4, path: &str) -> Result<Self, CastError> {
        if path.contains('#') {
            return Err(CastError::Malformed("url has a fragment"));
        }
        if !path.starts_with('/') || !path.bytes().all(|b| b.is_ascii_graphic()) {
            return Err(CastError::Malformed("bad url path"));
        }
        Ok(Self {
            addr,
            path: path.to_string(),
        })
    }

    /// Resolve a reference (absolute URL, absolute path, or relative path) against this URL.
    pub fn join(&self, reference: &str) -> Result<Self, CastError> {
        let reference = reference.trim();
        if reference.len() > MAX_URL {
            return Err(CastError::Malformed("url too long"));
        }
        if reference.starts_with("//") {
            // A network-path reference names another authority; treat it as absolute http.
            return Self::parse(&format!("http:{reference}"));
        }
        if reference
            .split(['/', '?', '#'])
            .next()
            .is_some_and(|scheme| scheme.contains(':'))
        {
            return Self::parse(reference);
        }
        if reference.starts_with('/') {
            return Self::with_path(self.addr, reference);
        }
        let base = self.path.split('?').next().unwrap_or("/");
        let dir = &base[..base.rfind('/').map_or(0, |at| at + 1)];
        let dir = if dir.is_empty() { "/" } else { dir };
        Self::with_path(self.addr, &format!("{dir}{reference}"))
    }

    pub fn ip(&self) -> Ipv4Addr {
        *self.addr.ip()
    }
}

fn strip_prefix_ignore_case<'a>(value: &'a str, prefix: &str) -> Option<&'a str> {
    let head = value.get(..prefix.len())?;
    head.eq_ignore_ascii_case(prefix)
        .then(|| &value[prefix.len()..])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_ipv4_literal_urls() {
        let url = LanUrl::parse("http://192.168.1.40:49152/description.xml").unwrap();
        assert_eq!(url.addr, "192.168.1.40:49152".parse().unwrap());
        assert_eq!(url.path, "/description.xml");
        let url = LanUrl::parse("HTTP://10.0.0.2").unwrap();
        assert_eq!(url.addr.port(), 80);
        assert_eq!(url.path, "/");
        let url = LanUrl::parse("http://10.0.0.2:8080?x=1").unwrap();
        assert_eq!(url.path, "/?x=1");
    }

    #[test]
    fn rejects_hostnames_userinfo_ipv6_and_other_schemes() {
        for bad in [
            "https://192.168.1.4/",
            "http://tv.local/desc.xml",
            "http://user@192.168.1.4/",
            "http://[fe80::1]/",
            "http://192.168.001.4/",
            "http://192.168.1.4:0/",
            "http://192.168.1.4:99999/",
            "http://192.168.1.4:/",
            "http://192.168.1.4/a b",
            "http://192.168.1.4/#frag",
            "file:///etc/passwd",
            "",
        ] {
            assert!(LanUrl::parse(bad).is_err(), "{bad:?} must be rejected");
        }
        let long = format!("http://192.168.1.4/{}", "a".repeat(MAX_URL));
        assert!(LanUrl::parse(&long).is_err());
    }

    #[test]
    fn joins_references() {
        let base = LanUrl::parse("http://192.168.1.4:1400/dev/desc.xml").unwrap();
        assert_eq!(base.join("/ctl/av").unwrap().path, "/ctl/av");
        assert_eq!(base.join("ctl/av").unwrap().path, "/dev/ctl/av");
        assert_eq!(base.join("ctl/av").unwrap().addr, base.addr);
        let other = base.join("http://192.168.1.9:80/x").unwrap();
        assert_eq!(other.addr, "192.168.1.9:80".parse().unwrap());
        let network_path = base.join("//192.168.1.9:81/y").unwrap();
        assert_eq!(network_path.addr, "192.168.1.9:81".parse().unwrap());
        assert!(base.join("gopher://192.168.1.4/").is_err());
    }

    #[test]
    fn lan_addresses_are_private_or_link_local_only() {
        for ok in ["10.1.2.3", "172.16.0.1", "192.168.1.1", "169.254.3.4"] {
            assert!(is_lan_ipv4(ok.parse().unwrap()), "{ok}");
        }
        for bad in [
            "127.0.0.1",
            "8.8.8.8",
            "0.0.0.0",
            "100.64.0.1",
            "239.255.255.250",
            "255.255.255.255",
        ] {
            assert!(!is_lan_ipv4(bad.parse().unwrap()), "{bad}");
        }
    }
}
