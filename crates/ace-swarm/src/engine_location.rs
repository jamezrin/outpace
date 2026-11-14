//! Public-address tally and one RIPEstat lookup for the optional AceStream identity.
//!
//! Peers echo our address in `yourip`. After two distinct peers agree on a globally routable
//! address, one task asks RIPEstat for the ASN and country and records them on the
//! installed identity. Redirects are not followed. The lookup host is fixed, and DNS
//! is pinned to an address that passes the same global-address check as other clients.
//! An `OUTPACE_ACESTREAM_IDENTITY_ASN` / `_COUNTRY` pair skips this entirely.

use crate::engine_profile;
use crate::http_fetch::{self, FetchPolicy};
use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;
use tokio::sync::watch;

const STAT_HOST: &str = "stat.ripe.net";
const LOOKUP_USER_AGENT: &str = "Python-urllib/3.10";
const BODY_LIMIT: usize = 256 * 1024;
const LOOKUP_TIMEOUT: Duration = Duration::from_secs(5);

/// Remember a public address `peer` echoed. Two distinct peers agreeing on a global
/// address schedule the single lookup, and only when an identity is installed without a
/// location yet. Repeated echoes from one peer (reconnects, reactivations) count once.
pub fn note_public_address(ip: IpAddr, peer: IpAddr) {
    let Some(identity) = engine_profile::identity() else {
        return;
    };
    if identity.has_geo() || !http_fetch::is_safe_ip(&ip) {
        return;
    }
    let ready = {
        let mut watch = watch().lock().unwrap();
        // Once the lookup fired, later echoes have nothing left to corroborate.
        if watch.fired {
            return;
        }
        watch
            .counts
            .observe(ip, peer)
            .filter(|_| watch.listener.is_some())
    };
    if let Some(ip) = ready {
        dispatch(ip);
    }
}

/// Register the one-shot lookup. A candidate that is already corroborated is dispatched now.
pub fn enable_lookup() {
    let ready = {
        let mut watch = watch().lock().unwrap();
        if watch.listener.is_some() {
            return;
        }
        watch.listener = Some(Arc::new(spawn_lookup));
        watch.counts.chosen.filter(|_| !watch.fired)
    };
    if let Some(ip) = ready {
        dispatch(ip);
    }
}

fn dispatch(ip: IpAddr) {
    let listener = {
        let mut watch = watch().lock().unwrap();
        if watch.fired {
            return;
        }
        watch.fired = true;
        // The tally is complete; keep only the chosen address.
        watch.counts.peers = HashMap::new();
        watch.listener.clone()
    };
    if let Some(listener) = listener {
        listener(ip);
    }
}

fn spawn_lookup(ip: IpAddr) {
    let Ok(handle) = tokio::runtime::Handle::try_current() else {
        return;
    };
    handle.spawn(async move {
        match lookup(ip).await {
            Ok((asn, country)) => {
                if let Some(identity) = engine_profile::identity() {
                    identity.set_geo(asn, country);
                    crate::alog!("[identity] recorded network location");
                }
            }
            Err(()) => crate::alog!("[identity] network location lookup failed"),
        }
    });
}

async fn lookup(ip: IpAddr) -> Result<(i64, [u8; 2]), ()> {
    if !http_fetch::is_safe_ip(&ip) {
        return Err(());
    }
    let overview = get_json(&stat_url("prefix-overview", ip)?).await?;
    let geoloc = get_json(&stat_url("geoloc", ip)?).await?;
    let asn = asn_from_prefix_overview(&overview).ok_or(())?;
    let country = country_from_geoloc(&geoloc).ok_or(())?;
    Ok((asn, country))
}

fn stat_url(call: &str, ip: IpAddr) -> Result<String, ()> {
    let mut url = reqwest::Url::parse(&format!("https://{STAT_HOST}/data/{call}/data.json"))
        .map_err(|_| ())?;
    url.query_pairs_mut()
        .append_pair("resource", &ip.to_string());
    Ok(url.into())
}

async fn get_json(url: &str) -> Result<String, ()> {
    let parsed = reqwest::Url::parse(url).map_err(|_| ())?;
    let (_cancel_tx, cancel) = watch::channel(false);
    let addr = http_fetch::resolve_addr(STAT_HOST, 443, FetchPolicy::default(), cancel)
        .await
        .map_err(|_| ())?;
    let client = pinned_client(addr)?;
    let response = client
        .get(parsed)
        .header(reqwest::header::USER_AGENT, LOOKUP_USER_AGENT)
        .header(reqwest::header::ACCEPT, "application/json")
        .send()
        .await
        .map_err(|_| ())?;
    if !response.status().is_success() {
        return Err(());
    }
    let mut body = Vec::new();
    let mut response = response;
    while let Some(chunk) = response.chunk().await.map_err(|_| ())? {
        if body.len().saturating_add(chunk.len()) > BODY_LIMIT {
            return Err(());
        }
        body.extend_from_slice(&chunk);
    }
    String::from_utf8(body).map_err(|_| ())
}

fn pinned_client(addr: std::net::SocketAddr) -> Result<reqwest::Client, ()> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let roots = webpki_root_certs::TLS_SERVER_ROOT_CERTS.iter().map(|cert| {
        reqwest::Certificate::from_der(cert.as_ref()).expect("bundled TLS root certificate")
    });
    reqwest::Client::builder()
        .tls_certs_only(roots)
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .retry(reqwest::retry::never())
        .referer(false)
        .timeout(LOOKUP_TIMEOUT)
        .connect_timeout(Duration::from_secs(2))
        .dns_resolver(Arc::new(PinnedDns))
        .resolve(STAT_HOST, addr)
        .build()
        .map_err(|_| ())
}

struct PinnedDns;

impl reqwest::dns::Resolve for PinnedDns {
    fn resolve(&self, _name: reqwest::dns::Name) -> reqwest::dns::Resolving {
        Box::pin(async { Err(std::io::Error::other("unpinned DNS request").into()) })
    }
}

fn asn_from_prefix_overview(body: &str) -> Option<i64> {
    let value: serde_json::Value = serde_json::from_str(body).ok()?;
    let asn = value.pointer("/data/asns/0/asn")?.as_i64()?;
    (1..=i64::from(u32::MAX)).contains(&asn).then_some(asn)
}

fn country_from_geoloc(body: &str) -> Option<[u8; 2]> {
    let value: serde_json::Value = serde_json::from_str(body).ok()?;
    let country = value
        .pointer("/data/located_resources/0/locations/0/country")?
        .as_str()?;
    engine_profile::parse_country(country).ok()
}

#[derive(Default)]
struct AddressCounts {
    /// The first peer that echoed each address; a second, different peer corroborates it.
    peers: HashMap<IpAddr, IpAddr>,
    chosen: Option<IpAddr>,
}

impl AddressCounts {
    /// The address that was just echoed by a second distinct peer, once.
    fn observe(&mut self, ip: IpAddr, peer: IpAddr) -> Option<IpAddr> {
        if self.chosen.is_some() {
            return None;
        }
        let first = *self.peers.entry(ip).or_insert(peer);
        if first != peer {
            self.chosen = Some(ip);
            return Some(ip);
        }
        None
    }
}

struct Watch {
    counts: AddressCounts,
    listener: Option<Arc<dyn Fn(IpAddr) + Send + Sync>>,
    fired: bool,
}

fn watch() -> &'static Mutex<Watch> {
    static WATCH: OnceLock<Mutex<Watch>> = OnceLock::new();
    WATCH.get_or_init(|| {
        Mutex::new(Watch {
            counts: AddressCounts::default(),
            listener: None,
            fired: false,
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    fn ip(octets: [u8; 4]) -> IpAddr {
        IpAddr::V4(Ipv4Addr::from(octets))
    }

    #[test]
    fn two_agreeing_addresses_choose_once() {
        let mut counts = AddressCounts::default();
        let first = ip([203, 0, 113, 7]);
        let other = ip([203, 0, 113, 8]);
        let (peer_a, peer_b, peer_c) = (
            ip([198, 51, 100, 1]),
            ip([198, 51, 100, 2]),
            ip([198, 51, 100, 3]),
        );
        assert_eq!(counts.observe(first, peer_a), None);
        assert_eq!(counts.observe(other, peer_b), None);
        assert_eq!(counts.observe(first, peer_b), Some(first));
        assert_eq!(counts.observe(first, peer_c), None);
        assert_eq!(counts.observe(other, peer_c), None);
    }

    #[test]
    fn repeated_echoes_from_one_peer_count_once() {
        let mut counts = AddressCounts::default();
        let addr = ip([203, 0, 113, 7]);
        let peer = ip([198, 51, 100, 1]);
        for _ in 0..5 {
            assert_eq!(counts.observe(addr, peer), None);
        }
        assert!(counts.chosen.is_none());
        assert_eq!(counts.observe(addr, ip([198, 51, 100, 2])), Some(addr));
    }

    #[test]
    fn ripe_payloads_yield_asn_and_country() {
        let overview = r#"{"data":{"asns":[{"asn":13335,"holder":"example"}]}}"#;
        let geoloc = r#"{"data":{"located_resources":[{"locations":[{"country":"us"}]}]}}"#;
        assert_eq!(asn_from_prefix_overview(overview), Some(13335));
        assert_eq!(country_from_geoloc(geoloc), Some(*b"US"));
        assert_eq!(asn_from_prefix_overview(r#"{"data":{"asns":[]}}"#), None);
        assert_eq!(
            country_from_geoloc(
                r#"{"data":{"located_resources":[{"locations":[{"country":"?"}]}]}}"#
            ),
            None
        );
    }

    #[test]
    fn note_without_an_identity_does_not_choose() {
        note_public_address(ip([203, 0, 113, 7]), ip([198, 51, 100, 1]));
        note_public_address(ip([203, 0, 113, 7]), ip([198, 51, 100, 2]));
        let watch = watch().lock().unwrap();
        assert!(watch.counts.chosen.is_none());
    }
}
