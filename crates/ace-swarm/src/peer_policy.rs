//! Shared immutable outbound peer policy. Runtime integration is required for private operation.

use std::{collections::HashSet, fmt, future::Future, net::SocketAddrV4, sync::Arc};

const MAX_PRIVATE_PEERS: usize = 256;
const MAX_PRIVATE_PEER_BYTES: usize = 8192;

/// Startup-validated exact peer endpoints. Clones share one immutable set.
#[derive(Clone, Default)]
pub struct PeerPolicy {
    private: Option<Arc<HashSet<SocketAddrV4>>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidPrivatePeers;

impl fmt::Display for InvalidPrivatePeers {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("invalid_private_peers")
    }
}
impl std::error::Error for InvalidPrivatePeers {}

/// Denial is distinct from a permitted dial's transport error, without retaining endpoints.
#[derive(Debug)]
pub enum PeerDialError<E> {
    Denied,
    Transport(E),
}

impl fmt::Debug for PeerPolicy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PeerPolicy")
            .field("private", &self.is_private())
            .field(
                "endpoint_count",
                &self.private.as_ref().map_or(0, |set| set.len()),
            )
            .finish()
    }
}

pub struct PeerLabel {
    endpoint: SocketAddrV4,
    private: bool,
}
impl fmt::Display for PeerLabel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.private {
            f.write_str("private-peer")
        } else {
            fmt::Display::fmt(&self.endpoint, f)
        }
    }
}
impl fmt::Debug for PeerLabel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}
pub struct PayloadLabel<'a> {
    payload: &'a [u8],
    private: bool,
}
impl fmt::Display for PayloadLabel<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.private {
            return f.write_str("redacted");
        }
        for byte in self.payload.iter().take(24) {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}
impl fmt::Debug for PayloadLabel<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

impl PeerPolicy {
    /// Private nodes never accept inbound peers; advertise no listening port.
    pub(crate) fn node_fields(&self) -> ace_wire::extended::NodeFields {
        let mut fields = crate::engine_profile::node_fields();
        if self.is_private() {
            fields.p = 0;
        }
        fields
    }

    /// Absence preserves public behavior. Present-but-empty or invalid input fails closed.
    /// Only numeric IPv4 endpoints are accepted; no DNS lookup occurs during parsing or checks.
    pub fn from_private_peers(raw: Option<&str>) -> Result<Self, InvalidPrivatePeers> {
        let Some(raw) = raw else {
            return Ok(Self::default());
        };
        if raw.is_empty() || raw.len() > MAX_PRIVATE_PEER_BYTES {
            return Err(InvalidPrivatePeers);
        }
        let mut endpoints = HashSet::new();
        for (index, entry) in raw.split(',').enumerate() {
            if index >= MAX_PRIVATE_PEERS {
                return Err(InvalidPrivatePeers);
            }
            let addr: SocketAddrV4 = entry.parse().map_err(|_| InvalidPrivatePeers)?;
            let ip = addr.ip();
            if addr.port() == 0 || ip.is_unspecified() || ip.is_multicast() || ip.is_broadcast() {
                return Err(InvalidPrivatePeers);
            }
            endpoints.insert(addr);
        }
        Ok(Self {
            private: Some(Arc::new(endpoints)),
        })
    }

    pub fn log_endpoint(&self, endpoint: SocketAddrV4) -> PeerLabel {
        PeerLabel {
            endpoint,
            private: self.is_private(),
        }
    }
    pub fn log_payload<'a>(&self, payload: &'a [u8]) -> PayloadLabel<'a> {
        PayloadLabel {
            payload,
            private: self.is_private(),
        }
    }

    pub fn is_private(&self) -> bool {
        self.private.is_some()
    }

    pub fn endpoint_count(&self) -> Option<usize> {
        self.private.as_ref().map(|set| set.len())
    }

    pub fn permits(&self, endpoint: SocketAddrV4) -> bool {
        self.private
            .as_ref()
            .is_none_or(|set| set.contains(&endpoint))
    }

    /// Deterministic initial candidates. Public discovery is constructed separately by callers.
    pub fn endpoints(&self) -> Vec<SocketAddrV4> {
        let mut peers: Vec<_> = self
            .private
            .as_ref()
            .map(|set| set.iter().copied().collect())
            .unwrap_or_default();
        peers.sort_unstable();
        peers
    }

    /// Enforce policy before invoking any socket/DNS factory. Callers supply only numeric peers.
    pub async fn dial_with<T, E, F, Fut>(
        &self,
        endpoint: SocketAddrV4,
        dial: F,
    ) -> Result<T, PeerDialError<E>>
    where
        F: FnOnce(SocketAddrV4) -> Fut,
        Fut: Future<Output = Result<T, E>>,
    {
        if !self.permits(endpoint) {
            return Err(PeerDialError::Denied);
        }
        dial(endpoint).await.map_err(PeerDialError::Transport)
    }
}

impl serde::Serialize for PeerPolicy {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if self.is_private() {
            serializer.serialize_some(
                &self
                    .endpoints()
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(","),
            )
        } else {
            serializer.serialize_none()
        }
    }
}
impl<'de> serde::Deserialize<'de> for PeerPolicy {
    fn deserialize<D: serde::Deserializer<'de>>(decoder: D) -> Result<Self, D::Error> {
        let raw = <Option<String> as serde::Deserialize>::deserialize(decoder)?;
        Self::from_private_peers(raw.as_deref()).map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
pub(crate) fn assert_signed_listen_port(payload: &[u8], expected: i64) {
    use ace_wire::bencode::Bencode;
    let Bencode::Dict(dict) = Bencode::parse(payload).unwrap() else {
        panic!("signed dictionary")
    };
    let signature: [u8; 64] = dict[b"signature".as_slice()]
        .as_bytes()
        .unwrap()
        .try_into()
        .unwrap();
    let node_id: [u8; 32] = dict[b"node_id".as_slice()]
        .as_bytes()
        .unwrap()
        .try_into()
        .unwrap();
    assert!(ace_wire::identity::verify_handshake(
        &node_id, &signature, &dict
    ));
    assert_eq!(dict[b"p".as_slice()].as_int(), Some(expected));
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddrV4;
    fn addr(raw: &str) -> SocketAddrV4 {
        raw.parse().unwrap()
    }

    #[test]
    fn public_default_and_private_exact_endpoint_contract() {
        let public = PeerPolicy::from_private_peers(None).unwrap();
        assert!(!public.is_private());
        assert!(public.permits(addr("127.0.0.1:1")));
        let policy =
            PeerPolicy::from_private_peers(Some("127.0.0.1:1234,192.0.2.1:4321,127.0.0.1:1234"))
                .unwrap();
        assert!(policy.is_private());
        assert_eq!(policy.endpoints().len(), 2);
        assert!(policy.permits(addr("127.0.0.1:1234")));
        assert!(!policy.permits(addr("127.0.0.1:4321")));
        assert!(!policy.permits(addr("127.0.0.2:1234")));
        assert_eq!(policy.clone().endpoints(), policy.endpoints());
        assert!(!format!("{policy:?}").contains("127.0.0.1"));
    }

    #[test]
    fn malformed_or_unusable_private_list_never_becomes_public() {
        for input in [
            "",
            " ",
            "localhost:1234",
            "127.0.0.1:0",
            "0.0.0.0:1234",
            "224.0.0.1:1234",
            "255.255.255.255:1234",
            "[::1]:1234",
            "127.0.0.1:1234,",
            "127.0.0.1:1234,,127.0.0.2:1234",
        ] {
            assert!(
                PeerPolicy::from_private_peers(Some(input)).is_err(),
                "accepted malformed private setting"
            );
        }
        let max = (1..=256)
            .map(|port| format!("127.0.0.1:{port}"))
            .collect::<Vec<_>>()
            .join(",");
        assert_eq!(
            PeerPolicy::from_private_peers(Some(&max))
                .unwrap()
                .endpoints()
                .len(),
            256
        );
        assert!(PeerPolicy::from_private_peers(Some(&(max + ",127.0.0.1:257"))).is_err());
    }

    #[tokio::test]
    async fn denied_endpoint_never_invokes_dial_factory() {
        let policy = PeerPolicy::from_private_peers(Some("127.0.0.1:1234")).unwrap();
        let called = std::cell::Cell::new(false);
        let result = policy
            .dial_with(addr("127.0.0.1:4321"), |_| async {
                called.set(true);
                Ok::<_, std::io::Error>(())
            })
            .await;
        assert!(matches!(result, Err(PeerDialError::Denied)));
        assert!(!called.get());
    }

    #[tokio::test]
    async fn permitted_endpoint_reaches_real_loopback_socket() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let std::net::SocketAddr::V4(endpoint) = listener.local_addr().unwrap() else {
            unreachable!()
        };
        let policy = PeerPolicy::from_private_peers(Some(&endpoint.to_string())).unwrap();
        let connection = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            policy.dial_with(endpoint, tokio::net::TcpStream::connect),
        )
        .await
        .unwrap()
        .unwrap();
        let (accepted, _) =
            tokio::time::timeout(std::time::Duration::from_secs(1), listener.accept())
                .await
                .unwrap()
                .unwrap();
        drop((connection, accepted));
    }
    #[test]
    fn private_log_labels_redact_debug_and_packet_previews_without_changing_public_format() {
        let endpoint = addr("127.0.0.1:1234");
        let private = PeerPolicy::from_private_peers(Some(&endpoint.to_string())).unwrap();
        assert_eq!(
            format!("{}", private.log_endpoint(endpoint)),
            "private-peer"
        );
        assert_eq!(
            format!("{:?}", private.log_endpoint(endpoint)),
            "private-peer"
        );
        assert_eq!(format!("{}", private.log_payload(&[0xab; 64])), "redacted");
        assert_eq!(
            format!("{:?}", private.log_payload(&[0xab; 64])),
            "redacted"
        );
        let public = PeerPolicy::default();
        assert_eq!(
            format!("{}", public.log_endpoint(endpoint)),
            endpoint.to_string()
        );
        assert_eq!(
            format!("{}", public.log_payload(&[0xab; 64])),
            "ab".repeat(24)
        );
    }
}
