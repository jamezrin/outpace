use super::*;
use std::{future::Future, pin::Pin};
pub(super) type WriteResult = Result<ConnectedUpstream, SocketAddrV4>;
pub(super) type WriteJob = Pin<Box<dyn Future<Output = WriteResult> + Send>>;
pub(super) struct Activation {
    pub addr: SocketAddrV4,
    pub peer_id: u64,
    pub replacement: Option<u64>,
    pub write: WriteJob,
}
pub(super) fn prepare(
    peer_id: u64,
    mut upstream: ConnectedUpstream,
    start: u64,
    head: u64,
    identity: &Identity,
    reachability: Option<&Arc<ReachabilityMonitor>>,
) -> WriteJob {
    // Observe the already-received echo exactly once, before any write can fail
    // or be cancelled. The returned future owns no observer/identity borrow.
    if let (Some(monitor), Some(ip)) = (reachability, upstream.yourip) {
        monitor.observe_yourip(ip);
    }
    let hs = OutgoingExtendedHandshake {
        ace_metadata_version: 1,
        ut_metadata_id: 2,
        mi: Some(WireLivePosition {
            min_piece: start as i64,
            max_piece: head as i64,
            position: -1,
            distance_from_source: 1,
        }),
        node: NodeFields {
            ts: 5000 + peer_id as i64,
            ..NodeFields::default()
        },
        peer_ip: Some(upstream.addr.ip().octets()),
        metadata_size: None,
    };
    let message = PeerMessage::Extended {
        ext_id: 0,
        payload: hs.sign_and_encode(identity),
    };
    Box::pin(async move {
        #[cfg(test)]
        let observed_addr = upstream.addr;
        let write = async {
            upstream.session.send(&message).await?;
            upstream.session.send(&PeerMessage::Interested).await
        };
        #[cfg(test)]
        let write = async {
            let mut write = std::pin::pin!(write);
            std::future::poll_fn(|cx| {
                let result = std::future::Future::poll(write.as_mut(), cx);
                if result.is_pending() {
                    let _ = ACTIVATION_PROBE.try_with(|probe| {
                        if probe.addr == observed_addr {
                            probe.pending.notify_one();
                        }
                    });
                }
                result
            })
            .await
        };
        if write.await.is_err() {
            return Err(upstream.addr);
        }
        Ok(upstream)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[tokio::test]
    async fn preparation_observes_echo_once_even_when_cancelled_or_writes_fail() {
        let monitor = Arc::new(ReachabilityMonitor::default());
        let identity = Identity::generate();
        for fail in [false, true] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = match listener.local_addr().unwrap() {
                std::net::SocketAddr::V4(addr) => addr,
                _ => unreachable!(),
            };
            let mut socket = TcpStream::connect(addr).await.unwrap();
            let (mut remote, _) = listener.accept().await.unwrap();
            if fail {
                socket.shutdown().await.unwrap();
            }
            let job = prepare(
                1,
                ConnectedUpstream {
                    session: PeerSession::new(socket),
                    addr,
                    window: WireLivePosition {
                        min_piece: 0,
                        max_piece: 1,
                        position: 0,
                        distance_from_source: 1,
                    },
                    unchoked: false,
                    yourip: Some(std::net::Ipv4Addr::new(192, 0, 2, 1).into()),
                },
                0,
                1,
                &identity,
                Some(&monitor),
            );
            let expected = if fail { 2 } else { 1 };
            assert_eq!(monitor.observed().unwrap().total, expected);
            if fail {
                assert!(job.await.is_err());
            } else {
                drop(job);
            }
            assert_eq!(monitor.observed().unwrap().total, expected);
            let mut bytes = Vec::new();
            tokio::time::timeout(Duration::from_secs(1), remote.read_to_end(&mut bytes))
                .await
                .unwrap()
                .unwrap();
            assert!(bytes.is_empty());
        }
    }
}
