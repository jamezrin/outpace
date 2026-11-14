use super::*;
use std::{future::Future, pin::Pin};
pub(in crate::live_session) struct Prepared {
    pub upstream: ConnectedUpstream,
    pub permit: WorkerPermit,
}
pub(super) struct Start {
    pub cursor: u64,
    pub head: u64,
}
pub(in crate::live_session) type WriteResult = Result<Prepared, SocketAddrV4>;
pub(super) type WriteJob = Pin<Box<dyn Future<Output = WriteResult> + Send>>;
pub(super) struct Activation {
    pub addr: SocketAddrV4,
    pub peer_id: u64,
    pub replacement: Option<u64>,
    pub write: WriteJob,
}
#[cfg(test)]
pub(super) fn prepare(
    _peer_id: u64,
    upstream: ConnectedUpstream,
    start: u64,
    head: u64,
    identity: &Identity,
    reachability: Option<&Arc<ReachabilityMonitor>>,
) -> WriteJob {
    prepare_with_policy(
        Start {
            cursor: start,
            head,
        },
        upstream,
        identity,
        reachability,
        &Default::default(),
        WorkerOwnership::new(1).admission(),
    )
}
pub(super) fn prepare_with_policy(
    start: Start,
    mut upstream: ConnectedUpstream,
    identity: &Identity,
    reachability: Option<&Arc<ReachabilityMonitor>>,
    policy: &crate::peer_policy::PeerPolicy,
    admission: WorkerAdmission,
) -> WriteJob {
    let Start {
        cursor: start,
        head,
    } = start;
    // Observe the already-received echo exactly once, before any write can fail
    // or be cancelled. The returned future owns no observer/identity borrow.
    if let Some(ip) = upstream.yourip {
        if let Some(monitor) = reachability {
            monitor.observe_yourip(ip);
        }
        // Identity mode learns our public address from the same echo. No identity, no work.
        crate::engine_location::note_public_address(ip, (*upstream.addr.ip()).into());
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
        node: policy.node_fields(),
        peer_ip: Some(upstream.addr.ip().octets()),
        metadata_size: None,
    };
    let message = PeerMessage::Extended {
        ext_id: 0,
        payload: hs.sign_and_encode(identity),
    };
    Box::pin(async move {
        let permit = admission.acquire().await;
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
        Ok(Prepared { upstream, permit })
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

#[cfg(test)]
#[tokio::test]
async fn private_activation_and_gossip_wire_do_not_advertise_inbound_port() {
    for private in [false, true] {
        for gossip in [false, true] {
            let observed = tokio::time::timeout(Duration::from_secs(2), async {
                let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                let std::net::SocketAddr::V4(addr) = listener.local_addr().unwrap() else {
                    unreachable!()
                };
                let client = TcpStream::connect(addr).await.unwrap();
                let (remote, _) = listener.accept().await.unwrap();
                let policy = if private {
                    crate::peer_policy::PeerPolicy::from_private_peers(Some(&addr.to_string()))
                        .unwrap()
                } else {
                    Default::default()
                };
                let upstream = ConnectedUpstream {
                    session: PeerSession::new(client),
                    addr,
                    window: WireLivePosition {
                        min_piece: 1,
                        max_piece: 2,
                        position: 1,
                        distance_from_source: 1,
                    },
                    unchoked: false,
                    yourip: None,
                };
                let identity = Identity::generate();
                let mut remote = PeerSession::new(remote);
                let capture = async {
                    let PeerMessage::Extended { ext_id: 0, payload } =
                        remote.read_message().await.unwrap()
                    else {
                        panic!("signed extension")
                    };
                    payload
                };
                if gossip {
                    let job = pool_support::stale_gossip_job_with_policy(
                        upstream,
                        &identity,
                        tokio::time::Instant::now() + Duration::from_millis(50),
                        &policy,
                    );
                    let (_, payload) = tokio::join!(job, capture);
                    payload
                } else {
                    let job = prepare_with_policy(
                        Start { cursor: 1, head: 2 },
                        upstream,
                        &identity,
                        None,
                        &policy,
                        WorkerOwnership::new(1).admission(),
                    );
                    let (written, payload) = tokio::join!(job, capture);
                    drop(written.unwrap());
                    payload
                }
            })
            .await
            .expect("bounded activation/gossip capture");
            crate::peer_policy::assert_signed_listen_port(
                &observed,
                if private { 0 } else { 8621 },
            );
        }
    }
}
