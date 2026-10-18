use ace_peer::session::PeerSession;
use ace_swarm::live_session::{
    follow_live, LiveCounters, LiveRecoveryOptions, LiveSeedOptions, LiveSessionInputs,
    WarmPeerCache,
};
use ace_swarm::{listen::SeedRegistry, store::PieceStore, types::StreamInfo};
use ace_wire::bencode::Bencode;
use ace_wire::extended::{LivePosition, NodeFields, OutgoingExtendedHandshake};
use ace_wire::handshake::Handshake;
use ace_wire::identity::{verify_handshake, Identity};
use ace_wire::message::PeerMessage;
use std::sync::{
    atomic::{AtomicU32, AtomicU64},
    Arc,
};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Exercise the public production follower with an isolated loopback peer and no discovery.
/// Supply trailing TS packets: TsResync retains packets to confirm the next sync byte.
pub async fn follow_prefix(
    info: StreamInfo,
    start: u32,
    pieces: Vec<Vec<u8>>,
    expected: usize,
) -> Vec<u8> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let std::net::SocketAddr::V4(addr) = listener.local_addr().unwrap() else {
        unreachable!()
    };
    let peer_info = info.clone();
    let count = pieces.len();
    let peer = tokio::spawn(async move {
        let (mut server, _) = listener.accept().await.unwrap();
        let mut handshake = [0; 66];
        server.read_exact(&mut handshake).await.unwrap();
        server
            .write_all(&Handshake::new(peer_info.infohash, *b"R30-MOCKLIVE-1234567").encode())
            .await
            .unwrap();
        let mut session = PeerSession::new(server);
        session
            .send(&PeerMessage::Extended {
                ext_id: 0,
                payload: OutgoingExtendedHandshake {
                    ace_metadata_version: 1,
                    ut_metadata_id: 2,
                    mi: Some(LivePosition {
                        min_piece: start as i64,
                        max_piece: start as i64 + count as i64 - 1,
                        position: -1,
                        distance_from_source: 1,
                    }),
                    node: NodeFields::default(),
                    peer_ip: None,
                    metadata_size: None,
                }
                .encode_payload(),
            })
            .await
            .unwrap();
        let PeerMessage::Extended { ext_id: 0, payload } = session.read_message().await.unwrap()
        else {
            panic!("expected signed extended handshake")
        };
        let Bencode::Dict(dict) = Bencode::parse(&payload).unwrap() else {
            panic!("handshake not a dict")
        };
        let node_id = dict[b"node_id".as_slice()]
            .as_bytes()
            .unwrap()
            .try_into()
            .unwrap();
        let signature = dict[b"signature".as_slice()]
            .as_bytes()
            .unwrap()
            .try_into()
            .unwrap();
        assert!(
            verify_handshake(&node_id, &signature, &dict),
            "mock peer rejected handshake signature"
        );
        assert_eq!(
            session.read_message().await.unwrap(),
            PeerMessage::Interested
        );
        session.send(&PeerMessage::Unchoke).await.unwrap();
        loop {
            let Ok(message) = session.read_message().await else {
                break;
            };
            match message {
                PeerMessage::Unknown { id: 6, payload } => {
                    let piece = u32::from_be_bytes(payload[4..8].try_into().unwrap());
                    let chunk = u16::from_be_bytes(payload[8..10].try_into().unwrap());
                    let offset = chunk as usize * peer_info.chunk_length as usize;
                    let mut block = vec![0; 8];
                    block.extend_from_slice(&chunk.to_be_bytes());
                    block.extend_from_slice(
                        &pieces[(piece - start) as usize]
                            [offset..offset + peer_info.chunk_length as usize],
                    );
                    if session
                        .send(&PeerMessage::Piece {
                            index: 0,
                            begin: piece,
                            block,
                        })
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
                other => panic!("expected Acestream chunk request, got {other:?}"),
            }
        }
    });
    let (tx, mut rx) = tokio::sync::mpsc::channel(64);
    let piece_length = info.piece_length;
    let chunk_length = info.chunk_length;
    let follower = tokio::spawn(follow_live(LiveSessionInputs {
        info,
        peers: vec![addr],
        identity: Arc::new(Identity::generate()),
        output: tx,
        counters: LiveCounters {
            peers: Arc::new(AtomicU32::new(0)),
            downloaded: Arc::new(AtomicU64::new(0)),
            uploaded: Arc::new(AtomicU64::new(0)),
            peers_served: Arc::new(AtomicU32::new(0)),
        },
        seed: LiveSeedOptions {
            metrics: None,
            registry: SeedRegistry::new(),
            store_factory: Arc::new(move |_| PieceStore::new(piece_length, chunk_length, 1 << 20)),
            enabled: false,
            prefetch_pieces: count as u64,
            live_recovery: LiveRecoveryOptions {
                request_timeout_ms: 1500,
                stale_upstream_timeout_ms: 12000,
                request_check_interval_ms: 1000,
                max_active_upstreams: 4,
                max_parallel_connect: 12,
                max_piece_advance: 256,
                max_reasm_pieces_ahead: 512,
            },
            warm_peers: WarmPeerCache::memory(),
        },
        discovery: Arc::new(|_, _| Box::pin(async {})),
        reachability: None,
        initial_run: None,
    }));
    let got = tokio::time::timeout(Duration::from_secs(5), async {
        let mut got = Vec::new();
        while got.len() < expected {
            let output = rx
                .recv()
                .await
                .expect("follower stopped before expected prefix");
            assert!(!output.discontinuity);
            assert!(ace_media::mpegts::is_aligned(&output.bytes));
            got.extend_from_slice(&output.bytes);
        }
        got.truncate(expected);
        got
    })
    .await;
    drop(rx);
    // The follower is continuous, not a finite download. Stop only tasks owned by this fixture.
    follower.abort();
    let _ = follower.await;
    peer.abort();
    let peer_result = peer.await;
    if let Err(error) = peer_result {
        assert!(error.is_cancelled(), "mock peer failed: {error}");
    }
    got.expect("timed out waiting for known media prefix")
}

pub fn ts_content(total: usize) -> Vec<u8> {
    (0..total)
        .map(|i| if i % 188 == 0 { 0x47 } else { (i % 251) as u8 })
        .collect()
}
