//! End-to-end pipeline test with a mock peer (no live network).
//!
//! Proves the full data path works: our client performs the peer handshake + a SIGNED
//! extended handshake (which the mock peer VERIFIES with `ace_wire::identity`), gets
//! unchoked, requests pieces, and the driver reassembles the `Piece` blocks into the
//! original byte stream — which we then confirm is valid MPEG-TS and HLS-segmentable.
//! Only the real-network leg remains untested; the wiring is exercised here.

use ace_peer::session::PeerSession;
use ace_swarm::driver::{download_from_peer, DownloadParams};
use ace_wire::bencode::Bencode;
use ace_wire::handshake::Handshake;
use ace_wire::identity::verify_handshake;
use ace_wire::message::PeerMessage;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const TS_PACKET: usize = 188;

/// Build `n_pieces * piece_len` bytes of well-formed MPEG-TS (188-byte packets, sync 0x47),
/// distinct per offset so reassembly order is checkable.
fn make_ts_content(total: usize) -> Vec<u8> {
    let mut v = vec![0u8; total];
    for (i, b) in v.iter_mut().enumerate() {
        *b = if i % TS_PACKET == 0 {
            0x47
        } else {
            (i % 251) as u8
        };
    }
    v
}

#[tokio::test]
async fn full_pipeline_mock_peer_to_mpegts() {
    let infohash = [0x42u8; 20];
    let piece_length = (TS_PACKET * 2) as u64; // 376
    let chunk_length = TS_PACKET as u64; // 188 → 2 blocks/piece
    let start_piece = 0u64;
    let head = 3u64; // pieces 0..=3 → 4 pieces
    let n_pieces = (head - start_piece + 1) as usize;
    let content = make_ts_content(n_pieces * piece_length as usize);
    let content_for_peer = content.clone();

    let (client, mut server) = tokio::io::duplex(64 * 1024);

    // ---- mock peer ----
    let peer = tokio::spawn(async move {
        // 1) handshake: read our 66 bytes, reply with our own (same infohash).
        let mut hs = [0u8; 66];
        server.read_exact(&mut hs).await.unwrap();
        let reply = Handshake::new(infohash, *b"R30------MOCKPEER001");
        server.write_all(&reply.encode()).await.unwrap();

        let mut sess = PeerSession::new(server);

        // 2) read our SIGNED extended handshake and VERIFY the signature.
        match sess.read_message().await.unwrap() {
            PeerMessage::Extended { ext_id, payload } => {
                assert_eq!(ext_id, 0);
                let dict = match Bencode::parse(&payload).unwrap() {
                    Bencode::Dict(d) => d,
                    _ => panic!("handshake not a dict"),
                };
                let node_id: [u8; 32] = dict[b"node_id".as_slice()]
                    .as_bytes()
                    .unwrap()
                    .try_into()
                    .unwrap();
                let sig: [u8; 64] = dict[b"signature".as_slice()]
                    .as_bytes()
                    .unwrap()
                    .try_into()
                    .unwrap();
                assert!(
                    verify_handshake(&node_id, &sig, &dict),
                    "mock peer rejected our signature"
                );
            }
            other => panic!("expected extended handshake, got {other:?}"),
        }

        // 3) unchoke, then serve every Request with the matching content bytes.
        sess.send(&PeerMessage::Unchoke).await.unwrap();
        let total_blocks = n_pieces * 2; // 2 blocks per piece
        for _ in 0..total_blocks {
            match sess.read_message().await.unwrap() {
                PeerMessage::Request {
                    index,
                    begin,
                    length,
                } => {
                    let off = index as usize * piece_length as usize + begin as usize;
                    let block = content_for_peer[off..off + length as usize].to_vec();
                    sess.send(&PeerMessage::Piece {
                        index,
                        begin,
                        block,
                    })
                    .await
                    .unwrap();
                }
                other => panic!("expected request, got {other:?}"),
            }
        }
    });

    // ---- our client ----
    let mut session = PeerSession::new(client);
    session
        .perform_handshake(infohash, ace_wire::handshake::random_peer_id())
        .await
        .unwrap();

    let identity = ace_wire::identity::Identity::generate();
    let hs = ace_wire::extended::OutgoingExtendedHandshake {
        ace_metadata_version: 1,
        ut_metadata_id: 2,
        mi: Some(ace_wire::extended::LivePosition {
            min_piece: start_piece as i64,
            max_piece: head as i64,
            position: head as i64,
            distance_from_source: 1,
        }),
        node: ace_wire::extended::NodeFields {
            ts: 1,
            ..Default::default()
        },
        peer_ip: None,
        metadata_size: None,
    };
    session
        .send_signed_extended_handshake(&hs, &identity)
        .await
        .unwrap();

    let params = DownloadParams {
        piece_length,
        chunk_length,
        start_piece,
        head,
        max_in_flight: 4,
    };
    let got = download_from_peer(&mut session, params, start_piece, head)
        .await
        .unwrap();
    peer.await.unwrap();

    // The reassembled stream is byte-identical to the source...
    assert_eq!(got, content, "reassembled bytes differ from source");
    // ...and is valid MPEG-TS that the media layer can segment for HLS.
    assert!(ace_media::mpegts::is_aligned(&got), "output not TS-aligned");
    let segs = ace_media::hls::segment(&got, 2).unwrap();
    assert_eq!(segs.len(), 4); // 8 packets / 2 per segment
}
