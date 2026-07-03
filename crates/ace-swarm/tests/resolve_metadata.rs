//! End-to-end content-id resolution over an in-memory peer: BT handshake → extended
//! handshake (advertising `ut_metadata` + `metadata_size`) → serve the `AceStreamTransport`
//! blob via BEP-9 → decode to a `StreamInfo`. No network, no Acestream API.

use ace_peer::session::PeerSession;
use ace_swarm::resolve::resolve_via_peer;
use ace_wire::handshake::Handshake;
use ace_wire::identity::Identity;
use ace_wire::infohash::infohash_of_transport;
use ace_wire::message::PeerMessage;
use ace_wire::ut_metadata::{data_piece, MetadataMessage, METADATA_BLOCK_LEN};
use cbc::cipher::{block_padding::Pkcs7, BlockModeEncrypt, KeyIvInit};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Wrap raw bencode into an `AceStreamTransport` file under the global key/IV.
fn make_transport(plaintext: &[u8]) -> Vec<u8> {
    type Enc = cbc::Encryptor<aes::Aes128>;
    let body = Enc::new_from_slices(
        &ace_wire::transport::TRANSPORT_KEY,
        &ace_wire::transport::TRANSPORT_IV,
    )
    .unwrap()
    .encrypt_padded_vec::<Pkcs7>(plaintext);
    let mut out = b"AceStreamTransport\x00\x02".to_vec();
    out.extend_from_slice(&body);
    out
}

/// The peer's extended handshake: advertises `ut_metadata` id 5 and the blob size.
fn peer_extended_handshake(metadata_size: usize) -> Vec<u8> {
    format!("d1:md11:ut_metadatai5ee13:metadata_sizei{metadata_size}ee").into_bytes()
}

#[tokio::test]
async fn resolve_via_peer_fetches_and_decodes_transport() {
    // A transport file describing the stream — larger than one 16 KiB block so the fetch
    // must assemble multiple pieces.
    let filler = "x".repeat(20_000);
    let name = format!("Test {filler}");
    let pubkey = "p".repeat(124);
    let descriptor = format!(
        "d10:authmethod3:RSA7:bitratei100000e12:chunk_lengthi16384e4:name{}:{}12:piece_lengthi1048576e6:pubkey{}:{}8:trackersl18:udp://t.example:80ee",
        name.len(),
        name,
        pubkey.len(),
        pubkey,
    );
    let transport = make_transport(descriptor.as_bytes());
    let expected_infohash = infohash_of_transport(&transport);
    let metadata_size = transport.len();
    assert!(
        metadata_size > METADATA_BLOCK_LEN,
        "want a multi-piece blob"
    );

    // The content-id used as the metadata-swarm handshake key.
    let content_id = [0xC1u8; 20];
    let transport_peer = transport.clone();

    let (client, mut server) = tokio::io::duplex(128 * 1024);
    let srv = tokio::spawn(async move {
        // 1. BT handshake.
        let mut hs = [0u8; 66];
        server.read_exact(&mut hs).await.unwrap();
        let reply = Handshake::new(content_id, *b"R30------RESOLVEPEER");
        server.write_all(&reply.encode()).await.unwrap();

        let mut sess = PeerSession::new(server);
        // 2. Read the client's (signed) extended handshake, then send ours.
        let _ = sess.read_message().await.unwrap();
        sess.send(&PeerMessage::Extended {
            ext_id: 0,
            payload: peer_extended_handshake(metadata_size),
        })
        .await
        .unwrap();
        // 3. Serve every ut_metadata request from the transport blob.
        let pieces = metadata_size.div_ceil(METADATA_BLOCK_LEN);
        let mut served = 0;
        while served < pieces {
            match sess.read_message().await {
                Ok(PeerMessage::Extended { ext_id: 5, payload }) => {
                    if let Some(MetadataMessage::Request { piece }) =
                        MetadataMessage::parse(&payload)
                    {
                        let off = piece as usize * METADATA_BLOCK_LEN;
                        let end = (off + METADATA_BLOCK_LEN).min(metadata_size);
                        let resp =
                            data_piece(piece, metadata_size as i64, &transport_peer[off..end]);
                        sess.send(&PeerMessage::Extended {
                            ext_id: 2,
                            payload: resp,
                        })
                        .await
                        .unwrap();
                        served += 1;
                    }
                }
                _ => break,
            }
        }
    });

    let identity = Identity::generate();
    let mut session = PeerSession::new(client);
    let info = resolve_via_peer(&mut session, content_id, &identity)
        .await
        .unwrap();

    // The resolved StreamInfo carries the official swarm infohash, geometry, and trackers
    // from the descriptor.
    assert_eq!(info.infohash, expected_infohash);
    assert_eq!(info.piece_length, 1_048_576);
    assert_eq!(info.chunk_length, 16_384);
    assert_eq!(info.trackers, vec!["udp://t.example:80".to_string()]);
    srv.await.unwrap();
}

#[tokio::test]
async fn resolve_via_peer_rejects_oversized_metadata_size() {
    // A hostile metadata peer advertises a huge `metadata_size` to force a large BEP-9 request
    // fan-out and allocation. Resolution must fail at the handshake before any ut_metadata is
    // requested — the server task below never serves a piece.
    let content_id = [0xC2u8; 20];
    let oversized = ace_swarm::resolve::MAX_METADATA_SIZE + 1;

    let (client, mut server) = tokio::io::duplex(128 * 1024);
    let srv = tokio::spawn(async move {
        let mut hs = [0u8; 66];
        server.read_exact(&mut hs).await.unwrap();
        let reply = Handshake::new(content_id, *b"R30------RESOLVEPEER");
        server.write_all(&reply.encode()).await.unwrap();

        let mut sess = PeerSession::new(server);
        let _ = sess.read_message().await.unwrap();
        sess.send(&PeerMessage::Extended {
            ext_id: 0,
            payload: peer_extended_handshake(oversized),
        })
        .await
        .unwrap();
        // The client must not request any metadata piece; it should drop the connection.
        assert!(
            sess.read_message().await.is_err(),
            "peer should see no ut_metadata request after an oversized advertisement"
        );
    });

    let identity = Identity::generate();
    let mut session = PeerSession::new(client);
    let result = resolve_via_peer(&mut session, content_id, &identity).await;
    assert!(
        result.is_err(),
        "oversized metadata_size must be rejected, got {result:?}"
    );
    // Close our end so the peer task's read observes EOF instead of waiting for the timeout.
    drop(session);
    srv.await.unwrap();
}
