use super::protocol::{prepare_reply, ServeProfile};
use crate::store::PieceStore;
use ace_wire::message::PeerMessage;
use std::sync::Arc;
use tokio::sync::Mutex;

async fn store_with_chunk(header: [u8; 8]) -> Arc<Mutex<PieceStore>> {
    let store = Arc::new(Mutex::new(PieceStore::new(32, 16, 1024)));
    // Two full chunks make the piece layout deterministic.
    let mut guard = store.lock().await;
    guard.put_chunk_with_header(7, 0, header, b"0123456789abcdef");
    guard.put_chunk_with_header(7, 1, header, b"ghijklmnopqrstuv");
    drop(guard);
    store
}

fn id6(stream: u32, piece: u32, chunk: u16) -> PeerMessage {
    let mut payload = Vec::from(stream.to_be_bytes());
    payload.extend_from_slice(&piece.to_be_bytes());
    payload.extend_from_slice(&chunk.to_be_bytes());
    PeerMessage::Unknown { id: 6, payload }
}

#[tokio::test]
async fn stored_chunk_reply_preserves_data_and_piece_header_in_both_profiles() {
    let header = *b"headr123";
    let store = store_with_chunk(header).await;
    for profile in [
        ServeProfile::Inbound {
            fallback_header: [9; 8],
        },
        ServeProfile::Reciprocal,
    ] {
        let reply = prepare_reply(&id6(41, 7, 0), Some(&store), profile)
            .await
            .expect("stored id6 request should be answered");
        assert_eq!(reply.media_bytes, 16);
        let Some((data, got_header)) =
            ace_wire::live_codec::LiveChunk::from_message(&reply.message)
                .map(|chunk| (chunk.data, chunk.piece_header))
        else {
            panic!("id6 reply should be a live Piece")
        };
        assert_eq!(data, b"0123456789abcdef");
        assert_eq!(got_header, header);
    }
}

#[tokio::test]
async fn stored_chunk_reply_preserves_trailer_bytes_and_accounts_them_in_both_profiles() {
    let header = *b"headr123";
    let data = b"0123456789abcdefSIG";
    let store = Arc::new(Mutex::new(PieceStore::new(32, 16, 1024)));
    store.lock().await.put_chunk_with_header(7, 0, header, data);

    for profile in [
        ServeProfile::Inbound {
            fallback_header: [0; 8],
        },
        ServeProfile::Reciprocal,
    ] {
        let reply = prepare_reply(&id6(41, 7, 0), Some(&store), profile)
            .await
            .expect("stored id6 request should be answered");
        assert_eq!(reply.media_bytes, data.len());
        let Some(chunk) = ace_wire::live_codec::LiveChunk::from_message(&reply.message) else {
            panic!("id6 reply should be a live Piece")
        };
        assert_eq!(chunk.piece_header, header);
        assert_eq!(chunk.data, data);
    }
}

#[tokio::test]
async fn id6_reply_echoes_stream_for_inbound_and_uses_zero_for_reciprocal() {
    let store = store_with_chunk([1; 8]).await;
    let inbound = prepare_reply(
        &id6(0x1234_5678, 7, 0),
        Some(&store),
        ServeProfile::Inbound {
            fallback_header: [0; 8],
        },
    )
    .await
    .unwrap();
    let reciprocal = prepare_reply(
        &id6(0x1234_5678, 7, 0),
        Some(&store),
        ServeProfile::Reciprocal,
    )
    .await
    .unwrap();
    for (reply, expected_stream) in [(inbound, 0x1234_5678), (reciprocal, 0)] {
        let PeerMessage::Piece {
            index,
            begin,
            block: _,
        } = reply.message
        else {
            panic!("id6 reply should be Piece")
        };
        assert_eq!(index, expected_stream);
        assert_eq!(begin, 7);
    }
}

#[tokio::test]
async fn zero_stored_header_uses_inbound_fallback_and_reciprocal_zero() {
    let store = store_with_chunk([0; 8]).await;
    let inbound = prepare_reply(
        &id6(1, 7, 0),
        Some(&store),
        ServeProfile::Inbound {
            fallback_header: *b"fallback",
        },
    )
    .await
    .unwrap();
    let reciprocal = prepare_reply(&id6(1, 7, 0), Some(&store), ServeProfile::Reciprocal)
        .await
        .unwrap();
    for (reply, expected_header) in [(inbound, *b"fallback"), (reciprocal, [0; 8])] {
        let PeerMessage::Piece { block, .. } = reply.message else {
            panic!("id6 reply should be Piece")
        };
        assert_eq!(&block[0..8], &expected_header);
    }
}

#[tokio::test]
async fn short_unavailable_and_storeless_id6_requests_are_silent_but_trailing_bytes_are_accepted() {
    let short = PeerMessage::Unknown {
        id: 6,
        payload: vec![0; 9],
    };
    let mut trailing_payload = id6(0x1234, 7, 0);
    let PeerMessage::Unknown { id: 6, payload } = &mut trailing_payload else {
        unreachable!()
    };
    payload.push(0xaa);
    let store = store_with_chunk([1; 8]).await;
    for profile in [
        ServeProfile::Inbound {
            fallback_header: [0; 8],
        },
        ServeProfile::Reciprocal,
    ] {
        assert!(prepare_reply(&short, Some(&store), profile).await.is_none());
        assert!(prepare_reply(&id6(0, 9, 0), Some(&store), profile)
            .await
            .is_none());
        assert!(prepare_reply(&id6(0, 7, 2), Some(&store), profile)
            .await
            .is_none());
        assert!(prepare_reply(&trailing_payload, Some(&store), profile)
            .await
            .is_some());
        assert!(prepare_reply(&id6(0, 7, 0), None, profile).await.is_none());
    }
}

#[tokio::test]
async fn inbound_bt_requests_use_shared_block_validation_and_account_bytes() {
    let store = store_with_chunk([0; 8]).await;
    let request = PeerMessage::Request {
        index: 7,
        begin: 14,
        length: 5,
    };
    let reply = prepare_reply(
        &request,
        Some(&store),
        ServeProfile::Inbound {
            fallback_header: [0; 8],
        },
    )
    .await
    .expect("valid inbound BT request should be answered");
    assert_eq!(
        reply.message,
        PeerMessage::Piece {
            index: 7,
            begin: 14,
            block: b"efghi".to_vec()
        }
    );
    assert_eq!(reply.media_bytes, 5);
    for invalid in [
        PeerMessage::Request {
            index: 7,
            begin: 0,
            length: 0,
        },
        PeerMessage::Request {
            index: 7,
            begin: 0,
            length: crate::store::MAX_BT_REQUEST_LEN + 1,
        },
        PeerMessage::Request {
            index: 7,
            begin: 30,
            length: 5,
        },
    ] {
        assert!(prepare_reply(
            &invalid,
            Some(&store),
            ServeProfile::Inbound {
                fallback_header: [0; 8]
            },
        )
        .await
        .is_none());
    }
}

#[tokio::test]
async fn reciprocal_profile_silently_ignores_bt_requests() {
    let store = store_with_chunk([0; 8]).await;
    let request = PeerMessage::Request {
        index: 7,
        begin: 0,
        length: 4,
    };
    assert!(
        prepare_reply(&request, Some(&store), ServeProfile::Reciprocal)
            .await
            .is_none()
    );
}
