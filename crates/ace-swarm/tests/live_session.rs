//! Public production follower authentication, trailer stripping and ordered TS output.
mod common;

#[tokio::test]
async fn live_session_emits_contiguous_ts_from_one_peer() {
    let piece_length = 188 * 2;
    let expected = 3 * piece_length;
    let content = common::ts_content(5 * piece_length);
    let info = ace_swarm::types::StreamInfo {
        infohash: [0x42; 20],
        piece_length: piece_length as u64,
        chunk_length: 188,
        trackers: vec![],
        metadata: Default::default(),
        sig_len: 0,
        source_pubkey: vec![],
    };
    let pieces = content
        .chunks_exact(piece_length)
        .map(<[u8]>::to_vec)
        .collect();
    let got = common::follow_prefix(info, 10, pieces, expected).await;
    assert_eq!(got, content[..expected]);
}

#[tokio::test]
async fn live_session_verifies_signed_pieces_and_emits_only_media() {
    let auth = ace_wire::live_auth::LiveSourceAuth::generate();
    let media_per_piece = 188 * 2;
    let piece_length = media_per_piece + auth.signature_len();
    let expected = 3 * media_per_piece;
    let media = common::ts_content(5 * media_per_piece);
    let pieces = media
        .chunks_exact(media_per_piece)
        .map(|payload| {
            let mut wire = payload.to_vec();
            wire.extend_from_slice(&auth.sign(payload));
            assert_eq!(wire.len(), piece_length);
            wire
        })
        .collect();
    let info = ace_swarm::types::StreamInfo {
        infohash: [0x24; 20],
        piece_length: piece_length as u64,
        chunk_length: piece_length as u64,
        trackers: vec![],
        metadata: Default::default(),
        sig_len: auth.signature_len(),
        source_pubkey: auth.pubkey_der(),
    };
    let got = common::follow_prefix(info, 10, pieces, expected).await;
    assert_eq!(
        got,
        media[..expected],
        "source signatures must be verified and stripped"
    );
}
