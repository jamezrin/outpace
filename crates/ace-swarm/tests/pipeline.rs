//! Public production follower -> ordered MPEG-TS -> HLS, with no live network.
mod common;

#[tokio::test]
async fn full_pipeline_mock_peer_to_mpegts() {
    let piece_length = 188 * 2;
    let expected = 4 * piece_length;
    let content = common::ts_content(6 * piece_length);
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
    let got = common::follow_prefix(info, 0, pieces, expected).await;
    assert_eq!(
        got,
        content[..expected],
        "reassembled bytes differ from source"
    );
    assert!(ace_media::mpegts::is_aligned(&got));
    let segments = ace_media::hls::segment(&got, 2).unwrap();
    assert_eq!(segments.len(), 4);
}
