use super::*;

#[tokio::test]
async fn queued_live_outputs_keep_each_discontinuity_with_its_chunk() {
    let (tx, rx) = mpsc::channel(8);
    for (bytes, discontinuity) in [
        (b"pre".as_slice(), false),
        (b"post-one".as_slice(), true),
        (b"between".as_slice(), false),
        (b"post-two".as_slice(), true),
    ] {
        tx.send(LiveOutput {
            bytes: Bytes::copy_from_slice(bytes),
            discontinuity,
        })
        .await
        .unwrap();
    }
    drop(tx);
    let mut source = AceSource {
        metrics: None,
        _sampler: None,
        rx,
        discontinuity: false,
        peers: Arc::new(AtomicU32::new(0)),
        downloaded: Arc::new(AtomicU64::new(0)),
        uploaded: Arc::new(AtomicU64::new(0)),
        peers_served: Arc::new(AtomicU32::new(0)),
        metadata: StreamMetadata::default(),
    };

    for (expected, gap) in [
        (b"pre".as_slice(), false),
        (b"post-one".as_slice(), true),
        (b"between".as_slice(), false),
        (b"post-two".as_slice(), true),
    ] {
        assert_eq!(source.next().await.unwrap().as_ref(), expected);
        assert_eq!(source.take_discontinuity(), gap);
    }
}
