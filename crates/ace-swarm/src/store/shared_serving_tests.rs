use super::*;

async fn complete_store(disk: bool, directory: &std::path::Path) -> Arc<Mutex<PieceStore>> {
    let backend = if disk {
        BackendKind::Disk {
            dir: directory.join("cache"),
        }
    } else {
        BackendKind::Memory
    };
    let store = Arc::new(Mutex::new(
        PieceStore::with_backend(8, 4, 8, backend).unwrap(),
    ));
    assert!(matches!(
        PieceStore::shared_put_complete_piece(&store, 7, [1; 8], b"abcdefgh")
            .await
            .unwrap(),
        CompletePut::Published
    ));
    store
}

#[tokio::test]
async fn shared_memory_payloads_are_independent_and_survive_eviction() {
    let directory = tempfile::tempdir().unwrap();
    let store = complete_store(false, directory.path()).await;
    let first = PieceStore::shared_serving_chunk(&store, 7, 0)
        .await
        .unwrap();
    let second = PieceStore::shared_serving_chunk(&store, 7, 1)
        .await
        .unwrap();
    {
        let guard = store.lock().await;
        let Backend::Memory(memory) = &guard.backend else {
            panic!("memory fixture")
        };
        assert_eq!(first.0.as_ptr(), memory.pieces[&7][&0].as_ptr());
        assert_eq!(second.0.as_ptr(), memory.pieces[&7][&1].as_ptr());
        assert_eq!(guard.cur_bytes, 8);
    }
    assert!(matches!(
        PieceStore::shared_put_complete_piece(&store, 7, [9; 8], b"XXXXXXXX")
            .await
            .unwrap(),
        CompletePut::AlreadyPresent
    ));
    PieceStore::shared_put_complete_piece(&store, 8, [2; 8], b"ijklmnop")
        .await
        .unwrap();
    {
        let guard = store.lock().await;
        assert!(!guard.has_piece(7));
        assert!(guard.has_piece(8));
        assert_eq!(guard.cur_bytes, 8);
    }
    assert_eq!(first.0.as_ref(), b"abcd");
    assert_eq!(second.0.as_ref(), b"efgh");
    assert_eq!(first.1, [1; 8]);
    assert_eq!(second.1, [1; 8]);
    // A small held chunk must not pin the backing allocation of another chunk.
    assert!(first.0.is_unique());
    assert!(second.0.is_unique());
}

#[tokio::test]
async fn shared_serving_preserves_both_backends_and_public_owned_apis() {
    fn owned_result(_: (Vec<u8>, [u8; 8])) {}
    for disk in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let store = complete_store(disk, directory.path()).await;
        let held = PieceStore::shared_serving_chunk(&store, 7, 1)
            .await
            .unwrap();
        assert_eq!(held.0.as_ref(), b"efgh");
        assert_eq!(held.1, [1; 8]);
        owned_result(PieceStore::shared_chunk(&store, 7, 1).await.unwrap());
        assert_eq!(
            PieceStore::shared_block(&store, 7, 2, 4).await,
            Some(b"cdef".to_vec())
        );
        assert!(PieceStore::shared_serving_chunk(&store, 8, 0)
            .await
            .is_none());
        assert!(PieceStore::shared_serving_chunk(&store, 7, 2)
            .await
            .is_none());
    }
}

#[tokio::test]
async fn reciprocal_borrowed_view_keeps_disk_permit_with_payload_owner() {
    let directory = tempfile::tempdir().unwrap();
    let store = complete_store(true, directory.path()).await;
    let held = PieceStore::shared_reciprocal_chunk(&store, 7, 0)
        .await
        .unwrap();
    let borrowed: &[u8] = held.data();
    assert_eq!(borrowed, b"abcd");
    let snapshot = borrowed.to_vec();
    assert_ne!(snapshot.as_ptr(), borrowed.as_ptr());
    assert_eq!(store.lock().await.reciprocal_available(), 0);
    drop(held);
    assert_eq!(store.lock().await.reciprocal_available(), 1);
    assert_eq!(snapshot, b"abcd");
}

#[tokio::test]
async fn legacy_replacement_keeps_held_generation_and_header_snapshot_immutable() {
    let store = Arc::new(Mutex::new(PieceStore::new(8, 4, 8)));
    store.lock().await.put_chunk(7, 0, b"abcd");
    let old = PieceStore::shared_serving_chunk(&store, 7, 0)
        .await
        .unwrap();
    store
        .lock()
        .await
        .put_chunk_with_header(7, 0, [2; 8], b"WXYZ");
    let current = PieceStore::shared_serving_chunk(&store, 7, 0)
        .await
        .unwrap();
    assert_eq!(store.lock().await.cur_bytes, 4);
    assert_eq!(old.0.as_ref(), b"abcd");
    assert_eq!(old.1, [0; 8]);
    assert_eq!(current.0.as_ref(), b"WXYZ");
    assert_eq!(current.1, [2; 8]);
    store.lock().await.put_chunk(8, 0, b"1234");
    store.lock().await.put_chunk(9, 0, b"5678");
    assert_eq!(store.lock().await.cur_bytes, 8);
    assert!(!store.lock().await.has_piece(7));
    assert!(old.0.is_unique());
    assert!(current.0.is_unique());
    assert_eq!(current.0.as_ref(), b"WXYZ");
}
