use super::*;
use ace_wire::handshake::Handshake;
use std::sync::Barrier;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::time::{timeout, Duration};

fn policy(global: usize, per_stream: usize, unchoked: usize) -> InboundAdmission {
    InboundPolicy {
        max_inbound: global,
        max_per_stream: per_stream,
        max_unchoked: unchoked,
    }
    .admission()
}

fn store() -> SharedStore {
    Arc::new(tokio::sync::Mutex::new(PieceStore::new(4, 4, 1024)))
}

fn identity() -> Arc<Identity> {
    Arc::new(Identity::from_seed([7; 32]))
}

fn peer_handshake(key: [u8; 20]) -> [u8; ace_wire::handshake::HANDSHAKE_LEN] {
    Handshake::new(key, [9; 20]).encode()
}

#[test]
fn canonical_capacity_is_atomic() {
    let registry = SeedRegistry::new();
    let key = [1; 20];
    registry.register(key, store());
    let admission = policy(32, 4, 0);
    let barrier = Arc::new(Barrier::new(13));
    let mut threads = Vec::new();
    for _ in 0..12 {
        let registry = registry.clone();
        let admission = admission.clone();
        let barrier = barrier.clone();
        threads.push(std::thread::spawn(move || {
            barrier.wait();
            registry.admit_inbound(&key, &admission)
        }));
    }
    barrier.wait();
    let mut permits: Vec<_> = threads.into_iter().map(|t| t.join().unwrap()).collect();
    assert_eq!(permits.iter().filter(|p| p.is_some()).count(), 4);
    assert_eq!(permits.iter().filter(|p| p.is_none()).count(), 8);
    assert_eq!(
        admission.accounting.lock().unwrap().counts.get(&key),
        Some(&4)
    );

    let other = [2; 20];
    registry.register(other, store());
    let independent = registry.admit_inbound(&other, &admission);
    assert!(
        independent.is_some(),
        "different canonical streams have independent N"
    );

    let released = permits.iter_mut().find_map(Option::take).unwrap();
    drop(released);
    let replacement = registry.admit_inbound(&key, &admission).unwrap();
    assert!(registry.admit_inbound(&key, &admission).is_none());
    let next_released = permits.iter_mut().find_map(Option::take).unwrap();
    drop(next_released);
    let second_replacement = registry.admit_inbound(&key, &admission).unwrap();
    assert!(registry.admit_inbound(&key, &admission).is_none());
    drop((replacement, second_replacement));

    let unknown = [3; 20];
    assert!(registry.admit_inbound(&unknown, &admission).is_none());
    let empty = [4; 20];
    registry.stores.lock().unwrap().insert(
        empty,
        SeedEntry {
            generation: next_generation(),
            ..Default::default()
        },
    );
    assert!(registry.admit_inbound(&empty, &admission).is_none());
    assert!(!admission
        .accounting
        .lock()
        .unwrap()
        .counts
        .contains_key(&empty));

    drop(independent);
    drop(permits);
    assert!(!admission
        .accounting
        .lock()
        .unwrap()
        .counts
        .contains_key(&key));
    assert!(admission.accounting.lock().unwrap().bindings.is_empty());
}

#[test]
fn aliases_share_capacity_across_generations() {
    let registry = SeedRegistry::new();
    let media = [11; 20];
    let metadata_key = [12; 20];
    let admission = policy(8, 2, 8);
    let (media_store, first_lease) =
        registry.lease_broadcast(media, metadata_key, b"synthetic metadata".to_vec(), || {
            PieceStore::new(4, 4, 1024)
        });
    let (same_store, second_lease) =
        registry.lease_broadcast(media, metadata_key, b"synthetic metadata".to_vec(), || {
            panic!("repeated broadcast leases must reuse the existing store")
        });
    assert!(Arc::ptr_eq(&media_store, &same_store));

    let media_permit = registry.admit_inbound(&media, &admission).unwrap();
    let metadata_permit = registry.admit_inbound(&metadata_key, &admission).unwrap();
    assert!(registry.admit_inbound(&media, &admission).is_none());
    registry.remove(&media);
    registry.remove(&metadata_key);
    assert!(!registry.serves(&media) && !registry.serves(&metadata_key));

    let (_replacement_store, replacement_lease) = registry.lease_broadcast(
        media,
        metadata_key,
        b"replacement metadata".to_vec(),
        || PieceStore::new(4, 4, 1024),
    );
    drop(first_lease);
    drop(second_lease);
    assert!(registry.serves(&media) && registry.serves(&metadata_key));
    assert!(registry.admit_inbound(&media, &admission).is_none());
    assert!(registry.admit_inbound(&metadata_key, &admission).is_none());

    // The old alias binding remains while old canonical occupants exist. Releasing one old
    // connection permits exactly one new connection, and stale-generation producer release
    // cannot remove the replacement entry.
    drop(media_permit);
    let new_connection = registry.admit_inbound(&metadata_key, &admission).unwrap();
    drop(metadata_permit);
    assert_eq!(
        admission.accounting.lock().unwrap().counts.get(&media),
        Some(&1)
    );
    let final_slot = registry.admit_inbound(&media, &admission).unwrap();
    assert!(registry.admit_inbound(&metadata_key, &admission).is_none());
    drop((new_connection, final_slot));
    drop(replacement_lease);
}

#[test]
fn alias_reassociation_fails_closed_and_keeps_unused_alias_binding() {
    let registry = SeedRegistry::new();
    let first_media = [21; 20];
    let second_media = [22; 20];
    let metadata_key = [23; 20];
    let admission = policy(8, 0, 8);
    let (_store, _lease) =
        registry.lease_broadcast(first_media, metadata_key, b"metadata".to_vec(), || {
            PieceStore::new(4, 4, 1024)
        });
    let old_media_occupant = registry.admit_inbound(&first_media, &admission).unwrap();
    registry.remove(&metadata_key);
    registry.remove(&first_media);

    // Even though only the media key has an occupant, its unoccupied metadata alias remains bound.
    assert!(registry.admit_inbound(&first_media, &admission).is_none());
    let (_new_store, _new_lease) = registry.lease_broadcast(
        second_media,
        metadata_key,
        b"metadata reassociated".to_vec(),
        || PieceStore::new(4, 4, 1024),
    );
    assert!(registry.admit_inbound(&metadata_key, &admission).is_none());
    assert!(registry.admit_inbound(&second_media, &admission).is_none());
    assert_eq!(
        admission
            .accounting
            .lock()
            .unwrap()
            .counts
            .get(&first_media),
        Some(&1)
    );
    drop(old_media_occupant);
    assert!(admission.accounting.lock().unwrap().counts.is_empty());
    assert!(admission.accounting.lock().unwrap().bindings.is_empty());

    let ordinary = [24; 20];
    registry.register(ordinary, store());
    registry.register_metadata(ordinary, b"ordinary metadata".to_vec());
    let ordinary_occupant = registry.admit_inbound(&ordinary, &admission).unwrap();
    let (_store, _lease) =
        registry.lease_broadcast([25; 20], ordinary, b"conflicting lease".to_vec(), || {
            PieceStore::new(4, 4, 1024)
        });
    assert!(registry.admit_inbound(&ordinary, &admission).is_none());
    drop(ordinary_occupant);
}

#[test]
fn late_broadcast_alias_registration_stays_bound_to_existing_occupant() {
    let registry = SeedRegistry::new();
    let old_media = [26; 20];
    let new_media = [27; 20];
    let alias = [28; 20];
    let admission = policy(8, 0, 8);
    registry.register(old_media, store());
    let old_occupant = registry.admit_inbound(&old_media, &admission).unwrap();

    // The media occupant predates broadcast registration, so the alias must be retained into its
    // existing accounting owner even though no snapshot originally knew about this alias.
    let (_store, _old_lease) =
        registry.lease_broadcast(old_media, alias, b"late alias".to_vec(), || {
            panic!("broadcast lease reuses the registered media store")
        });
    assert_eq!(
        admission.accounting.lock().unwrap().bindings.get(&alias),
        Some(&old_media)
    );

    registry.remove(&old_media);
    registry.remove(&alias);
    let (_new_store, _new_lease) =
        registry.lease_broadcast(new_media, alias, b"reassociated alias".to_vec(), || {
            PieceStore::new(4, 4, 1024)
        });
    assert!(registry.admit_inbound(&alias, &admission).is_none());
    assert!(registry.admit_inbound(&new_media, &admission).is_none());
    assert_eq!(
        admission.accounting.lock().unwrap().counts.get(&old_media),
        Some(&1)
    );
    assert!(!admission
        .accounting
        .lock()
        .unwrap()
        .counts
        .contains_key(&new_media));

    drop(old_occupant);
    assert!(admission.accounting.lock().unwrap().bindings.is_empty());
    let replacement = registry.admit_inbound(&new_media, &admission).unwrap();
    drop(replacement);
}

#[tokio::test]
async fn zero_and_large_n_preserve_global_limit_and_nonblocking_accept() {
    let registry = SeedRegistry::new();
    let key = [31; 20];
    registry.register(key, store());
    let admission = policy(20, 0, 8);
    let mut futures = Vec::new();
    for _ in 0..20 {
        let (server, _client) = tokio::io::duplex(128);
        futures.push(
            admission
                .accept(
                    server,
                    registry.clone(),
                    [1; 20],
                    [0; 8],
                    identity(),
                    [127, 0, 0, 1],
                )
                .expect("global permits should be available below G"),
        );
    }
    let (server, _client) = tokio::io::duplex(128);
    assert!(admission
        .accept(
            server,
            registry.clone(),
            [1; 20],
            [0; 8],
            identity(),
            [127, 0, 0, 1]
        )
        .is_none());
    drop(futures);
    assert_eq!(admission.global.available_permits(), 20);

    let above_global = policy(0, 99, 8);
    let (server, _client) = tokio::io::duplex(128);
    let retained = above_global
        .accept(
            server,
            registry,
            [1; 20],
            [0; 8],
            identity(),
            [127, 0, 0, 1],
        )
        .expect("configured global zero is one");
    let (server, _client) = tokio::io::duplex(128);
    assert!(above_global
        .accept(
            server,
            SeedRegistry::new(),
            [1; 20],
            [0; 8],
            identity(),
            [127, 0, 0, 1]
        )
        .is_none());
    drop(retained);
    assert_eq!(above_global.global.available_permits(), 1);
}

async fn connect_handler(
    admission: &InboundAdmission,
    registry: SeedRegistry,
    key: [u8; 20],
    capacity: usize,
) -> (
    tokio::io::DuplexStream,
    tokio::task::JoinHandle<ace_peer::Result<()>>,
) {
    let (client, server) = tokio::io::duplex(capacity);
    let future = admission
        .accept(
            server,
            registry,
            [8; 20],
            [0; 8],
            identity(),
            [127, 0, 0, 1],
        )
        .expect("global capacity");
    let task = tokio::spawn(future);
    let mut client = client;
    client.write_all(&peer_handshake(key)).await.unwrap();
    (client, task)
}

async fn expect_handshake_reply(client: &mut tokio::io::DuplexStream) {
    let mut bytes = [0; ace_wire::handshake::HANDSHAKE_LEN];
    timeout(Duration::from_secs(1), client.read_exact(&mut bytes))
        .await
        .expect("server handshake response deadline")
        .unwrap();
}

async fn expect_closed_without_reply(client: &mut tokio::io::DuplexStream) {
    let mut bytes = [0; ace_wire::handshake::HANDSHAKE_LEN];
    let read = timeout(Duration::from_secs(1), client.read(&mut bytes))
        .await
        .expect("rejected handshake close deadline")
        .unwrap();
    assert_eq!(read, 0, "rejection must close without a handshake response");
}

#[tokio::test]
async fn handler_rejects_without_reply_and_shares_media_metadata_capacity() {
    let registry = SeedRegistry::new();
    let media = [41; 20];
    let metadata_key = [42; 20];
    let admission = policy(4, 1, 8);
    let (_broadcast_store, _broadcast_lease) =
        registry.lease_broadcast(media, metadata_key, b"metadata".to_vec(), || {
            PieceStore::new(4, 4, 1024)
        });

    let (mut media_client, media_task) =
        connect_handler(&admission, registry.clone(), media, 128).await;
    expect_handshake_reply(&mut media_client).await;
    // The signed extended handshake is the first serving message and proves the real handler
    // retained the admission snapshot after replying to the peer handshake.
    let mut message_client = PeerSession::new(media_client);
    assert!(matches!(
        timeout(Duration::from_secs(1), message_client.read_message())
            .await
            .unwrap()
            .unwrap(),
        ace_wire::message::PeerMessage::Extended { ext_id: 0, .. }
    ));

    let (mut metadata_client, metadata_task) =
        connect_handler(&admission, registry.clone(), metadata_key, 128).await;
    expect_closed_without_reply(&mut metadata_client).await;
    assert!(metadata_task.await.unwrap().is_err());
    assert_eq!(admission.global.available_permits(), 3);

    drop(message_client);
    media_task.abort();
    let _ = media_task.await;
    assert_eq!(admission.global.available_permits(), 4);

    let unknown_key = [43; 20];
    let unknown_registry = SeedRegistry::new();
    let (mut unknown_client, unknown_task) =
        connect_handler(&admission, unknown_registry, unknown_key, 128).await;
    expect_closed_without_reply(&mut unknown_client).await;
    assert!(unknown_task.await.unwrap().is_err());
    assert_eq!(admission.global.available_permits(), 4);

    let conflicted_registry = SeedRegistry::new();
    let old_media = [44; 20];
    let new_media = [45; 20];
    let shared_alias = [46; 20];
    let (_old_store, _old_lease) = conflicted_registry.lease_broadcast(
        old_media,
        shared_alias,
        b"old association".to_vec(),
        || PieceStore::new(4, 4, 1024),
    );
    let old_occupant = conflicted_registry
        .admit_inbound(&old_media, &admission)
        .unwrap();
    conflicted_registry.remove(&old_media);
    conflicted_registry.remove(&shared_alias);
    let (_new_store, _new_lease) = conflicted_registry.lease_broadcast(
        new_media,
        shared_alias,
        b"new association".to_vec(),
        || PieceStore::new(4, 4, 1024),
    );
    let (mut inconsistent_client, inconsistent_task) =
        connect_handler(&admission, conflicted_registry.clone(), shared_alias, 128).await;
    expect_closed_without_reply(&mut inconsistent_client).await;
    assert!(inconsistent_task.await.unwrap().is_err());
    assert_eq!(admission.global.available_permits(), 4);
    assert!(!admission
        .accounting
        .lock()
        .unwrap()
        .counts
        .contains_key(&new_media));
    drop(old_occupant);
}

#[tokio::test]
async fn handler_removal_is_coherent_and_uses_required_coordinator() {
    let registry = SeedRegistry::new();
    let key = [51; 20];
    let shared = store();
    shared.lock().await.put_chunk(5, 0, &[5; 4]);
    registry.register(key, shared);
    let admission = policy(4, 1, 1);

    // Removal before admission rejects without sending the handshake response.
    registry.remove(&key);
    let (mut rejected_client, rejected_task) =
        connect_handler(&admission, registry.clone(), key, 128).await;
    expect_closed_without_reply(&mut rejected_client).await;
    assert!(rejected_task.await.unwrap().is_err());

    // Re-register and admit; removal after the predicate must not invalidate the captured store
    // or coordinator. Interest must still be driven through the exact key's coordinator.
    let shared = store();
    shared.lock().await.put_chunk(5, 0, &[5; 4]);
    registry.register(key, shared);
    let (mut client, task) = connect_handler(&admission, registry.clone(), key, 256).await;
    expect_handshake_reply(&mut client).await;
    registry.remove(&key);
    let mut peer = PeerSession::new(client);
    assert!(matches!(
        timeout(Duration::from_secs(1), peer.read_message())
            .await
            .unwrap()
            .unwrap(),
        ace_wire::message::PeerMessage::Extended { ext_id: 0, .. }
    ));
    peer.send(&ace_wire::message::PeerMessage::Extended {
        ext_id: 0,
        payload: b"d1:md11:ut_metadatai2eee".to_vec(),
    })
    .await
    .unwrap();
    assert!(matches!(
        timeout(Duration::from_secs(1), peer.read_message())
            .await
            .unwrap()
            .unwrap(),
        ace_wire::message::PeerMessage::Bitfield(_)
    ));
    peer.send(&ace_wire::message::PeerMessage::Interested)
        .await
        .unwrap();
    assert_eq!(
        timeout(Duration::from_secs(1), peer.read_message())
            .await
            .unwrap()
            .unwrap(),
        ace_wire::message::PeerMessage::Unchoke
    );
    peer.send(&ace_wire::live_codec::chunk_request(5, 0))
        .await
        .unwrap();
    let ace_wire::message::PeerMessage::Piece { block, .. } =
        timeout(Duration::from_secs(1), peer.read_message())
            .await
            .unwrap()
            .unwrap()
    else {
        panic!("admitted handler must keep serving the removed snapshot's piece data");
    };
    assert!(block.ends_with(&[5; 4]));
    peer.send(&ace_wire::message::PeerMessage::NotInterested)
        .await
        .unwrap();
    assert_eq!(
        timeout(Duration::from_secs(1), peer.read_message())
            .await
            .unwrap()
            .unwrap(),
        ace_wire::message::PeerMessage::Choke,
        "the production coordinator must react to NotInterested after removal"
    );
    drop(peer);
    task.abort();
    let _ = task.await;
    assert_eq!(admission.global.available_permits(), 4);
}

#[tokio::test]
async fn handler_releases_canonical_and_global_permits_after_eof_and_cancellation() {
    let registry = SeedRegistry::new();
    let key = [61; 20];
    registry.register(key, store());
    let admission = policy(1, 1, 8);
    let (mut client, task) = connect_handler(&admission, registry.clone(), key, 128).await;
    expect_handshake_reply(&mut client).await;
    assert!(admission
        .accounting
        .lock()
        .unwrap()
        .counts
        .contains_key(&key));
    drop(client);
    let handler_result = timeout(Duration::from_secs(1), task)
        .await
        .expect("handler EOF completion")
        .unwrap();
    assert!(handler_result.is_err(), "serving EOF exits the handler");
    assert!(admission.accounting.lock().unwrap().counts.is_empty());
    assert_eq!(admission.global.available_permits(), 1);

    let (mut client, task) = connect_handler(&admission, registry.clone(), key, 128).await;
    expect_handshake_reply(&mut client).await;
    task.abort();
    let _ = task.await;
    assert!(admission.accounting.lock().unwrap().counts.is_empty());
    assert_eq!(admission.global.available_permits(), 1);
    let replacement = registry.admit_inbound(&key, &admission).unwrap();
    drop(replacement);
}

#[tokio::test]
async fn pre_admission_handshake_failure_leaves_no_canonical_reservation() {
    let registry = SeedRegistry::new();
    let key = [63; 20];
    registry.register(key, store());
    let admission = policy(1, 1, 8);
    let (client, server) = tokio::io::duplex(128);
    let task = tokio::spawn(
        admission
            .accept(
                server,
                registry.clone(),
                [8; 20],
                [0; 8],
                identity(),
                [127, 0, 0, 1],
            )
            .unwrap(),
    );
    let mut client = client;
    client.write_all(&[0u8; 16]).await.unwrap();
    client.shutdown().await.unwrap();
    let mut response = [0u8; 1];
    assert_eq!(
        timeout(Duration::from_secs(1), client.read(&mut response))
            .await
            .unwrap()
            .unwrap(),
        0
    );
    assert!(task.await.unwrap().is_err());
    assert!(admission.accounting.lock().unwrap().counts.is_empty());
    assert_eq!(admission.global.available_permits(), 1);
}

struct FailHandshakeReply<S>(S);

impl<S: AsyncRead + Unpin> AsyncRead for FailHandshakeReply<S> {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.0).poll_read(cx, buf)
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for FailHandshakeReply<S> {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        _buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        std::task::Poll::Ready(Err(std::io::Error::other("injected handshake write error")))
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.0).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.0).poll_shutdown(cx)
    }
}

struct FailAfterHandshakePrefix<S> {
    inner: S,
    read: usize,
}

impl<S: AsyncRead + Unpin> AsyncRead for FailAfterHandshakePrefix<S> {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        if self.read >= ace_wire::handshake::HANDSHAKE_PREFIX_LEN {
            return std::task::Poll::Ready(Err(std::io::Error::other(
                "injected peer-id read error after handshake admission",
            )));
        }
        let before = buf.filled().len();
        let result = std::pin::Pin::new(&mut self.inner).poll_read(cx, buf);
        if result.is_ready() {
            self.read += buf.filled().len() - before;
        }
        result
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for FailAfterHandshakePrefix<S> {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        std::pin::Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

#[tokio::test]
async fn handshake_peer_id_read_failure_after_predicate_releases_permits() {
    let registry = SeedRegistry::new();
    let key = [64; 20];
    registry.register(key, store());
    let admission = policy(1, 1, 8);
    let (client, server) = tokio::io::duplex(128);
    let task = tokio::spawn(
        admission
            .accept(
                FailAfterHandshakePrefix {
                    inner: server,
                    read: 0,
                },
                registry.clone(),
                [8; 20],
                [0; 8],
                identity(),
                [127, 0, 0, 1],
            )
            .unwrap(),
    );
    let mut client = client;
    client.write_all(&peer_handshake(key)).await.unwrap();
    assert!(task.await.unwrap().is_err());
    assert!(admission.accounting.lock().unwrap().counts.is_empty());
    assert_eq!(admission.global.available_permits(), 1);
    let replacement = registry.admit_inbound(&key, &admission).unwrap();
    drop(replacement);
}

#[tokio::test]
async fn handshake_reply_failure_after_predicate_releases_both_permits() {
    let registry = SeedRegistry::new();
    let key = [62; 20];
    registry.register(key, store());
    let admission = policy(1, 1, 8);
    let (client, server) = tokio::io::duplex(128);
    let task = tokio::spawn(
        admission
            .accept(
                FailHandshakeReply(server),
                registry.clone(),
                [8; 20],
                [0; 8],
                identity(),
                [127, 0, 0, 1],
            )
            .unwrap(),
    );
    let mut client = client;
    client.write_all(&peer_handshake(key)).await.unwrap();
    assert!(task.await.unwrap().is_err());
    assert!(admission.accounting.lock().unwrap().counts.is_empty());
    assert_eq!(admission.global.available_permits(), 1);
    let replacement = registry.admit_inbound(&key, &admission).unwrap();
    drop(replacement);
}

struct PendingWriteSignal<S> {
    inner: S,
    armed: Arc<std::sync::atomic::AtomicBool>,
    pending: Arc<tokio::sync::Notify>,
}

impl<S: tokio::io::AsyncRead + Unpin> tokio::io::AsyncRead for PendingWriteSignal<S> {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl<S: tokio::io::AsyncWrite + Unpin> tokio::io::AsyncWrite for PendingWriteSignal<S> {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        let result = std::pin::Pin::new(&mut self.inner).poll_write(cx, buf);
        if result.is_pending() && self.armed.load(std::sync::atomic::Ordering::SeqCst) {
            self.pending.notify_one();
        }
        result
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

#[tokio::test]
async fn handler_backpressure_retains_permits_until_cancelled() {
    let registry = SeedRegistry::new();
    let key = [66; 20];
    let shared = store();
    shared.lock().await.put_chunk(5, 0, &[6; 4]);
    registry.register(key, shared);
    let admission = policy(1, 1, 1);
    let (client, server) = tokio::io::duplex(8);
    let armed = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let pending = Arc::new(tokio::sync::Notify::new());
    let stream = PendingWriteSignal {
        inner: server,
        armed: armed.clone(),
        pending: pending.clone(),
    };
    let task = tokio::spawn(
        admission
            .accept(
                stream,
                registry.clone(),
                [8; 20],
                [0; 8],
                identity(),
                [127, 0, 0, 1],
            )
            .unwrap(),
    );
    let mut raw = client;
    raw.write_all(&peer_handshake(key)).await.unwrap();
    // Drive the actual inbound handshake and signed seeder bootstrap through the narrow duplex.
    let mut reply = [0; ace_wire::handshake::HANDSHAKE_LEN];
    timeout(Duration::from_secs(1), raw.read_exact(&mut reply))
        .await
        .unwrap()
        .unwrap();
    let mut peer = PeerSession::new(raw);
    assert!(matches!(
        timeout(Duration::from_secs(1), peer.read_message())
            .await
            .unwrap()
            .unwrap(),
        ace_wire::message::PeerMessage::Extended { ext_id: 0, .. }
    ));
    peer.send(&ace_wire::message::PeerMessage::Extended {
        ext_id: 0,
        payload: b"d1:md11:ut_metadatai2eee".to_vec(),
    })
    .await
    .unwrap();
    assert!(matches!(
        timeout(Duration::from_secs(1), peer.read_message())
            .await
            .unwrap()
            .unwrap(),
        ace_wire::message::PeerMessage::Bitfield(_)
    ));
    peer.send(&ace_wire::message::PeerMessage::Interested)
        .await
        .unwrap();
    assert_eq!(
        timeout(Duration::from_secs(1), peer.read_message())
            .await
            .unwrap()
            .unwrap(),
        ace_wire::message::PeerMessage::Unchoke
    );

    armed.store(true, std::sync::atomic::Ordering::SeqCst);
    peer.send(&ace_wire::live_codec::chunk_request(5, 0))
        .await
        .unwrap();
    timeout(Duration::from_secs(1), pending.notified())
        .await
        .expect("actual prepared piece reply must reach a blocked duplex write");
    assert!(registry.admit_inbound(&key, &admission).is_none());
    assert_eq!(admission.global.available_permits(), 0);

    task.abort();
    let _ = task.await;
    assert_eq!(admission.global.available_permits(), 1);
    assert!(admission.accounting.lock().unwrap().counts.is_empty());
    let replacement = registry.admit_inbound(&key, &admission).unwrap();
    drop(replacement);
}

#[tokio::test]
async fn compatibility_policy_uses_zero_stream_limit_and_global_acquisition() {
    let selected = PeerListener::compatibility_policy(2, 3);
    assert_eq!(selected.max_inbound, 2);
    assert_eq!(selected.max_per_stream, 0);
    assert_eq!(selected.max_unchoked, 3);
    let admission = selected.admission();
    let registry = SeedRegistry::new();
    let key = [71; 20];
    let content = store();
    content.lock().await.put_chunk(5, 0, &[7; 4]);
    registry.register(key, content);
    let first = registry.admit_inbound(&key, &admission).unwrap();
    let second = registry.admit_inbound(&key, &admission).unwrap();
    assert!(admission
        .accounting
        .lock()
        .unwrap()
        .counts
        .contains_key(&key));
    drop((first, second));
    assert!(!admission
        .accounting
        .lock()
        .unwrap()
        .counts
        .contains_key(&key));

    let (mut client, task) = connect_handler(&admission, registry, key, 128).await;
    expect_handshake_reply(&mut client).await;
    let mut peer = PeerSession::new(client);
    assert!(matches!(
        timeout(Duration::from_secs(1), peer.read_message())
            .await
            .unwrap()
            .unwrap(),
        ace_wire::message::PeerMessage::Extended { ext_id: 0, .. }
    ));
    peer.send(&ace_wire::message::PeerMessage::Extended {
        ext_id: 0,
        payload: b"d1:md11:ut_metadatai2eee".to_vec(),
    })
    .await
    .unwrap();
    assert!(matches!(
        timeout(Duration::from_secs(1), peer.read_message())
            .await
            .unwrap()
            .unwrap(),
        ace_wire::message::PeerMessage::Bitfield(_)
    ));
    peer.send(&ace_wire::message::PeerMessage::Interested)
        .await
        .unwrap();
    assert_eq!(
        timeout(Duration::from_secs(1), peer.read_message())
            .await
            .unwrap()
            .unwrap(),
        ace_wire::message::PeerMessage::Unchoke
    );
    peer.send(&ace_wire::message::PeerMessage::NotInterested)
        .await
        .unwrap();
    assert_eq!(
        timeout(Duration::from_secs(1), peer.read_message())
            .await
            .unwrap()
            .unwrap(),
        ace_wire::message::PeerMessage::Choke,
        "the compatibility handler must retain the coordinator-required behavior"
    );
    drop(peer);
    task.abort();
    let _ = task.await;
}
