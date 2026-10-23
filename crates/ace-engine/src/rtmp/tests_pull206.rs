//! RTMP callback identity and operation-barrier regressions against the real registry.

use super::{RtmpIngestHandler, RtmpOperation};
use crate::broadcast::BroadcastRegistry;
use crate::http::BroadcastState;
use ace_swarm::listen::SeedRegistry;
use rtmp_rs::protocol::message::PublishParams;
use rtmp_rs::server::handler::{AuthResult, RtmpHandler};
use rtmp_rs::session::{SessionContext, StreamContext};
use std::sync::Arc;
use tokio::sync::watch;

fn state() -> BroadcastState {
    BroadcastState {
        registry: BroadcastRegistry::new(),
        seed_registry: SeedRegistry::new(),
        trackers: vec![],
        store_bytes: 2 << 20,
        inbound_peer_port: watch::channel(None).1,
    }
}

fn session(id: u64) -> SessionContext {
    SessionContext::new(id, "127.0.0.1:40000".parse().unwrap())
}

fn publish(stream_key: &str) -> PublishParams {
    PublishParams {
        stream_key: stream_key.to_owned(),
        publish_type: "live".to_owned(),
        stream_id: 1,
    }
}

#[tokio::test]
async fn pull206_stale_rtmp_callbacks_cannot_touch_replacement() {
    let state = state();
    let handler = Arc::new(RtmpIngestHandler::new(state.clone()));
    let old_session = session(10);
    assert!(matches!(
        handler.on_publish(&old_session, &publish("channel")).await,
        AuthResult::Accept
    ));
    let old_generation = handler.streams.lock().unwrap()["channel"].generation;

    state.delete("channel").await.unwrap();
    let replacement_session = session(11);
    assert!(matches!(
        handler
            .on_publish(&replacement_session, &publish("channel"))
            .await,
        AuthResult::Accept
    ));
    let replacement = handler.streams.lock().unwrap()["channel"].clone();
    assert_ne!(replacement.generation, old_generation);

    handler
        .on_unpublish(&StreamContext::new(
            old_session,
            1,
            "channel".to_owned(),
            true,
        ))
        .await;
    assert!(Arc::ptr_eq(
        &handler.streams.lock().unwrap()["channel"],
        &replacement
    ));
    assert!(state
        .acquire_producer("channel", crate::broadcast::ProducerKind::Pull)
        .await
        .is_err());

    assert!(matches!(
        handler
            .on_publish(&session(12), &publish(" channel "))
            .await,
        AuthResult::Reject(_)
    ));
    assert!(matches!(
        handler.on_publish(&session(12), &publish("channel")).await,
        AuthResult::Reject(_)
    ));
    assert!(Arc::ptr_eq(
        &handler.streams.lock().unwrap()["channel"],
        &replacement
    ));
}

#[tokio::test]
async fn pull206_rtmp_operation_cleanup_barrier() {
    let state = state();
    let handler = Arc::new(RtmpIngestHandler::new(state.clone()));
    let owner = session(21);
    assert!(matches!(
        handler.on_publish(&owner, &publish("gated")).await,
        AuthResult::Accept
    ));

    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let entered_tx = Arc::new(std::sync::Mutex::new(Some(entered_tx)));
    let release_rx = Arc::new(std::sync::Mutex::new(Some(release_rx)));
    handler.set_operation_hook(Arc::new(move |operation| {
        let entered_tx = if operation == RtmpOperation::Finish {
            entered_tx.lock().unwrap().take()
        } else {
            None
        };
        let release_rx = release_rx.clone();
        Box::pin(async move {
            if let Some(entered_tx) = entered_tx {
                let _ = entered_tx.send(());
                let release_rx = { release_rx.lock().unwrap().take() };
                if let Some(release_rx) = release_rx {
                    let _ = release_rx.await;
                }
            }
        })
    }));

    let callback = tokio::spawn({
        let handler = handler.clone();
        async move {
            handler
                .on_unpublish(&StreamContext::new(owner, 1, "gated".to_owned(), true))
                .await;
        }
    });
    tokio::time::timeout(std::time::Duration::from_secs(2), entered_rx)
        .await
        .unwrap()
        .unwrap();

    let delete = state.delete("gated");
    tokio::pin!(delete);
    assert!(
        futures::poll!(delete.as_mut()).is_pending(),
        "delete waits for the in-flight RTMP operation"
    );
    assert!(
        state.registry.get("gated").await.is_none(),
        "stopping hides descriptor immediately"
    );
    release_tx.send(()).unwrap();
    callback.await.unwrap();
    delete.await.unwrap();
    assert!(!handler.streams.lock().unwrap().contains_key("gated"));
    assert!(state.registry.get("gated").await.is_none());
}
