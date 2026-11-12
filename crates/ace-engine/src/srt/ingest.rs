//! SRT connections use the existing exclusive signed-broadcast authority.
use super::owner::NativeIncoming;
use crate::{
    broadcast::{BroadcastError, IngressTask, ProducerKind},
    http::BroadcastState,
};

pub(crate) async fn attach_connection(
    mut incoming: NativeIncoming,
    state: BroadcastState,
) -> Result<IngressTask, BroadcastError> {
    let name = incoming.name.clone();
    // Install the immutable physical-retirement fence atomically with acquisition.
    // Cancellation during a later await cannot expose an unfenced producer claim.
    let (broadcast, session) = state
        .registry
        .acquire_producer_with_retirement(
            &name,
            &state.trackers,
            &state.seed_registry,
            state.store_bytes,
            ProducerKind::Srt,
            incoming.retirement.clone(),
        )
        .await?;
    state.ensure_announcing(&name).await?;
    state.registry.spawn_ingress(&name, session, move |mut session| async move {
        let mut cancelled = session.cancellation();
        let mut generation_cancelled = session.generation_cancellation();
        let mut ingest = match session.open_ingest() {
            Ok(ingest) => ingest,
            Err(_) => { crate::alog!("[srt] ingest_open_failed"); return; }
        };
        if !incoming.admit() { return; }
        loop {
            let bytes = tokio::select! { biased;
                () = crate::broadcast::cancelled(&mut cancelled) => break,
                () = crate::broadcast::cancelled(&mut generation_cancelled) => break,
                bytes = incoming.receiver.recv() => match bytes { Some(bytes) => bytes, None => break },
            };
            let result = tokio::select! { biased;
                () = crate::broadcast::cancelled(&mut cancelled) => break,
                () = crate::broadcast::cancelled(&mut generation_cancelled) => break,
                result = ingest.push_bytes(&bytes) => result,
            };
            if result.is_err() { crate::alog!("[srt] ingest_failed"); break; }
        }
        // SRT disconnect is a live boundary, not finite EOF. Discard partial TS
        // and signing tails; the existing publication jobs retain their fences.
        drop(ingest);
        drop(incoming);
        // Persist only after admitted writes settle. Keep the producer alive
        // through this checkpoint; its transport fence independently owns close.
        if broadcast.cursor.flush().await.is_err() {
            crate::alog!("[srt] cursor_checkpoint_failed");
        }
    })
}
