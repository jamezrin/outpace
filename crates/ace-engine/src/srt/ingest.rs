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
        if let Err(error) = finish_cursor(&broadcast.cursor).await {
            crate::alog!("[srt] cursor_checkpoint_failed: {}", error.code());
        }
    })
}

async fn finish_cursor(cursor: &crate::broadcast::BroadcastCursor) -> Result<(), BroadcastError> {
    match cursor.flush().await {
        // Registry shutdown owns the final durable record after closing admission.
        // Preserve actual I/O poison/errors, which cursor.check reports before stop.
        Err(BroadcastError::Stopping) => Ok(()),
        result => result,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::broadcast::{BroadcastRegistry, ProducerKind};
    use ace_swarm::listen::SeedRegistry;

    #[tokio::test]
    async fn normal_registry_stop_does_not_report_an_ingest_checkpoint_failure() {
        let registry = BroadcastRegistry::new();
        let (broadcast, producer) = registry
            .acquire_producer(
                "synthetic-finish",
                &[],
                &SeedRegistry::new(),
                1 << 20,
                ProducerKind::Srt,
            )
            .await
            .unwrap();
        registry.close_admissions();
        let result = finish_cursor(&broadcast.cursor).await;
        drop(producer);
        registry.shutdown().await.unwrap();
        assert!(
            result.is_ok(),
            "normal stopping is not a persistence failure"
        );
    }

    #[tokio::test]
    async fn stopping_does_not_hide_a_real_checkpoint_io_failure() {
        use std::os::unix::fs::DirBuilderExt;
        struct Temp(std::path::PathBuf);
        impl Drop for Temp {
            fn drop(&mut self) {
                let _ = std::fs::remove_dir_all(&self.0);
            }
        }
        let path = std::env::temp_dir().join(format!("outpace-srt-io-{}", rand::random::<u64>()));
        std::fs::DirBuilder::new()
            .mode(0o700)
            .create(&path)
            .unwrap();
        let temp = Temp(path);
        let registry = BroadcastRegistry::with_persist(
            &temp.0,
            crate::config::CacheType::Memory,
            temp.0.join("cache"),
        );
        let (broadcast, producer) = registry
            .acquire_producer(
                "synthetic-io",
                &[],
                &SeedRegistry::new(),
                1 << 20,
                ProducerKind::Srt,
            )
            .await
            .unwrap();
        let directory = temp.0.join("broadcasts");
        let saved = temp.0.join("saved");
        std::fs::rename(&directory, &saved).unwrap();
        std::fs::write(&directory, b"not-a-directory").unwrap();
        let failed = finish_cursor(&broadcast.cursor).await;
        registry.close_admissions();
        let stopped = finish_cursor(&broadcast.cursor).await;
        std::fs::remove_file(&directory).unwrap();
        std::fs::rename(saved, directory).unwrap();
        drop(producer);
        let _ = registry.shutdown().await;
        assert!(matches!(failed, Err(BroadcastError::PersistenceFailed)));
        assert!(
            matches!(stopped, Err(BroadcastError::PersistenceFailed)),
            "real failure must remain visible after cancellation"
        );
    }
}
