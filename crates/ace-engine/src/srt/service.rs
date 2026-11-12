//! One listener driver; broadcast entries retain ownership of admitted ingest tasks.
use super::config::PreparedConfig;
use crate::http::BroadcastState;
use std::{io, net::SocketAddr};
#[cfg(feature = "srt")]
use {
    super::owner::NativeOwner,
    crate::manager::{CleanupTracker, WorkerControl},
    std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    tokio::sync::watch,
};

#[derive(Clone)]
pub(crate) struct StopSignal {
    #[cfg(feature = "srt")]
    native: Arc<AtomicBool>,
    #[cfg(feature = "srt")]
    driver: watch::Sender<bool>,
}
impl StopSignal {
    pub(crate) fn cancel(&self) {
        #[cfg(feature = "srt")]
        {
            self.native.store(true, Ordering::Release);
            self.driver.send_replace(true);
        }
    }
}
#[cfg(feature = "srt")]
#[derive(Clone, Copy)]
enum Outcome {
    Confirmed,
    Failed,
}

pub(crate) struct SrtService {
    stop: StopSignal,
    #[cfg(feature = "srt")]
    address: SocketAddr,
    #[cfg(feature = "srt")]
    control: Option<WorkerControl>,
    #[cfg(feature = "srt")]
    completed: watch::Receiver<bool>,
    #[cfg(feature = "srt")]
    outcome: watch::Receiver<Option<Outcome>>,
    #[cfg(feature = "srt")]
    _cleanup: CleanupTracker,
}
impl SrtService {
    pub(crate) async fn start(
        config: Option<PreparedConfig>,
        state: BroadcastState,
    ) -> io::Result<Option<Self>> {
        let Some(config) = config else {
            return Ok(None);
        };
        #[cfg(not(feature = "srt"))]
        {
            let _ = (config, state);
            Err(io::Error::other("srt_feature_unavailable"))
        }
        #[cfg(feature = "srt")]
        {
            let mut native = NativeOwner::start(config)
                .await
                .map_err(|_| io::Error::other("srt_native_startup_failed"))?;
            let address = native.local_addr();
            let native_stop = native.stop_signal();
            let cleanup = CleanupTracker::default();
            let (published, outcome) = watch::channel(None);
            // Startup publishes no unowned async gap: NativeOwner already has its
            // cancellation-safe join, and the driver is registered before returning.
            let mut control = cleanup.spawn(move |mut cancel| async move {
                loop {
                    let incoming = tokio::select! { biased;
                        _ = cancel.changed() => break,
                        incoming = native.incoming.recv() => match incoming { Some(incoming) => incoming, None => break },
                    };
                    let attached = tokio::select! { biased;
                        _ = cancel.changed() => break,
                        result = super::ingest::attach_connection(incoming, state.clone()) => result,
                    };
                    if let Err(error) = attached { crate::alog!("[srt] producer_rejected: {}", error.code()); }
                }
                let result = match native.shutdown().await {
                    Ok(()) => Outcome::Confirmed,
                    Err(_) => { crate::alog!("[srt] listener_cleanup_failed"); Outcome::Failed },
                };
                published.send_replace(Some(result));
            });
            let completed = control.completion_receiver();
            let stop = StopSignal {
                native: native_stop,
                driver: control.cancellation_signal(),
            };
            control.transfer();
            Ok(Some(Self {
                stop,
                address,
                control: Some(control),
                completed,
                outcome,
                _cleanup: cleanup,
            }))
        }
    }
    pub(crate) fn completion_receiver(&self) -> Option<tokio::sync::watch::Receiver<bool>> {
        #[cfg(feature = "srt")]
        {
            Some(self.completed.clone())
        }
        #[cfg(not(feature = "srt"))]
        {
            None
        }
    }
    pub(crate) fn stop_signal(&self) -> StopSignal {
        self.stop.clone()
    }
    pub(crate) fn local_addr(&self) -> Option<SocketAddr> {
        #[cfg(feature = "srt")]
        {
            Some(self.address)
        }
        #[cfg(not(feature = "srt"))]
        {
            None
        }
    }
    pub(crate) async fn shutdown(&mut self) -> io::Result<()> {
        self.stop.cancel();
        #[cfg(feature = "srt")]
        {
            drop(self.control.take());
            if !crate::manager::wait_cleanup(&mut self.completed).await
                || !matches!(*self.outcome.borrow(), Some(Outcome::Confirmed))
            {
                return Err(io::Error::other("srt_cleanup_unconfirmed"));
            }
        }
        Ok(())
    }
}
impl Drop for SrtService {
    fn drop(&mut self) {
        self.stop.cancel();
    }
}
