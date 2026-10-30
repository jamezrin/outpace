use super::*;
use crate::store::{CompleteAdmission, CompleteReceipt};
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::sync::{oneshot, Mutex, OwnedMutexGuard};
type Admission = Pin<Box<dyn Future<Output = std::io::Result<CompleteAdmission>> + Send>>;
type Refresh = Pin<Box<dyn Future<Output = OwnedMutexGuard<PieceStore>> + Send>>;
pub(in crate::live_session::pool) enum PublicationOperation {
    Acquiring {
        piece: u64,
        future: Admission,
    },
    Awaiting {
        piece: u64,
        done: oneshot::Receiver<std::io::Result<crate::store::CompletePut>>,
    },
    Refreshing {
        piece: u64,
        result: std::io::Result<crate::store::CompletePut>,
        future: Refresh,
    },
}
fn terminal(c: &mut Continuity, piece: u64, result: std::io::Result<crate::store::CompletePut>) {
    if let Err(error) = &result {
        crate::alog!("[cache] whole live piece publication failed: {error}");
    }
    *c.publication.get_mut(&piece).expect("retained publication") =
        PublicationStatus::Terminal(result.map_err(|_| ()));
}
pub(super) fn poll_operation(
    op: &mut Option<PublicationOperation>,
    c: &mut Continuity,
    store: &Arc<Mutex<PieceStore>>,
    cx: &mut Context<'_>,
) -> bool {
    let Some(current) = op.as_mut() else {
        return false;
    };
    match current {
        PublicationOperation::Acquiring { piece, future } => {
            let Poll::Ready(admission) = future.as_mut().poll(cx) else {
                return false;
            };
            let piece = *piece;
            let result = admission.and_then(|admission| {
                let done = c
                    .reasm
                    .completed_live_piece(piece)
                    .expect("held accepted wire");
                admission.submit(done.header, done.wire)
            });
            match result {
                Ok(CompleteReceipt::Awaiting(done)) => {
                    *op = Some(PublicationOperation::Awaiting { piece, done })
                }
                other => {
                    terminal(
                        c,
                        piece,
                        other.map(|receipt| match receipt {
                            CompleteReceipt::Ready(result) => result,
                            _ => unreachable!(),
                        }),
                    );
                    *op = None;
                }
            }
        }
        PublicationOperation::Awaiting { piece, done } => {
            let Poll::Ready(result) = Pin::new(done).poll(cx) else {
                return false;
            };
            *op = Some(PublicationOperation::Refreshing {
                piece: *piece,
                result: result.unwrap_or_else(|_| Err(std::io::ErrorKind::BrokenPipe.into())),
                future: Box::pin(Arc::clone(store).lock_owned()),
            });
        }
        PublicationOperation::Refreshing { piece, future, .. } => {
            let Poll::Ready(mut guard) = future.as_mut().poll(cx) else {
                return false;
            };
            guard.refresh_complete_accounting();
            let piece = *piece;
            let Some(PublicationOperation::Refreshing { result, .. }) = op.take() else {
                unreachable!()
            };
            terminal(c, piece, result);
        }
    }
    true
}
impl PoolActor<'_> {
    pub(super) fn start_publication(&mut self) {
        start_operation(&mut self.publication, self.continuity, self.store);
    }
    pub(super) fn poll_publication(&mut self, cx: &mut Context<'_>) -> bool {
        poll_operation(&mut self.publication, self.continuity, self.store, cx)
    }
}

pub(super) fn start_operation(
    op: &mut Option<PublicationOperation>,
    c: &mut Continuity,
    store: &Arc<Mutex<PieceStore>>,
) {
    if op.is_some() {
        return;
    }
    if let Some((&piece, _)) = c
        .publication
        .iter()
        .find(|(_, terminal)| matches!(terminal, PublicationStatus::Needed))
    {
        let wire_len = c
            .reasm
            .completed_live_piece(piece)
            .expect("held wire")
            .wire
            .len();
        *op = Some(PublicationOperation::Acquiring {
            piece,
            future: Box::pin(PieceStore::begin_complete_admission(
                Arc::clone(store),
                piece,
                wire_len,
            )),
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn continuity() -> Continuity {
        let info = StreamInfo {
            infohash: [0; 20],
            piece_length: 4,
            chunk_length: 2,
            trackers: vec![],
            metadata: Default::default(),
            sig_len: 0,
            source_pubkey: vec![],
        };
        let (mut c, _) = Continuity::fresh(&info, 0, 0, 1, LiveRecoveryConfig::default());
        c.reasm.add_live_chunk(0, 0, 2, [1; 8], b"ab").unwrap();
        c.reasm.add_live_chunk(0, 1, 2, [1; 8], b"cd").unwrap();
        c.publication.insert(0, PublicationStatus::Needed);
        c
    }
    fn acquire(store: &Arc<Mutex<PieceStore>>) -> Option<PublicationOperation> {
        Some(PublicationOperation::Acquiring {
            piece: 0,
            future: Box::pin(PieceStore::begin_complete_admission(store.clone(), 0, 4)),
        })
    }
    async fn step(
        op: &mut Option<PublicationOperation>,
        c: &mut Continuity,
        store: &Arc<Mutex<PieceStore>>,
    ) {
        std::future::poll_fn(|cx| {
            if poll_operation(op, c, store, cx) {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
        .await;
    }
    async fn pending(
        op: &mut Option<PublicationOperation>,
        c: &mut Continuity,
        store: &Arc<Mutex<PieceStore>>,
    ) -> bool {
        std::future::poll_fn(|cx| Poll::Ready(!poll_operation(op, c, store, cx))).await
    }
    #[tokio::test]
    async fn memory_mutex_cancellation_retains_needed_and_original_playback() {
        let store = Arc::new(Mutex::new(PieceStore::new(4, 2, 8)));
        let mut c = continuity();
        let held = store.lock().await;
        let mut op = acquire(&store);
        assert!(pending(&mut op, &mut c, &store).await);
        drop(op);
        assert!(c.publication_hold());
        assert_eq!(c.reasm.completed_live_piece(0).unwrap().wire, b"abcd");
        drop(held);
        let mut replacement = acquire(&store);
        step(&mut replacement, &mut c, &store).await;
        assert!(!c.publication_hold());
        assert_eq!(c.reasm.take_ready(), b"abcd");
    }
    #[tokio::test]
    async fn disk_accounting_refresh_wait_remains_pollable_and_cancel_safe() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(Mutex::new(
            PieceStore::new_disk(4, 2, 8, dir.path().join("cache")).unwrap(),
        ));
        let mut c = continuity();
        let mut op = acquire(&store);
        step(&mut op, &mut c, &store).await;
        assert!(matches!(op, Some(PublicationOperation::Awaiting { .. })));
        let held = store.lock().await;
        step(&mut op, &mut c, &store).await;
        assert!(matches!(op, Some(PublicationOperation::Refreshing { .. })));
        assert!(pending(&mut op, &mut c, &store).await);
        assert!(c.publication_hold());
        assert_eq!(held.publication_budget(), (0, 1));
        drop(op);
        drop(held);
        // Lost acknowledgement is retried through immutable AlreadyPresent.
        let mut replacement = acquire(&store);
        for _ in 0..3 {
            step(&mut replacement, &mut c, &store).await;
        }
        assert!(matches!(
            c.publication[&0],
            PublicationStatus::Terminal(Ok(crate::store::CompletePut::AlreadyPresent))
        ));
        assert_eq!(c.reasm.take_ready(), b"abcd");
    }
    #[tokio::test]
    async fn terminal_nonretention_and_io_error_preserve_playback_and_later_progress() {
        for budget in [0, 3] {
            let store = Arc::new(Mutex::new(PieceStore::new(4, 2, budget)));
            let mut c = continuity();
            let mut op = acquire(&store);
            step(&mut op, &mut c, &store).await;
            assert!(matches!(
                c.publication[&0],
                PublicationStatus::Terminal(Ok(crate::store::CompletePut::NotRetained))
            ));
            assert!(!store.lock().await.has_piece(0));
            assert_eq!(c.reasm.take_ready(), b"abcd");
        }
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path().join("cache");
        let store = Arc::new(Mutex::new(
            PieceStore::new_disk(4, 2, 8, cache.clone()).unwrap(),
        ));
        std::fs::remove_dir(&cache).unwrap();
        std::fs::write(&cache, b"force non-directory write failure").unwrap();
        let mut c = continuity();
        let mut op = acquire(&store);
        for _ in 0..3 {
            step(&mut op, &mut c, &store).await;
        }
        assert!(matches!(
            c.publication[&0],
            PublicationStatus::Terminal(Err(()))
        ));
        assert!(!c.publication_hold());
        assert!(!store.lock().await.has_piece(0));
        assert_eq!(store.lock().await.publication_budget(), (0, 1));
        assert_eq!(c.reasm.take_ready(), b"abcd");
        std::fs::remove_file(&cache).unwrap();
        std::fs::create_dir(&cache).unwrap();
        assert_eq!(
            PieceStore::shared_put_complete_piece(&store, 1, [1; 8], b"efgh")
                .await
                .unwrap(),
            crate::store::CompletePut::Published
        );
    }
}
