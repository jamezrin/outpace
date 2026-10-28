//! Provider ownership accounting. Field order deliberately destroys resources before permits.
use crate::provider::*;
use async_trait::async_trait;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

pub(crate) struct Admission {
    occupied: AtomicUsize,
    max: Option<usize>,
}
impl Admission {
    pub(crate) fn new(max: usize) -> Arc<Self> {
        Arc::new(Self {
            occupied: AtomicUsize::new(0),
            max: (max != 0).then_some(max),
        })
    }
    pub(crate) fn snapshot(&self) -> (usize, Option<usize>) {
        (self.occupied.load(Ordering::Acquire), self.max)
    }
    pub(crate) fn reserve(self: &Arc<Self>) -> Result<Reservation, ProviderError> {
        self.occupied
            .try_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                if self.max.is_some_and(|max| n >= max) {
                    None
                } else {
                    n.checked_add(1)
                }
            })
            .map_err(|occupied| {
                crate::alog!(
                    "[manager] session capacity reached: occupied={} max={}",
                    occupied,
                    self.max.unwrap_or(usize::MAX)
                );
                ProviderError::CapacityExhausted
            })?;
        Ok(Reservation(self.clone()))
    }
}
pub(crate) struct Reservation(Arc<Admission>);
impl Drop for Reservation {
    fn drop(&mut self) {
        self.0.occupied.fetch_sub(1, Ordering::AcqRel);
    }
}
pub(crate) struct Source {
    pub(crate) inner: Box<dyn TsSource>,
    pub(crate) _reservation: Reservation,
}
#[async_trait]
impl TsSource for Source {
    async fn next(&mut self) -> Option<bytes::Bytes> {
        self.inner.next().await
    }
    fn take_discontinuity(&mut self) -> bool {
        self.inner.take_discontinuity()
    }
    fn stats(&self) -> SourceStats {
        self.inner.stats()
    }
    fn stats_handle(&self) -> Option<StatsHandle> {
        self.inner.stats_handle()
    }
    fn metadata(&self) -> ace_swarm::types::StreamMetadata {
        self.inner.metadata()
    }
}
pub(crate) struct ContentOwner {
    pub(crate) inner: Box<dyn VodContent>,
    pub(crate) _reservation: Reservation,
}
pub(crate) struct Content(pub(crate) Arc<ContentOwner>);
struct Range {
    inner: Box<dyn VodByteSource>,
    _owner: Arc<ContentOwner>,
}
#[async_trait]
impl VodContent for Content {
    fn content_length(&self) -> u64 {
        self.0.inner.content_length()
    }
    async fn open_range(
        &self,
        start: u64,
        end: u64,
    ) -> Result<Box<dyn VodByteSource>, ProviderError> {
        let inner = self.0.inner.open_range(start, end).await?;
        Ok(Box::new(Range {
            inner,
            _owner: self.0.clone(),
        }))
    }
}
#[async_trait]
impl VodByteSource for Range {
    fn content_length(&self) -> u64 {
        self.inner.content_length()
    }
    async fn next(&mut self) -> Option<bytes::Bytes> {
        self.inner.next().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    struct Inner(Arc<Admission>);
    impl Drop for Inner {
        fn drop(&mut self) {
            assert_eq!(self.0.snapshot().0, 1, "inner destroyed before permit");
        }
    }
    impl StatsReader for Inner {
        fn snapshot(&self) -> SourceStats {
            SourceStats {
                peers: 7,
                ..Default::default()
            }
        }
    }
    #[async_trait]
    impl TsSource for Inner {
        async fn next(&mut self) -> Option<bytes::Bytes> {
            Some(bytes::Bytes::from_static(b"x"))
        }
        fn take_discontinuity(&mut self) -> bool {
            true
        }
        fn stats(&self) -> SourceStats {
            self.snapshot()
        }
        fn stats_handle(&self) -> Option<StatsHandle> {
            Some(Arc::new(Inner(self.0.clone())))
        }
        fn metadata(&self) -> ace_swarm::types::StreamMetadata {
            ace_swarm::types::StreamMetadata {
                bitrate: Some(7),
                ..Default::default()
            }
        }
    }
    #[async_trait]
    impl VodContent for Inner {
        fn content_length(&self) -> u64 {
            11
        }
        async fn open_range(
            &self,
            _: u64,
            _: u64,
        ) -> Result<Box<dyn VodByteSource>, ProviderError> {
            Ok(Box::new(Inner(self.0.clone())))
        }
    }
    #[async_trait]
    impl VodByteSource for Inner {
        fn content_length(&self) -> u64 {
            2
        }
        async fn next(&mut self) -> Option<bytes::Bytes> {
            Some(bytes::Bytes::from_static(b"ab"))
        }
    }

    #[tokio::test]
    async fn admission_decorators_forward_and_destroy_inner_before_release() {
        let admission = Admission::new(1);
        let mut source = Source {
            inner: Box::new(Inner(admission.clone())),
            _reservation: admission.reserve().unwrap(),
        };
        assert_eq!(
            source.next().await.unwrap(),
            bytes::Bytes::from_static(b"x")
        );
        assert!(source.take_discontinuity());
        assert_eq!(source.stats().peers, 7);
        assert_eq!(source.stats_handle().unwrap().snapshot().peers, 7);
        assert_eq!(source.metadata().bitrate, Some(7));
        drop(source);
        assert_eq!(admission.snapshot().0, 0);
        let content = Content(Arc::new(ContentOwner {
            inner: Box::new(Inner(admission.clone())),
            _reservation: admission.reserve().unwrap(),
        }));
        assert_eq!(content.content_length(), 11);
        let mut range = content.open_range(0, 1).await.unwrap();
        drop(content);
        assert_eq!(range.content_length(), 2);
        assert_eq!(
            range.next().await.unwrap(),
            bytes::Bytes::from_static(b"ab")
        );
        drop(range);
        assert_eq!(admission.snapshot().0, 0);
    }
}

#[cfg(test)]
mod cancellation_tests {
    use super::*;
    struct Pending(tokio::sync::Notify);
    #[async_trait]
    impl VodContent for Pending {
        fn content_length(&self) -> u64 {
            1
        }
        async fn open_range(
            &self,
            _: u64,
            _: u64,
        ) -> Result<Box<dyn VodByteSource>, ProviderError> {
            self.0.notify_one();
            std::future::pending().await
        }
    }
    #[tokio::test]
    async fn admission_cancelled_range_retains_then_releases_content_owner() {
        let admission = Admission::new(1);
        let content = Arc::new(Content(Arc::new(ContentOwner {
            inner: Box::new(Pending(tokio::sync::Notify::new())),
            _reservation: admission.reserve().unwrap(),
        })));
        let task = tokio::spawn({
            let content = content.clone();
            async move { content.open_range(0, 0).await }
        });
        // Poll until the task holds the only additional shared content handle.
        tokio::task::yield_now().await;
        assert_eq!(admission.snapshot().0, 1);
        drop(content);
        task.abort();
        let _ = task.await;
        assert_eq!(admission.snapshot().0, 0);
    }
}
