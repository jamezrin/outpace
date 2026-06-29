//! A network-free `StreamProvider` for exercising the engine in tests: serves a fixed
//! number of TS-looking chunks for any id, proving the provider seam without any I/O.

use crate::provider::{ProviderError, SourceStats, StreamProvider, TsSource};
use async_trait::async_trait;
use bytes::Bytes;

pub struct TestProvider {
    pub chunks: usize,
}

struct TestSource {
    remaining: usize,
    idx: usize,
}

#[async_trait]
impl TsSource for TestSource {
    async fn next(&mut self) -> Option<Bytes> {
        if self.remaining == 0 {
            return None;
        }
        self.remaining -= 1;
        let mut b = vec![0u8; 188];
        b[0] = 0x47;
        b[1] = self.idx as u8;
        self.idx += 1;
        Some(Bytes::from(b))
    }
    fn stats(&self) -> SourceStats {
        SourceStats { peers: 1, bitrate: 0, buffer_ms: 0 }
    }
}

#[async_trait]
impl StreamProvider for TestProvider {
    fn network(&self) -> &'static str {
        "test"
    }
    async fn open(&self, _id: &str) -> Result<Box<dyn TsSource>, ProviderError> {
        Ok(Box::new(TestSource { remaining: self.chunks, idx: 0 }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_source_yields_n_ts_packets_then_ends() {
        let p = TestProvider { chunks: 3 };
        let mut s = p.open("anything").await.unwrap();
        let mut n = 0;
        while let Some(b) = s.next().await {
            assert_eq!(b[0], 0x47);
            n += 1;
        }
        assert_eq!(n, 3);
    }
}
