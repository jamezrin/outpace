use super::*;

struct FakeVod {
    chunks: std::collections::VecDeque<Bytes>,
    total: u64,
}

#[async_trait]
impl VodByteSource for FakeVod {
    fn content_length(&self) -> u64 {
        self.total
    }
    async fn next(&mut self) -> Option<Bytes> {
        self.chunks.pop_front()
    }
}

#[tokio::test]
async fn vod_response_streams_body_with_content_length() {
    let src = FakeVod {
        chunks: [Bytes::from_static(b"abc"), Bytes::from_static(b"de")]
            .into_iter()
            .collect(),
        total: 5,
    };
    let resp = vod_response_from_source(Box::new(src));
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(resp.headers()[header::CONTENT_LENGTH], "5");
    assert_eq!(resp.headers()[header::ACCEPT_RANGES], "bytes");
    let body = axum::body::to_bytes(resp.into_body(), 64).await.unwrap();
    assert_eq!(&body[..], b"abcde");
}

#[test]
fn parse_byte_range_covers_the_range_header_forms() {
    use RangeOutcome::*;
    // No / non-bytes / unparseable / multi-range specs are ignored -> serve the whole file.
    assert!(matches!(parse_byte_range(None, 10), Full));
    assert!(matches!(parse_byte_range(Some("items=0-1"), 10), Full));
    assert!(matches!(parse_byte_range(Some("bytes=abc"), 10), Full));
    assert!(matches!(parse_byte_range(Some("bytes=-"), 10), Full));
    assert!(matches!(parse_byte_range(Some("bytes=0-1,3-4"), 10), Full));
    // Explicit range, clamped to the last byte.
    assert!(matches!(
        parse_byte_range(Some("bytes=2-4"), 10),
        Satisfiable(2, 4)
    ));
    assert!(matches!(
        parse_byte_range(Some("bytes=8-100"), 10),
        Satisfiable(8, 9)
    ));
    // Open-ended: from N to the end.
    assert!(matches!(
        parse_byte_range(Some("bytes=5-"), 10),
        Satisfiable(5, 9)
    ));
    // Suffix: the last N bytes (and a suffix longer than the file clamps to the whole file).
    assert!(matches!(
        parse_byte_range(Some("bytes=-3"), 10),
        Satisfiable(7, 9)
    ));
    assert!(matches!(
        parse_byte_range(Some("bytes=-100"), 10),
        Satisfiable(0, 9)
    ));
    // Out-of-bounds / inverted / zero-suffix are unsatisfiable -> 416.
    assert!(matches!(
        parse_byte_range(Some("bytes=100-200"), 10),
        Unsatisfiable
    ));
    assert!(matches!(
        parse_byte_range(Some("bytes=5-3"), 10),
        Unsatisfiable
    ));
    assert!(matches!(
        parse_byte_range(Some("bytes=-0"), 10),
        Unsatisfiable
    ));
}

// An in-memory VOD provider for exercising the HTTP `/vod` range contract without a swarm.
// (Real SHA-1 verification of served ranges is covered by the ace-swarm vod tests.)
struct MemVodProvider {
    data: Vec<u8>,
}
struct MemVod {
    data: Vec<u8>,
}
struct MemVodSource {
    chunk: Option<Bytes>,
    len: u64,
}
#[async_trait]
impl VodByteSource for MemVodSource {
    fn content_length(&self) -> u64 {
        self.len
    }
    async fn next(&mut self) -> Option<Bytes> {
        self.chunk.take()
    }
}
#[async_trait]
impl VodContent for MemVod {
    fn content_length(&self) -> u64 {
        self.data.len() as u64
    }
    async fn open_range(
        &self,
        start: u64,
        end: u64,
    ) -> Result<Box<dyn VodByteSource>, ProviderError> {
        let slice = self.data[start as usize..=end as usize].to_vec();
        let len = slice.len() as u64;
        Ok(Box::new(MemVodSource {
            chunk: Some(Bytes::from(slice)),
            len,
        }))
    }
}
#[async_trait]
impl StreamProvider for MemVodProvider {
    fn network(&self) -> &'static str {
        "memvod"
    }
    async fn open(&self, _id: &str) -> Result<Box<dyn TsSource>, ProviderError> {
        Err(ProviderError::NotFound)
    }
    async fn resolve_vod(&self, _id: &str) -> Result<Box<dyn VodContent>, ProviderError> {
        Ok(Box::new(MemVod {
            data: self.data.clone(),
        }))
    }
}

fn memvod_router(data: Vec<u8>) -> Router {
    let mut r = ProviderRegistry::new();
    r.register(std::sync::Arc::new(MemVodProvider { data }));
    router(AppState {
        manager: StreamManager::new(r),
        networks: vec!["memvod".into()],
        resolve_content_ids_in_getstream: false,
        ace_sessions: Arc::new(AceSessionStore::default()),
        experimental_ace_compat: false,
        broadcasts: None,
    })
}

#[tokio::test]
async fn vod_without_range_serves_200_and_advertises_accept_ranges() {
    let data: Vec<u8> = (0..20u8).collect();
    let resp = memvod_router(data.clone())
        .oneshot(Request::get("/vod/memvod/x").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(resp.headers()[header::ACCEPT_RANGES], "bytes");
    assert_eq!(resp.headers()[header::CONTENT_LENGTH], "20");
    let body = axum::body::to_bytes(resp.into_body(), 1 << 20)
        .await
        .unwrap();
    assert_eq!(&body[..], &data[..]);
}

#[tokio::test]
async fn vod_range_serves_206_partial_content() {
    let data: Vec<u8> = (0..20u8).collect();
    let resp = memvod_router(data.clone())
        .oneshot(
            Request::get("/vod/memvod/x")
                .header(header::RANGE, "bytes=5-9")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(resp.headers()[header::CONTENT_RANGE], "bytes 5-9/20");
    assert_eq!(resp.headers()[header::CONTENT_LENGTH], "5");
    assert_eq!(resp.headers()[header::ACCEPT_RANGES], "bytes");
    let body = axum::body::to_bytes(resp.into_body(), 1 << 20)
        .await
        .unwrap();
    assert_eq!(&body[..], &data[5..=9]);
}

#[tokio::test]
async fn vod_suffix_range_serves_the_last_bytes() {
    let data: Vec<u8> = (0..20u8).collect();
    let resp = memvod_router(data.clone())
        .oneshot(
            Request::get("/vod/memvod/x")
                .header(header::RANGE, "bytes=-4")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(resp.headers()[header::CONTENT_RANGE], "bytes 16-19/20");
    let body = axum::body::to_bytes(resp.into_body(), 1 << 20)
        .await
        .unwrap();
    assert_eq!(&body[..], &data[16..=19]);
}

#[tokio::test]
async fn vod_unsatisfiable_range_returns_416_with_content_range() {
    let data: Vec<u8> = (0..20u8).collect();
    let resp = memvod_router(data)
        .oneshot(
            Request::get("/vod/memvod/x")
                .header(header::RANGE, "bytes=50-60")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::RANGE_NOT_SATISFIABLE);
    assert_eq!(resp.headers()[header::CONTENT_RANGE], "bytes */20");
}

// A memvod router whose HLS segments are `seg_packets` TS packets (188 bytes each), so small
// fixtures still span several segments.
fn memvod_hls_router(data: Vec<u8>, seg_packets: usize) -> Router {
    let mut r = ProviderRegistry::new();
    r.register(std::sync::Arc::new(MemVodProvider { data }));
    let hls = crate::config::HlsConfig {
        segment_packets: seg_packets,
        window_segments: 6,
        segment_duration_ms: 2000,
        ..crate::config::HlsConfig::default()
    };
    router(AppState {
        manager: StreamManager::with_config(r, 256, hls),
        networks: vec!["memvod".into()],
        resolve_content_ids_in_getstream: false,
        ace_sessions: Arc::new(AceSessionStore::default()),
        experimental_ace_compat: false,
        broadcasts: None,
    })
}

#[tokio::test]
async fn vod_manifest_serves_a_terminated_vod_playlist() {
    // 1500 bytes over 564-byte (3-packet) segments => 3 segments.
    let resp = memvod_hls_router(vec![0u8; 1500], 3)
        .oneshot(
            Request::get("/vod/memvod/x/manifest.m3u8")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        resp.headers()[header::CONTENT_TYPE],
        "application/vnd.apple.mpegurl"
    );
    let body = axum::body::to_bytes(resp.into_body(), 1 << 20)
        .await
        .unwrap();
    let text = std::str::from_utf8(&body).unwrap();
    assert!(text.contains("#EXT-X-PLAYLIST-TYPE:VOD"));
    assert!(text.contains("/vod/memvod/x/seg/0.ts"));
    assert!(text.contains("/vod/memvod/x/seg/2.ts"));
    assert!(!text.contains("/vod/memvod/x/seg/3.ts"));
    assert!(text.trim_end().ends_with("#EXT-X-ENDLIST"));
}

#[tokio::test]
async fn vod_hls_segment_serves_its_verified_byte_range_as_ts() {
    let data: Vec<u8> = (0..255u8).cycle().take(1500).collect();
    // Segment 1 covers bytes [564, 1128).
    let resp = memvod_hls_router(data.clone(), 3)
        .oneshot(
            Request::get("/vod/memvod/x/seg/1.ts")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(resp.headers()[header::CONTENT_TYPE], "video/mp2t");
    assert_eq!(resp.headers()[header::CONTENT_LENGTH], "564");
    let body = axum::body::to_bytes(resp.into_body(), 1 << 20)
        .await
        .unwrap();
    assert_eq!(&body[..], &data[564..1128]);
}

#[tokio::test]
async fn vod_hls_final_segment_is_clamped_to_the_last_byte() {
    let data: Vec<u8> = (0..255u8).cycle().take(1500).collect();
    // 1500 bytes => final segment 2 covers [1128, 1500): 372 bytes.
    let resp = memvod_hls_router(data.clone(), 3)
        .oneshot(
            Request::get("/vod/memvod/x/seg/2.ts")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(resp.headers()[header::CONTENT_LENGTH], "372");
    let body = axum::body::to_bytes(resp.into_body(), 1 << 20)
        .await
        .unwrap();
    assert_eq!(&body[..], &data[1128..1500]);
}

#[tokio::test]
async fn vod_hls_segment_past_the_end_404s() {
    let resp = memvod_hls_router(vec![0u8; 1500], 3)
        .oneshot(
            Request::get("/vod/memvod/x/seg/99.ts")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}
