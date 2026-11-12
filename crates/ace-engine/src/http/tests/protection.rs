use super::*;
use std::sync::atomic::AtomicUsize;

struct RefusingProvider {
    opens: Arc<AtomicUsize>,
    vod_resolutions: Arc<AtomicUsize>,
}

#[async_trait]
impl StreamProvider for RefusingProvider {
    fn network(&self) -> &'static str {
        "fixture"
    }

    fn check_openable(&self, id: &str) -> Result<(), ProviderError> {
        Err(if id == "protected" {
            ProviderError::ProtectedContent
        } else {
            ProviderError::InvalidProtection
        })
    }

    async fn open(&self, _id: &str) -> Result<Box<dyn TsSource>, ProviderError> {
        self.opens.fetch_add(1, Ordering::SeqCst);
        Err(ProviderError::NotFound)
    }

    async fn resolve_vod(&self, _id: &str) -> Result<Box<dyn VodContent>, ProviderError> {
        self.vod_resolutions.fetch_add(1, Ordering::SeqCst);
        Err(ProviderError::NotFound)
    }
}

#[tokio::test]
async fn native_protection_refusals_preserve_policy_text_before_source_work() {
    let opens = Arc::new(AtomicUsize::new(0));
    let vod_resolutions = Arc::new(AtomicUsize::new(0));
    let mut registry = ProviderRegistry::new();
    registry.register(Arc::new(RefusingProvider {
        opens: opens.clone(),
        vod_resolutions: vod_resolutions.clone(),
    }));
    let app = router(AppState {
        peer_policy: Default::default(),
        manager: StreamManager::new(registry),
        networks: vec!["fixture".into()],
        resolve_content_ids_in_getstream: false,
        ace_sessions: Arc::new(AceSessionStore::default()),
        experimental_ace_compat: false,
        origin_hls: None,
        broadcasts: None,
    });
    let mut observed = Vec::new();
    let mut expected = Vec::new();
    for (id, error) in [
        ("protected", ProviderError::ProtectedContent),
        ("invalid", ProviderError::InvalidProtection),
    ] {
        for path in [
            format!("/streams/fixture/{id}.ts"),
            format!("/streams/fixture/{id}.m3u8"),
            format!("/vod/fixture/{id}"),
            format!("/vod/fixture/{id}/manifest.m3u8"),
            format!("/vod/fixture/{id}/seg/0.ts"),
        ] {
            let response = app
                .clone()
                .oneshot(Request::get(&path).body(Body::empty()).unwrap())
                .await
                .unwrap();
            let status = response.status();
            let body = axum::body::to_bytes(response.into_body(), 1024)
                .await
                .unwrap();
            observed.push((
                path.clone(),
                status,
                String::from_utf8(body.to_vec()).unwrap(),
            ));
            expected.push((path, StatusCode::UNPROCESSABLE_ENTITY, error.to_string()));
        }
    }
    assert_eq!(opens.load(Ordering::SeqCst), 0);
    assert_eq!(vod_resolutions.load(Ordering::SeqCst), 0);
    assert_eq!(observed, expected);
}

#[test]
fn descriptor_analysis_reports_flags_without_playback_authority() {
    use ace_wire::bencode::Bencode;
    for premium in 0..=2 {
        let (live, _) = crate::ace_provider::test_support::synthetic_live_transport(524_288);
        let mut root = ace_wire::transport::decode_transport(&live).unwrap().raw;
        let Bencode::Dict(ref mut fields) = root else {
            panic!("dictionary")
        };
        fields.insert(b"premium".to_vec(), Bencode::Int(premium));
        let live = ace_wire::transport::encode_transport(&root);
        for (bytes, is_live) in [
            (live, 1),
            (
                crate::ace_provider::test_support::protection_vod_bytes(premium, false),
                0,
            ),
            (
                crate::ace_provider::test_support::protection_vod_bytes(premium, true),
                0,
            ),
        ] {
            // This actual-route helper accepts only bytes and cannot access a manager,
            // provider, authenticated index or lease store.
            let result = super::super::compat::inspect_server_api_descriptor(&bytes, None);
            if premium == 2 {
                assert_eq!(result.unwrap_err(), "invalid content protection metadata");
            } else {
                let result = crate::server_api::analyze_result(&result.unwrap());
                assert_eq!(result["is_encrypted"], premium);
                assert_eq!(result["is_live"], is_live);
            }
        }
    }
}

struct WarmProvider {
    refuse: Arc<std::sync::atomic::AtomicBool>,
    opens: Arc<AtomicUsize>,
    resolves: Arc<AtomicUsize>,
}
struct WarmVod;
#[async_trait]
impl VodContent for WarmVod {
    fn content_length(&self) -> u64 {
        1
    }
    async fn open_range(
        &self,
        _: u64,
        _: u64,
    ) -> Result<Box<dyn crate::provider::VodByteSource>, ProviderError> {
        panic!("admission must not read ranges")
    }
}
#[async_trait]
impl StreamProvider for WarmProvider {
    fn network(&self) -> &'static str {
        "warm"
    }
    fn check_openable(&self, _: &str) -> Result<(), ProviderError> {
        if self.refuse.load(Ordering::SeqCst) {
            Err(ProviderError::ProtectedContent)
        } else {
            Ok(())
        }
    }
    async fn open(&self, _: &str) -> Result<Box<dyn TsSource>, ProviderError> {
        self.opens.fetch_add(1, Ordering::SeqCst);
        Ok(Box::new(StalledSource))
    }
    async fn resolve_vod(&self, _: &str) -> Result<Box<dyn VodContent>, ProviderError> {
        self.resolves.fetch_add(1, Ordering::SeqCst);
        Ok(Box::new(WarmVod))
    }
}
#[tokio::test]
async fn warm_protection_gate_precedes_live_hls_and_vod_reuse() {
    let refuse = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let opens = Arc::new(AtomicUsize::new(0));
    let resolves = Arc::new(AtomicUsize::new(0));
    let mut registry = ProviderRegistry::new();
    registry.register(Arc::new(WarmProvider {
        refuse: refuse.clone(),
        opens: opens.clone(),
        resolves: resolves.clone(),
    }));
    let manager = StreamManager::new(registry);
    let live = manager.get_or_start("warm", "same").await.unwrap();
    let hls = manager.get_or_start_hls("warm", "same").await.unwrap();
    let vod = manager.resolve_vod("warm", "same").await.unwrap();
    assert!(Arc::ptr_eq(
        &live,
        &manager.get_or_start("warm", "same").await.unwrap()
    ));
    assert!(Arc::ptr_eq(
        &hls,
        &manager.get_or_start_hls("warm", "same").await.unwrap()
    ));
    assert!(Arc::ptr_eq(
        &vod,
        &manager.resolve_vod("warm", "same").await.unwrap()
    ));
    refuse.store(true, Ordering::SeqCst);
    let errors = [
        manager
            .get_or_start("warm", "same")
            .await
            .err()
            .unwrap()
            .to_string(),
        manager
            .get_or_start_hls("warm", "same")
            .await
            .err()
            .unwrap()
            .to_string(),
        manager
            .resolve_vod("warm", "same")
            .await
            .err()
            .unwrap()
            .to_string(),
    ];
    assert_eq!(errors, ["encrypted content is not supported"; 3]);
    assert_eq!(opens.load(Ordering::SeqCst), 1);
    assert_eq!(resolves.load(Ordering::SeqCst), 1);
    manager.shutdown().await;
}

#[tokio::test]
async fn url_protection_preparation_refuses_raw_and_disk_without_leases_or_opens() {
    for disk in [false, true] {
        for premium in [1, 2] {
            let opens = Arc::new(AtomicUsize::new(0));
            let resolves = Arc::new(AtomicUsize::new(0));
            let mut registry = ProviderRegistry::new();
            registry.register(Arc::new(WarmProvider {
                refuse: Arc::new(std::sync::atomic::AtomicBool::new(false)),
                opens: opens.clone(),
                resolves: resolves.clone(),
            }));
            let leases = Arc::new(AceSessionStore::default());
            let manager = StreamManager::new(registry);
            let state = AppState {
                peer_policy: Default::default(),
                manager: manager.clone(),
                networks: vec!["warm".into()],
                resolve_content_ids_in_getstream: true,
                ace_sessions: leases.clone(),
                experimental_ace_compat: true,
                origin_hls: None,
                broadcasts: None,
            };
            let response = URL_DESCRIPTOR_BYTES.scope(
                crate::ace_provider::test_support::protection_vod_bytes(premium, disk),
                router(state).oneshot(Request::get("/ace/getstream?format=json&url=https%3A%2F%2Fexample.invalid%2Fsynthetic").body(Body::empty()).unwrap()),
            ).await.unwrap();
            let body = axum::body::to_bytes(response.into_body(), 4096)
                .await
                .unwrap();
            let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(body["response"], serde_json::Value::Null);
            assert_eq!(
                body["error"],
                if premium == 1 {
                    "encrypted content is not supported"
                } else {
                    "invalid content protection metadata"
                }
            );
            assert_eq!(leases.active_count(), 0);
            assert!(manager.list().await.is_empty());
            assert_eq!(opens.load(Ordering::SeqCst), 0);
            assert_eq!(resolves.load(Ordering::SeqCst), 0);
        }
    }
}
