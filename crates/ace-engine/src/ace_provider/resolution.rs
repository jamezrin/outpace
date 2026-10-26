use super::*;

impl AceProvider {
    /// Resolve a content-id to a [`StreamInfo`] by fetching its `AceStreamTransport` metadata
    /// from the signed catalog path or this daemon's own broadcast registry. With explicit
    /// opt-in, BEP-9 peers can resolve raw wrapped-transport hashes (cached with a TTL).
    /// Only a signed-catalog result is recorded in the
    /// infohash index; a BEP-9 result goes to `peer_resolve_cache` and never makes its infohash
    /// openable (#164).
    async fn resolve_content_id(&self, content_id: &str) -> Result<StreamInfo, ProviderError> {
        let cid = content_id.to_owned();
        self.resolve_content_id_with_catalog(
            content_id,
            async move { catalog_live_info(&cid).await },
        )
        .await
    }

    pub(super) async fn resolve_content_id_prepared(
        &self,
        cid: &str,
    ) -> Result<ResolvedContentId, ProviderError> {
        let content_id = cid.to_owned();
        self.resolve_content_id_prepared_with_catalog(cid, async move {
            catalog_live_info(&content_id).await
        })
        .await
    }

    pub(super) async fn resolve_content_id_with_catalog(
        &self,
        content_id: &str,
        catalog: impl std::future::Future<Output = Result<StreamInfo, ResolveError>> + Send + 'static,
    ) -> Result<StreamInfo, ProviderError> {
        self.resolve_content_id_prepared_with_catalog(content_id, catalog)
            .await
            .map(ResolvedContentId::into_info)
    }

    async fn resolve_content_id_prepared_with_catalog(
        &self,
        content_id: &str,
        catalog: impl std::future::Future<Output = Result<StreamInfo, ResolveError>> + Send + 'static,
    ) -> Result<ResolvedContentId, ProviderError> {
        if let Some(info) = self.resolve_cache.get(content_id) {
            // Re-record: the bounded infohash index may have evicted it since (#164).
            self.infohash_index.put(info.clone());
            return Ok(ResolvedContentId::Catalog(info));
        }
        let key = hex20(content_id).map_err(|_| ProviderError::Backend("bad content-id".into()))?;
        if let Some(bytes) = self.seed_registry.broadcast_transport_for_content_id(&key) {
            return stream_info_from_transport(&bytes)
                .map(ResolvedContentId::Provider)
                .map_err(|e| ProviderError::Backend(format!("local broadcast descriptor: {e:?}")));
        }
        if self.bep9_content_id_fallback {
            if let Some(info) = self.peer_resolve_cache.get(content_id) {
                return Ok(ResolvedContentId::Provider(info));
            }
        }

        let catalog_error = match self.resolve_cache.resolve_with(content_id, catalog).await {
            Ok(info) => {
                crate::alog!("[ace] resolved content id via catalog");
                self.infohash_index.put(info.clone());
                return Ok(ResolvedContentId::Catalog(info));
            }
            Err(CatalogLookupError::Capacity) => {
                return Err(ProviderError::Backend(
                    "content-id catalog resolution capacity exceeded".into(),
                ));
            }
            Err(CatalogLookupError::Catalog(error)) => error,
        };
        if !self.bep9_content_id_fallback {
            return Err(disabled_content_id_fallback(&catalog_error));
        }
        crate::alog!("[ace] catalog resolution failed; trying opted-in raw-hash BEP-9 fallback");

        let all = if self.bootstrap_peers.is_empty() {
            discover_peers(
                &self.default_trackers,
                &key,
                &random_peer_id(),
                self.discovery_announce_port(),
            )
            .await
        } else {
            self.bootstrap_peers.clone()
        };
        crate::alog!(
            "[ace] raw-hash content-id fallback: {} metadata peer(s)",
            all.len()
        );

        for addr in all {
            let Ok(Ok(session)) =
                tokio::time::timeout(CONNECT_TIMEOUT, connect(&addr.to_string())).await
            else {
                continue; // unreachable peer; don't waste the log on it
            };
            // Bound each peer's reads so a connected-but-silent peer doesn't stall resolution.
            let mut session = session.with_timeout(RESOLVE_PEER_TIMEOUT);
            match resolve_via_peer(&mut session, key, &self.identity).await {
                Ok(info) => {
                    crate::alog!("[ace] resolved raw-hash content id via metadata peer");
                    // A raw transport hash is not catalog authentication: keep the descriptor
                    // out of the infohash index (#164).
                    self.peer_resolve_cache.put(content_id, info.clone());
                    return Ok(ResolvedContentId::Provider(info));
                }
                Err(ResolveError::Peer(why)) => crate::alog!("[ace] resolve {addr}: {why}"),
                Err(e) => crate::alog!("[ace] resolve {addr}: {e:?}"),
            }
        }
        Err(ProviderError::Backend(
            "content-id resolution: no metadata peer responded".into(),
        ))
    }

    /// Resolve a live `id` to a verified [`StreamInfo`]: the single resolver behind every live
    /// entry point (native `/streams`, compat `/ace/getstream` + `/ace/manifest.m3u8`, and
    /// `outpace play`). See #164.
    ///
    /// - `cid:<40hex>`: local broadcast or signed catalog; raw-hash BEP-9 peers require opt-in
    ///   ([`Self::resolve_content_id`]).
    /// - a transport-url id: fetched under the SSRF guard; not recorded in the infohash index.
    /// - a bare 40-hex infohash: own broadcast, verified index, then authenticated catalog
    ///   lookup with strict requested-hash binding. Unavailable metadata fails closed;
    ///   BEP-9 and transport-url descriptors never qualify for this index.
    ///
    /// Every signed-catalog content-id descriptor resolved here is recorded in the infohash
    /// index, so the stream can later be opened by its infohash too (BEP-9 results are not). outpace never guesses live geometry.
    pub(super) async fn resolve_live_info(&self, id: &str) -> Result<StreamInfo, ProviderError> {
        if let Some(content_id) = id.strip_prefix("cid:") {
            return self.resolve_content_id(content_id).await;
        }
        if let Some(url) = crate::transport_url::decode_transport_url(id) {
            // Not recorded in the index: a transport URL is caller-supplied, and the index is
            // shared state whose trackers the infohash does not bind (#164).
            return stream_info_from_transport_url(&url)
                .await
                .map_err(|e| ProviderError::Backend(format!("transport url: {e:?}")));
        }
        if is_bare_hex40(id) {
            return self
                .resolve_infohash_with_catalog(id, resolve_infohash_via_catalog(id))
                .await;
        }
        Err(ProviderError::Backend(
            "id must be a 40-hex infohash, cid:<40hex>, or a transport-url id".into(),
        ))
    }

    /// The verified live descriptor for a bare 40-hex infohash, or
    /// [`ProviderError::Unresolvable`] with a user-facing reason. Offline and synchronous, so
    /// local metadata can take precedence over asynchronous catalog lookup.
    ///
    /// A broadcast this daemon originates wins: its transport is self-minted. Otherwise the
    /// shared index serves a descriptor resolved through the signed catalog. BEP-9 and
    /// transport-url descriptors are never in the index: neither binds the descriptor's
    /// trackers to the infohash (#164).
    pub(super) fn verified_info_for_infohash(&self, id: &str) -> Result<StreamInfo, ProviderError> {
        let infohash = hex20(id).map_err(|_| ProviderError::Backend("bad infohash".into()))?;
        // Decoding recomputes the infohash from the minted bytes.
        if let Some(transport) = self
            .seed_registry
            .broadcast_transport_for_infohash(&infohash)
        {
            if let Ok(info) = stream_info_from_transport(&transport) {
                if info.infohash == infohash {
                    return Ok(info);
                }
            }
        }
        if let Some(info) = self.infohash_index.get(&infohash) {
            return Ok(info);
        }
        Err(ProviderError::Unresolvable(unresolved_infohash_message(id)))
    }

    pub(super) async fn resolve_infohash_with_catalog(
        &self,
        id: &str,
        catalog: impl std::future::Future<Output = Result<StreamInfo, ResolveError>>,
    ) -> Result<StreamInfo, ProviderError> {
        let requested = hex20(id).map_err(|_| ProviderError::Backend("bad infohash".into()))?;
        if let Ok(info) = self.verified_info_for_infohash(id) {
            return Ok(info);
        }
        if self
            .infohash_failures
            .lock()
            .unwrap()
            .iter()
            .any(|(key, at)| *key == requested && at.elapsed() < INFOHASH_FAILURE_TTL)
        {
            return Err(ProviderError::Unresolvable(format!(
                "{} Recent catalog lookup failed; retry shortly.",
                unresolved_infohash_message(id)
            )));
        }
        let info = match catalog.await {
            Ok(info) => info,
            Err(error) => {
                self.remember_infohash_failure(requested);
                return Err(ProviderError::Unresolvable(format!(
                    "{} Catalog lookup failed: {error:?}",
                    unresolved_infohash_message(id)
                )));
            }
        };
        if info.infohash != requested {
            self.remember_infohash_failure(requested);
            return Err(ProviderError::Unresolvable(
                "catalog descriptor does not match the requested infohash".into(),
            ));
        }
        self.infohash_index.put(info.clone());
        crate::alog!("[ace] resolved infohash via authenticated catalog");
        Ok(info)
    }

    pub(super) fn remember_infohash_failure(&self, requested: [u8; 20]) {
        let mut failures = self.infohash_failures.lock().unwrap();
        failures.retain(|(key, at)| *key != requested && at.elapsed() < INFOHASH_FAILURE_TTL);
        if failures.len() >= INFOHASH_FAILURE_CAPACITY {
            failures.pop_front();
        }
        failures.push_back((requested, Instant::now()));
    }

    #[cfg(test)]
    pub(crate) fn cache_infohash_failure_for_test(&self, id: &str) {
        self.remember_infohash_failure(hex20(id).expect("synthetic infohash"));
    }

    /// Resolve `id` to a single-file VOD, returning a handle that knows its total length and can
    /// open verified byte ranges. Errors if the id is live, multi-file, or carries no VOD
    /// descriptor (a bare infohash has no piece hashes). Peer discovery and the actual download
    /// are deferred to [`AceVodContent::open_range`], so a seek fetches only its covering pieces.
    pub(super) async fn resolve_vod_inner(
        &self,
        id: &str,
    ) -> Result<Box<dyn VodContent>, ProviderError> {
        let info = self.resolve_vod_info(id).await?;
        let infohash = info.infohash;
        let piece_length = info.piece_length;
        let chunk_length = info.chunk_length;
        let store = self.seed_registry.lease_store(infohash, || {
            build_piece_store(
                piece_length,
                chunk_length,
                self.seed_store_bytes,
                // VOD keeps downloaded pieces for range serving — byte-only, no age bound.
                None,
                self.cache_type,
                &self.cache_dir,
                &infohash,
            )
        });
        Ok(Box::new(AceVodContent {
            info,
            bootstrap_peers: self.bootstrap_peers.clone(),
            announce_peer_port: self.announce_peer_port.clone(),
            store,
            peers: Arc::new(tokio::sync::Mutex::new(None)),
            range_lock: Arc::new(tokio::sync::Mutex::new(())),
        }))
    }

    /// Fetch the VOD transport descriptor for `id` and decode it into a [`VodInfo`]. Accepts a
    /// `cid:<40hex>` content-id (local broadcast or catalog; raw-hash peers require opt-in)
    /// or a transport-url id; a
    /// bare infohash is rejected (it carries no descriptor / piece hashes).
    async fn resolve_vod_info(&self, id: &str) -> Result<VodInfo, ProviderError> {
        self.resolve_vod_info_with_catalog(
            id,
            catalog_transport_bytes(id.strip_prefix("cid:").unwrap_or_default()),
        )
        .await
    }

    pub(super) async fn resolve_vod_info_with_catalog(
        &self,
        id: &str,
        catalog: impl std::future::Future<Output = Result<Vec<u8>, ResolveError>>,
    ) -> Result<VodInfo, ProviderError> {
        let bytes = if let Some(content_id) = id.strip_prefix("cid:") {
            let key =
                hex20(content_id).map_err(|_| ProviderError::Backend("bad content-id".into()))?;
            self.content_id_transport_with_catalog(key, catalog).await?
        } else if let Some(url) = crate::transport_url::decode_transport_url(id) {
            transport_bytes_from_url(&url)
                .await
                .map_err(|e| ProviderError::Backend(format!("transport url: {e:?}")))?
        } else if id.len() == 40 && id.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(ProviderError::Backend(
                "a bare infohash is not a VOD target (no descriptor with piece hashes)".into(),
            ));
        } else {
            return Err(ProviderError::Backend(
                "id must be cid:<40hex> or a transport-url id for VOD".into(),
            ));
        };
        vod_info_from_transport(&bytes)
            .map_err(|e| ProviderError::Backend(format!("vod descriptor: {e:?}")))
    }

    async fn content_id_transport_with_catalog(
        &self,
        key: [u8; 20],
        catalog: impl std::future::Future<Output = Result<Vec<u8>, ResolveError>>,
    ) -> Result<Vec<u8>, ProviderError> {
        if let Some(bytes) = self.seed_registry.broadcast_transport_for_content_id(&key) {
            return Ok(bytes.as_ref().clone());
        }
        match catalog.await {
            Ok(bytes) => Ok(bytes),
            Err(error) if !self.bep9_content_id_fallback => {
                Err(disabled_content_id_fallback(&error))
            }
            Err(_) => {
                crate::alog!(
                    "[ace] VOD catalog resolution failed; trying opted-in raw-hash BEP-9 fallback"
                );
                self.vod_transport_via_peers(key).await
            }
        }
    }

    /// Fetch VOD transport bytes over a metadata-swarm peer (BEP-9 `ut_metadata`), trying each
    /// discovered/bootstrap peer in turn — the VOD analogue of the peer fallback in
    /// [`Self::resolve_content_id`].
    async fn vod_transport_via_peers(&self, key: [u8; 20]) -> Result<Vec<u8>, ProviderError> {
        let all = if self.bootstrap_peers.is_empty() {
            discover_peers(
                &self.default_trackers,
                &key,
                &random_peer_id(),
                self.discovery_announce_port(),
            )
            .await
        } else {
            self.bootstrap_peers.clone()
        };
        for addr in all {
            let Ok(Ok(session)) =
                tokio::time::timeout(CONNECT_TIMEOUT, connect(&addr.to_string())).await
            else {
                continue;
            };
            let mut session = session.with_timeout(RESOLVE_PEER_TIMEOUT);
            match transport_bytes_via_peer(&mut session, key, &self.identity).await {
                Ok(b) => return Ok(b),
                Err(e) => crate::alog!("[ace] vod resolve {addr}: {e:?}"),
            }
        }
        Err(ProviderError::Backend(
            "vod content-id resolution: no metadata peer responded".into(),
        ))
    }
}

fn disabled_content_id_fallback(catalog_error: &ResolveError) -> ProviderError {
    ProviderError::Backend(format!(
        "content-id catalog resolution failed: {catalog_error:?}; raw-hash BEP-9 fallback is disabled (OUTPACE_BEP9_CONTENT_ID_FALLBACK=1 to opt in)"
    ))
}

/// Whether `id` is a bare 40-hex string: a swarm infohash, or a content id missing its `cid:`
/// prefix (#165). The two are indistinguishable by shape.
fn is_bare_hex40(id: &str) -> bool {
    id.len() == 40 && id.bytes().all(|b| b.is_ascii_hexdigit())
}

/// The user-facing reason a bare 40-hex id cannot be opened (#164), with the `cid:` hint for a
/// content id pasted without its prefix (#165).
fn unresolved_infohash_message(id: &str) -> String {
    format!(
        "no verified transport descriptor for infohash {id}: outpace does not guess live stream \
         geometry. Open the stream by content id first (cid:<content-id> on /streams, \
         content_id=<content-id> on /ace/getstream, acestream://<content-id> for outpace play); \
         its infohash then works in this process. If {id} is itself a content id, open it as one \
         (cid:{id})."
    )
}
