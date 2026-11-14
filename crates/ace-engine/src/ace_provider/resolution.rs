use super::*;

impl AceProvider {
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

    #[cfg(test)]
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
        self.resolve_content_id_prepared_with_catalog_classified(content_id, catalog)
            .await
            .map_err(|failure| failure.error)
    }

    /// Resolve a content-id to a [`StreamInfo`] by fetching its `AceStreamTransport` metadata
    /// from the signed catalog path or this daemon's own broadcast registry. With explicit
    /// opt-in, BEP-9 peers can resolve raw wrapped-transport hashes (cached with a TTL).
    /// Only a signed-catalog result is recorded in the
    /// infohash index; a BEP-9 result goes to `peer_resolve_cache` and never makes its infohash
    /// openable (#164).
    async fn resolve_content_id_prepared_with_catalog_classified(
        &self,
        content_id: &str,
        catalog: impl std::future::Future<Output = Result<StreamInfo, ResolveError>> + Send + 'static,
    ) -> Result<ResolvedContentId, ProviderStartFailure> {
        if let Some(info) = self
            .resolve_cache
            .get(content_id)
            .filter(|_| !self.peer_policy.is_private())
        {
            info.ensure_playable().map_err(ProviderStartFailure::from)?;
            // Re-record: the bounded infohash index may have evicted it since (#164).
            self.infohash_index.put(info.clone());
            return Ok(ResolvedContentId::Catalog(info));
        }
        let key = hex20(content_id).map_err(|_| {
            ProviderStartFailure::new(
                StartFailureKind::Resolution(
                    ace_swarm::resolve::ResolveFailureKind::InvalidSelector,
                ),
                ProviderError::Backend("bad content-id".into()),
            )
        })?;
        if let Some(bytes) = self.seed_registry.broadcast_transport_for_content_id(&key) {
            let info = stream_info_from_transport(&bytes).map_err(ProviderStartFailure::from)?;
            info.ensure_playable().map_err(ProviderStartFailure::from)?;
            return Ok(ResolvedContentId::Provider(info));
        }
        if self.bep9_content_id_fallback {
            if let Some(info) = self.peer_resolve_cache.get(content_id) {
                info.ensure_playable().map_err(ProviderStartFailure::from)?;
                return Ok(ResolvedContentId::Provider(info));
            }
        }

        if self.peer_policy.is_private() {
            if !self.bep9_content_id_fallback {
                return Err(ProviderStartFailure::new(
                    StartFailureKind::Resolution(
                        ace_swarm::resolve::ResolveFailureKind::SafetyRefusal,
                    ),
                    ProviderError::Unresolvable("catalog_disabled_in_private_mode".into()),
                ));
            }
        } else {
            let catalog_error = match self.resolve_cache.resolve_with(content_id, catalog).await {
                Ok(info) => {
                    info.ensure_playable().map_err(ProviderStartFailure::from)?;
                    crate::alog!("[ace] resolved content id via catalog");
                    self.infohash_index.put(info.clone());
                    return Ok(ResolvedContentId::Catalog(info));
                }
                Err(CatalogLookupError::Capacity) => {
                    return Err(ProviderStartFailure::new(
                        StartFailureKind::Capacity,
                        ProviderError::ResolverBusy(
                            "content-id catalog resolution capacity exceeded".into(),
                        ),
                    ));
                }
                Err(CatalogLookupError::Catalog(error)) => error,
            };
            match catalog_error.as_ref() {
                // A classified HLS descriptor is not a catalog outage: it must not be
                // reinterpreted as raw-hash peer data, and its signed URL is never cached.
                ResolveError::HlsOrigin(origin) => {
                    return Err(ResolveError::HlsOrigin(origin.clone()).into())
                }
                ResolveError::Hls(reason) => return Err(ResolveError::Hls(reason).into()),
                ResolveError::ProtectedContent => {
                    return Err(ProviderError::ProtectedContent.into())
                }
                ResolveError::InvalidProtection => {
                    return Err(ProviderError::InvalidProtection.into())
                }
                ResolveError::Transport(reason) => {
                    return Err(ProviderStartFailure::new(
                        StartFailureKind::Resolution(catalog_error.failure_kind()),
                        ProviderError::Backend(format!("invalid descriptor metadata: {reason}")),
                    ));
                }
                _ => {}
            }
            if !self.bep9_content_id_fallback {
                return Err(ProviderStartFailure::new(
                    StartFailureKind::Resolution(catalog_error.failure_kind()),
                    disabled_content_id_fallback(&catalog_error),
                ));
            }
            crate::alog!(
                "[ace] catalog resolution failed; trying opted-in raw-hash BEP-9 fallback"
            );
        }

        let all = if self.peer_policy.is_private() {
            self.peer_policy.endpoints()
        } else if self.bootstrap_peers.is_empty() {
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
            let Ok(Ok(session)) = tokio::time::timeout(
                CONNECT_TIMEOUT,
                self.peer_policy.dial_with(addr, |endpoint| async move {
                    connect(&endpoint.to_string()).await
                }),
            )
            .await
            else {
                continue; // unreachable peer; don't waste the log on it
            };
            // Bound each peer's reads so a connected-but-silent peer doesn't stall resolution.
            let mut session = session.with_timeout(RESOLVE_PEER_TIMEOUT);
            match async {
                stream_info_from_transport(
                    &ace_swarm::resolve::transport_bytes_via_peer_with_policy(
                        &mut session,
                        key,
                        &self.identity,
                        &self.peer_policy,
                    )
                    .await?,
                )
            }
            .await
            {
                Ok(info) => {
                    info.ensure_playable().map_err(ProviderStartFailure::from)?;
                    crate::alog!("[ace] resolved raw-hash content id via metadata peer");
                    // A raw transport hash is not catalog authentication: keep the descriptor
                    // out of the infohash index (#164).
                    self.peer_resolve_cache.put(content_id, info.clone());
                    return Ok(ResolvedContentId::Provider(info));
                }
                Err(error @ (ResolveError::ProtectedContent | ResolveError::InvalidProtection)) => {
                    return Err(ProviderStartFailure::from(error))
                }
                Err(ResolveError::Peer(why)) => {
                    if self.peer_policy.is_private() {
                        crate::alog!("[ace] private metadata peer failed");
                    } else {
                        crate::alog!("[ace] resolve {addr}: {why}");
                    }
                }
                Err(e) => {
                    if self.peer_policy.is_private() {
                        crate::alog!("[ace] private metadata peer failed");
                    } else {
                        crate::alog!("[ace] resolve {addr}: {e:?}");
                    }
                }
            }
        }
        Err(ProviderStartFailure::new(
            StartFailureKind::Discovery,
            ProviderError::NoPeers("content-id resolution: no metadata peer responded".into()),
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
    #[cfg(test)]
    pub(super) async fn resolve_live_info(&self, id: &str) -> Result<StreamInfo, ProviderError> {
        self.resolve_live_info_classified(id)
            .await
            .map_err(|failure| failure.error)
    }

    pub(super) async fn resolve_live_info_classified(
        &self,
        id: &str,
    ) -> Result<StreamInfo, ProviderStartFailure> {
        if id.starts_with("local:") {
            return match self.local_descriptors.get(id) {
                Some(crate::local_descriptors::LocalDescriptor::Live(info)) => Ok(info.clone()),
                Some(_) => Err(ProviderError::Unresolvable(
                    "local_descriptor_is_not_live".into(),
                )),
                None => Err(ProviderError::Unresolvable(
                    "local_descriptor_unknown".into(),
                )),
            }
            .map_err(|error| {
                ProviderStartFailure::new(
                    StartFailureKind::Resolution(
                        ace_swarm::resolve::ResolveFailureKind::InvalidSelector,
                    ),
                    error,
                )
            });
        }

        if let Some(content_id) = id.strip_prefix("cid:") {
            let cid = content_id.to_owned();
            return self
                .resolve_content_id_prepared_with_catalog_classified(content_id, async move {
                    catalog_live_info(&cid).await
                })
                .await
                .map(ResolvedContentId::into_info);
        }
        if let Some(url) = crate::transport_url::decode_transport_url(id) {
            if self.peer_policy.is_private() {
                return Err(ProviderStartFailure::new(
                    StartFailureKind::Resolution(
                        ace_swarm::resolve::ResolveFailureKind::SafetyRefusal,
                    ),
                    ProviderError::Unresolvable(
                        "remote_descriptor_disabled_in_private_mode".into(),
                    ),
                ));
            }
            if let Some(info) = self.peer_resolve_cache.get(id) {
                return Ok(info);
            }
            // Not recorded in the index: a transport URL is caller-supplied, and the index is
            // shared state whose trackers the infohash does not bind (#164).
            return stream_info_from_transport_url(&url).await.map_err(|error| {
                let kind = StartFailureKind::Resolution(error.failure_kind());
                let legacy = match error {
                    ResolveError::ProtectedContent
                    | ResolveError::InvalidProtection
                    | ResolveError::HlsOrigin(_)
                    | ResolveError::Hls(_) => ProviderError::from(error),
                    other => ProviderError::Backend(format!("transport url: {other:?}")),
                };
                ProviderStartFailure::new(kind, legacy)
            });
        }
        if is_bare_hex40(id) {
            return self
                .resolve_infohash_with_catalog_classified(id, resolve_infohash_via_catalog(id))
                .await;
        }
        Err(ProviderStartFailure::new(
            StartFailureKind::Resolution(ace_swarm::resolve::ResolveFailureKind::InvalidSelector),
            ProviderError::Backend(
                "id must be a 40-hex infohash, cid:<40hex>, or a transport-url id".into(),
            ),
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
        if let Some(info) = self
            .infohash_index
            .get(&infohash)
            .filter(|_| !self.peer_policy.is_private())
        {
            return Ok(info);
        }
        Err(ProviderError::Unresolvable(unresolved_infohash_message(id)))
    }

    #[cfg(test)]
    pub(super) async fn resolve_infohash_with_catalog(
        &self,
        id: &str,
        catalog: impl std::future::Future<Output = Result<StreamInfo, ResolveError>>,
    ) -> Result<StreamInfo, ProviderError> {
        self.resolve_infohash_with_catalog_classified(id, catalog)
            .await
            .map_err(|failure| failure.error)
    }

    pub(super) async fn resolve_infohash_with_catalog_classified(
        &self,
        id: &str,
        catalog: impl std::future::Future<Output = Result<StreamInfo, ResolveError>>,
    ) -> Result<StreamInfo, ProviderStartFailure> {
        let requested = hex20(id).map_err(|_| {
            ProviderStartFailure::new(
                StartFailureKind::Resolution(
                    ace_swarm::resolve::ResolveFailureKind::InvalidSelector,
                ),
                ProviderError::Backend("bad infohash".into()),
            )
        })?;
        if let Ok(info) = self.verified_info_for_infohash(id) {
            return Ok(info);
        }
        if self.peer_policy.is_private() {
            return Err(ProviderStartFailure::new(
                StartFailureKind::Resolution(ace_swarm::resolve::ResolveFailureKind::SafetyRefusal),
                ProviderError::Unresolvable("catalog_disabled_in_private_mode".into()),
            ));
        }
        if self
            .infohash_failures
            .lock()
            .unwrap()
            .iter()
            .any(|(key, at)| *key == requested && at.elapsed() < INFOHASH_FAILURE_TTL)
        {
            return Err(ProviderStartFailure::new(
                StartFailureKind::Cooldown,
                ProviderError::Unresolvable(format!(
                    "{} Recent catalog lookup failed; retry shortly.",
                    unresolved_infohash_message(id)
                )),
            ));
        }
        let info = match catalog.await {
            Ok(info) => info,
            Err(error @ (ResolveError::ProtectedContent | ResolveError::InvalidProtection)) => {
                return Err(ProviderStartFailure::from(error))
            }
            Err(error) => {
                self.remember_infohash_failure(requested);
                let kind = error.failure_kind();
                let reason = format!(
                    "{} Catalog lookup failed: {error:?}",
                    unresolved_infohash_message(id)
                );
                // An unreachable catalog keeps the `Unresolvable` statuses and message, but
                // native clients see `network` rather than a refusal of the id.
                let error = if kind == ace_swarm::resolve::ResolveFailureKind::Network {
                    ProviderError::UnresolvableNetwork(reason)
                } else {
                    ProviderError::Unresolvable(reason)
                };
                return Err(ProviderStartFailure::new(
                    StartFailureKind::Resolution(kind),
                    error,
                ));
            }
        };
        if info.infohash != requested {
            self.remember_infohash_failure(requested);
            return Err(ProviderStartFailure::new(
                StartFailureKind::Resolution(
                    ace_swarm::resolve::ResolveFailureKind::RejectedMetadata,
                ),
                ProviderError::Unresolvable(
                    "catalog descriptor does not match the requested infohash".into(),
                ),
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
        self.resolve_vod_inner_with_catalog(
            id,
            catalog_transport_bytes(id.strip_prefix("cid:").unwrap_or_default()),
        )
        .await
    }

    pub(super) async fn resolve_vod_inner_with_catalog(
        &self,
        id: &str,
        catalog: impl std::future::Future<Output = Result<Vec<u8>, ResolveError>>,
    ) -> Result<Box<dyn VodContent>, ProviderError> {
        let info = self.resolve_vod_info_with_catalog(id, catalog).await?;
        info.ensure_playable().map_err(ProviderError::from)?;
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
        self.check_vod_disk_space(&store.0, info.total_length)
            .await?;
        Ok(Box::new(AceVodContent {
            peer_policy: self.peer_policy.clone(),
            info,
            bootstrap_peers: self.bootstrap_peers.clone(),
            announce_peer_port: self.announce_peer_port.clone(),
            store,
            peers: Arc::new(tokio::sync::Mutex::new(None)),
            range_lock: Arc::new(tokio::sync::Mutex::new(())),
            no_space: Default::default(),
        }))
    }

    /// Refuse a VOD whose pieces would not fit in the disk cache (#485). Runs once per
    /// resolution: one free-space query, never on the piece path. Memory-backed stores write
    /// nothing to disk and skip it; bytes the store already holds for this content need no new
    /// space. Dropping the caller's lease on refusal releases a store created for this start.
    async fn check_vod_disk_space(
        &self,
        store: &Arc<tokio::sync::Mutex<PieceStore>>,
        total: u64,
    ) -> Result<(), ProviderError> {
        let (budget, held) = {
            let guard = store.lock().await;
            if !guard.is_disk_backed() {
                return Ok(());
            }
            (guard.max_bytes(), guard.retained_bytes())
        };
        let dir = self.cache_dir.clone();
        let available =
            tokio::task::spawn_blocking(move || crate::disk_space::available_bytes(&dir))
                .await
                .ok()
                .flatten();
        crate::disk_space::check_fit(total, budget, held, available, self.vod_disk_reserve).map_err(
            |no_space| {
                crate::alog!(
                    "[vod] start refused: code=no_space required={} available={}",
                    no_space.required,
                    no_space.available
                );
                ProviderError::NoSpace(no_space.message())
            },
        )
    }

    /// Fetch the VOD transport descriptor for `id` and decode it into a [`VodInfo`]. Accepts a
    /// `cid:<40hex>` content-id (local broadcast or catalog; raw-hash peers require opt-in)
    /// or a transport-url id; a
    /// bare infohash is rejected (it carries no descriptor / piece hashes).
    pub(super) async fn resolve_vod_info_with_catalog(
        &self,
        id: &str,
        catalog: impl std::future::Future<Output = Result<Vec<u8>, ResolveError>>,
    ) -> Result<VodInfo, ProviderError> {
        if id.starts_with("local:") {
            return match self.local_descriptors.get(id) {
                Some(crate::local_descriptors::LocalDescriptor::Vod(info)) => Ok(info.clone()),
                Some(_) => Err(ProviderError::Unresolvable(
                    "local_descriptor_is_not_vod".into(),
                )),
                None => Err(ProviderError::Unresolvable(
                    "local_descriptor_unknown".into(),
                )),
            };
        }
        let bytes = if let Some(content_id) = id.strip_prefix("cid:") {
            let key =
                hex20(content_id).map_err(|_| ProviderError::Backend("bad content-id".into()))?;
            self.content_id_transport_with_catalog(key, catalog).await?
        } else if let Some(url) = crate::transport_url::decode_transport_url(id) {
            if self.peer_policy.is_private() {
                return Err(ProviderError::Unresolvable(
                    "remote_descriptor_disabled_in_private_mode".into(),
                ));
            }
            transport_bytes_from_url(&url)
                .await
                .map_err(ProviderError::from)?
        } else if id.len() == 40 && id.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(ProviderError::Backend(
                "a bare infohash is not a VOD target (no descriptor with piece hashes)".into(),
            ));
        } else {
            return Err(ProviderError::Backend(
                "id must be cid:<40hex> or a transport-url id for VOD".into(),
            ));
        };
        vod_info_from_transport(&bytes).map_err(ProviderError::from)
    }

    async fn content_id_transport_with_catalog(
        &self,
        key: [u8; 20],
        catalog: impl std::future::Future<Output = Result<Vec<u8>, ResolveError>>,
    ) -> Result<Vec<u8>, ProviderError> {
        if let Some(bytes) = self.seed_registry.broadcast_transport_for_content_id(&key) {
            return Ok(bytes.as_ref().clone());
        }
        if self.peer_policy.is_private() {
            if !self.bep9_content_id_fallback {
                return Err(ProviderError::Unresolvable(
                    "catalog_disabled_in_private_mode".into(),
                ));
            }
            return self.vod_transport_via_peers(key).await;
        }
        match catalog.await {
            Ok(bytes) => Ok(bytes),
            Err(
                error @ (ResolveError::ProtectedContent
                | ResolveError::InvalidProtection
                | ResolveError::Transport(_)),
            ) => Err(ProviderError::from(error)),
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
        let all = if self.peer_policy.is_private() {
            self.peer_policy.endpoints()
        } else if self.bootstrap_peers.is_empty() {
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
            let Ok(Ok(session)) = tokio::time::timeout(
                CONNECT_TIMEOUT,
                self.peer_policy.dial_with(addr, |endpoint| async move {
                    connect(&endpoint.to_string()).await
                }),
            )
            .await
            else {
                continue;
            };
            let mut session = session.with_timeout(RESOLVE_PEER_TIMEOUT);
            match ace_swarm::resolve::transport_bytes_via_peer_with_policy(
                &mut session,
                key,
                &self.identity,
                &self.peer_policy,
            )
            .await
            {
                Ok(b) => return Ok(b),
                Err(e) => {
                    if self.peer_policy.is_private() {
                        crate::alog!("[ace] private VOD metadata peer failed");
                    } else {
                        crate::alog!("[ace] vod resolve {addr}: {e:?}");
                    }
                }
            }
        }
        Err(ProviderError::NoPeers(
            "vod content-id resolution: no metadata peer responded".into(),
        ))
    }
}

fn disabled_content_id_fallback(catalog_error: &ResolveError) -> ProviderError {
    let message = format!(
        "content-id catalog resolution failed: {catalog_error:?}; raw-hash BEP-9 fallback is disabled (OUTPACE_BEP9_CONTENT_ID_FALLBACK=1 to opt in)"
    );
    // Catalog hosts unreachable is a network condition, not a refusal of this id.
    if catalog_error.failure_kind() == ace_swarm::resolve::ResolveFailureKind::Network {
        ProviderError::Network(message)
    } else {
        ProviderError::Backend(message)
    }
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

#[cfg(test)]
mod classified_tests {
    use super::*;
    use crate::start_failure::StartFailureKind;
    use ace_swarm::resolve::ResolveFailureKind;

    #[tokio::test]
    async fn classified_content_id_retains_catalog_network_and_response_causes() {
        let provider = AceProvider::new(Arc::new(Identity::generate()), 0);
        let id = "0123456789abcdef0123456789abcdef01234567";
        let network = provider
            .resolve_content_id_prepared_with_catalog_classified(id, async {
                Err(ResolveError::CatalogNetwork("same diagnostic"))
            })
            .await
            .err()
            .unwrap();
        let response = provider
            .resolve_content_id_prepared_with_catalog_classified(id, async {
                Err(ResolveError::Catalog("same diagnostic"))
            })
            .await
            .err()
            .unwrap();
        assert_eq!(network.error.to_string(), response.error.to_string());
        // Same diagnostic, distinct typed variants for the native `network`/`internal` codes.
        assert!(matches!(network.error, ProviderError::Network(_)));
        assert!(matches!(response.error, ProviderError::Backend(_)));
        assert_eq!(
            network.kind,
            StartFailureKind::Resolution(ResolveFailureKind::Network)
        );
        assert_eq!(
            response.kind,
            StartFailureKind::Resolution(ResolveFailureKind::RejectedMetadata)
        );
        assert!(network.kind.retryable() && response.kind.retryable());
        let invalid = provider
            .resolve_content_id_prepared_with_catalog_classified("bad", async {
                panic!("invalid selector must not poll catalog")
            })
            .await
            .err()
            .unwrap();
        assert_eq!(
            invalid.kind,
            StartFailureKind::Resolution(ResolveFailureKind::InvalidSelector)
        );
        assert!(!invalid.kind.retryable());
    }

    #[tokio::test]
    async fn catalog_hls_origin_hands_off_and_never_enters_the_cache() {
        const URL: &str = "https://cdn.example.test/live/master.m3u8?token=synthetic";
        let provider = AceProvider::new(Arc::new(Identity::generate()), 0);
        let id = "0123456789abcdef0123456789abcdef01234567";
        let fetches = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        for _ in 0..2 {
            let counted = fetches.clone();
            let failure = provider
                .resolve_content_id_prepared_with_catalog_classified(id, async move {
                    counted.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    Err(ResolveError::HlsOrigin(Box::new(
                        ace_swarm::resolve::HlsOrigin {
                            manifest_url: URL.into(),
                            metadata: Default::default(),
                        },
                    )))
                })
                .await
                .err()
                .unwrap();
            let ProviderError::DirectHls(origin) = &failure.error else {
                panic!("expected an origin handoff, got {:?}", failure.error);
            };
            assert_eq!(origin.manifest_url, URL);
            assert!(!format!("{:?}", failure.error).contains("cdn.example.test"));
            assert!(!failure.error.to_string().contains("cdn.example.test"));
        }
        assert_eq!(
            fetches.load(std::sync::atomic::Ordering::SeqCst),
            2,
            "signed origin URLs are resolved per playback, not cached"
        );
        assert!(provider.resolve_cache.get(id).is_none());
    }

    #[tokio::test]
    async fn bare_infohash_catalog_outage_is_typed_network_but_keeps_unresolvable_text() {
        let id = "0123456789abcdef0123456789abcdef01234567";
        let provider = AceProvider::new(Arc::new(Identity::generate()), 0);
        let network = provider
            .resolve_infohash_with_catalog_classified(id, async {
                Err(ResolveError::CatalogNetwork("diagnostic"))
            })
            .await
            .err()
            .unwrap();
        assert!(matches!(
            network.error,
            ProviderError::UnresolvableNetwork(_)
        ));
        assert_eq!(
            network.kind,
            StartFailureKind::Resolution(ResolveFailureKind::Network)
        );
        assert!(network
            .error
            .to_string()
            .starts_with(&unresolved_infohash_message(id)));
        let provider = AceProvider::new(Arc::new(Identity::generate()), 0);
        let refused = provider
            .resolve_infohash_with_catalog_classified(id, async {
                Err(ResolveError::Catalog("diagnostic"))
            })
            .await
            .err()
            .unwrap();
        assert!(matches!(refused.error, ProviderError::Unresolvable(_)));
    }

    #[tokio::test]
    async fn classified_content_id_catalog_capacity_is_retryable_without_fetch() {
        let provider = AceProvider::new(Arc::new(Identity::generate()), 0);
        let ids: Vec<_> = (0..32).map(|index| infohash_hex(&[index; 20])).collect();
        let mut pending: Vec<_> = ids
            .iter()
            .map(|id| {
                Box::pin(provider.resolve_cache.resolve_with(
                    id,
                    std::future::pending::<Result<StreamInfo, ResolveError>>(),
                ))
            })
            .collect();
        for future in &mut pending {
            assert!(futures::poll!(future.as_mut()).is_pending());
        }
        let error = provider
            .resolve_content_id_prepared_with_catalog_classified(
                "0123456789abcdef0123456789abcdef01234567",
                async { panic!("saturated catalog must not start another fetch") },
            )
            .await
            .err()
            .unwrap();
        drop(pending);
        assert_eq!(error.kind, StartFailureKind::Capacity);
        // Typed for the native `capacity` code; HTTP statuses stay those of `Backend`.
        assert!(matches!(error.error, ProviderError::ResolverBusy(_)));
        assert!(error.kind.retryable());
        assert_eq!(
            error.error.to_string(),
            "content-id catalog resolution capacity exceeded"
        );
    }

    #[tokio::test]
    async fn classified_open_distinguishes_invalid_input_from_temporary_metadata_absence() {
        let provider = AceProvider::new(Arc::new(Identity::generate()), 0);
        let invalid = provider
            .open_classified("not-a-selector")
            .await
            .err()
            .unwrap();
        let alias = provider
            .open_classified("local:unknown")
            .await
            .err()
            .unwrap();
        let synthetic = "0123456789abcdef0123456789abcdef01234567";
        provider.cache_infohash_failure_for_test(synthetic);
        let cooldown = provider.open_classified(synthetic).await.err().unwrap();
        assert_eq!(
            invalid.kind,
            StartFailureKind::Resolution(ResolveFailureKind::InvalidSelector)
        );
        assert!(!alias.kind.retryable());
        assert_eq!(cooldown.kind, StartFailureKind::Cooldown);
        assert!(cooldown.kind.retryable());
    }

    #[tokio::test]
    async fn classified_open_preserves_guarded_url_safety_cause() {
        let provider = AceProvider::new(Arc::new(Identity::generate()), 0);
        let id = crate::transport_url::encode_transport_url("http://127.0.0.1/descriptor").unwrap();
        let error = provider.open_classified(&id).await.err().unwrap();
        assert_eq!(
            error.kind,
            StartFailureKind::Resolution(ResolveFailureKind::SafetyRefusal)
        );
        assert!(!error.kind.retryable());
        assert!(error.error.to_string().contains("blocked address"));
    }
}
