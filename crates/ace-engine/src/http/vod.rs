use super::*;

/// `GET /vod/{network}/{id}` — resolve `id` as a single-file VOD, then stream its verified
/// bytes with a `Content-Length`. The resolved VOD is cached by the manager and shared with the
/// HLS routes, so a playback resolves the descriptor once and reuses downloaded pieces.
pub(super) async fn vod_stream(
    State(s): State<AppState>,
    Path((network, id)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response {
    let vod = match resolve_vod(&s, &network, &id).await {
        Ok(v) => v,
        Err(resp) => return *resp,
    };
    let total = vod.content_length();
    if total == 0 {
        return empty_vod_response();
    }
    let range = headers.get(header::RANGE).and_then(|v| v.to_str().ok());
    match parse_byte_range(range, total) {
        // Whole file (no usable Range): 200 with Accept-Ranges so players know they *can* seek.
        RangeOutcome::Full => match vod.open_range(0, total - 1).await {
            Ok(source) => vod_response_from_source(source),
            Err(e) => (StatusCode::BAD_GATEWAY, format!("{e:?}")).into_response(),
        },
        RangeOutcome::Satisfiable(start, end) => match vod.open_range(start, end).await {
            Ok(source) => vod_partial_response(source, start, end, total),
            Err(e) => (StatusCode::BAD_GATEWAY, format!("{e:?}")).into_response(),
        },
        // A well-formed range that can't be met: 416 with the resource's true length.
        RangeOutcome::Unsatisfiable => axum::http::Response::builder()
            .status(StatusCode::RANGE_NOT_SATISFIABLE)
            .header(header::ACCEPT_RANGES, "bytes")
            .header(header::CONTENT_RANGE, format!("bytes */{total}"))
            .body(Body::empty())
            .expect("valid 416 response")
            .into_response(),
    }
}

/// How a `Range` request header resolves against a known content length.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum RangeOutcome {
    /// No usable single-range spec — serve the whole file (`200`).
    Full,
    /// One satisfiable byte range `[start, end]` (inclusive) — serve `206`.
    Satisfiable(u64, u64),
    /// A well-formed range that lies outside the content — serve `416`.
    Unsatisfiable,
}

/// Interpret a `Range` header against `total` (> 0) content bytes. Only single byte-ranges are
/// honored; anything else (missing header, non-`bytes` unit, multi-range, or unparseable spec)
/// is ignored and treated as [`RangeOutcome::Full`] — RFC 7233 §3.1 lets a server ignore a
/// `Range` it doesn't act on. Only a syntactically valid but out-of-bounds range is
/// [`RangeOutcome::Unsatisfiable`].
pub(super) fn parse_byte_range(header: Option<&str>, total: u64) -> RangeOutcome {
    let Some(spec) = header.and_then(|h| h.strip_prefix("bytes=")) else {
        return RangeOutcome::Full;
    };
    let spec = spec.trim();
    // Single range only; a comma means multiple ranges, which we decline to honor (serve 200).
    if spec.contains(',') {
        return RangeOutcome::Full;
    }
    let Some((first, last)) = spec.split_once('-') else {
        return RangeOutcome::Full;
    };
    let (first, last) = (first.trim(), last.trim());
    match (first.is_empty(), last.is_empty()) {
        // `-N`: the last N bytes (clamped to the whole file).
        (true, false) => match last.parse::<u64>() {
            Ok(0) => RangeOutcome::Unsatisfiable,
            Ok(n) => RangeOutcome::Satisfiable(total.saturating_sub(n), total - 1),
            Err(_) => RangeOutcome::Full,
        },
        // `N-`: from byte N to the end.
        (false, true) => match first.parse::<u64>() {
            Ok(start) if start < total => RangeOutcome::Satisfiable(start, total - 1),
            Ok(_) => RangeOutcome::Unsatisfiable,
            Err(_) => RangeOutcome::Full,
        },
        // `N-M`: an explicit range (end clamped to the last byte).
        (false, false) => match (first.parse::<u64>(), last.parse::<u64>()) {
            (Ok(start), Ok(end)) if start <= end && start < total => {
                RangeOutcome::Satisfiable(start, end.min(total - 1))
            }
            (Ok(_), Ok(_)) => RangeOutcome::Unsatisfiable,
            _ => RangeOutcome::Full,
        },
        // A bare `-`: not a valid range, ignore it.
        (true, true) => RangeOutcome::Full,
    }
}

/// Build an empty `200` for a zero-length VOD (no range possible), advertising `Accept-Ranges`.
fn empty_vod_response() -> Response {
    axum::http::Response::builder()
        .status(StatusCode::OK)
        .header(header::ACCEPT_RANGES, "bytes")
        .header(header::CONTENT_LENGTH, 0)
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .body(Body::empty())
        .expect("valid empty vod response")
        .into_response()
}

/// Bridge a [`VodByteSource`] to an HTTP body stream.
fn vod_body(source: Box<dyn VodByteSource>) -> Body {
    let stream = futures::stream::unfold(source, |mut src| async move {
        src.next()
            .await
            .map(|chunk| (Ok::<Bytes, std::io::Error>(chunk), src))
    });
    Body::from_stream(stream)
}

/// Build a streaming `200` response for a whole-file [`VodByteSource`], advertising its length
/// and that the resource is range-seekable.
pub(super) fn vod_response_from_source(source: Box<dyn VodByteSource>) -> Response {
    let total = source.content_length();
    axum::http::Response::builder()
        .status(StatusCode::OK)
        .header(header::ACCEPT_RANGES, "bytes")
        .header(header::CONTENT_LENGTH, total)
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .body(vod_body(source))
        .expect("valid vod response")
        .into_response()
}

/// Build a streaming `206 Partial Content` response for the inclusive byte range `[start, end]`
/// of a `total`-byte VOD.
fn vod_partial_response(
    source: Box<dyn VodByteSource>,
    start: u64,
    end: u64,
    total: u64,
) -> Response {
    axum::http::Response::builder()
        .status(StatusCode::PARTIAL_CONTENT)
        .header(header::ACCEPT_RANGES, "bytes")
        .header(header::CONTENT_LENGTH, end - start + 1)
        .header(
            header::CONTENT_RANGE,
            format!("bytes {start}-{end}/{total}"),
        )
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .body(vod_body(source))
        .expect("valid vod partial response")
        .into_response()
}

/// Resolve `id` on `network` to a single-file VOD via the manager's shared cache, or the HTTP
/// error response to return: `404` for an unknown network, `502` if resolution fails.
pub(super) async fn resolve_vod(
    s: &AppState,
    network: &str,
    id: &str,
) -> Result<Arc<dyn VodContent>, Box<Response>> {
    match s.manager.resolve_vod(network, id).await {
        Ok(v) => Ok(v),
        Err(ProviderError::CapacityExhausted) => Err(Box::new(super::streams::capacity_error())),
        Err(ProviderError::NotFound) => Err(Box::new(
            (StatusCode::NOT_FOUND, "unknown network").into_response(),
        )),
        Err(e) => Err(Box::new(
            (StatusCode::BAD_GATEWAY, format!("{e:?}")).into_response(),
        )),
    }
}

/// `GET /vod/{network}/{id}/manifest.m3u8` — a static VOD HLS media playlist for the resolved
/// single-file VOD. The whole file's length is known, so the playlist lists every byte-range
/// segment up front and ends with `#EXT-X-ENDLIST`; segments are fetched lazily via
/// [`vod_hls_segment`]. Segment geometry comes from the shared HLS config (same knobs as live).
pub(super) async fn vod_manifest(
    State(s): State<AppState>,
    Path((network, id)): Path<(String, String)>,
) -> Response {
    let vod = match resolve_vod(&s, &network, &id).await {
        Ok(v) => v,
        Err(resp) => return *resp,
    };
    let layout = crate::hls::VodHlsLayout::new(vod.content_length(), s.manager.hls_config());
    (
        [(header::CONTENT_TYPE, "application/vnd.apple.mpegurl")],
        layout.playlist(&network, &id),
    )
        .into_response()
}

/// `GET /vod/{network}/{id}/seg/{n}.ts` — HLS segment `n` of the resolved VOD, served as the
/// verified byte range the [`VodHlsLayout`](crate::hls::VodHlsLayout) assigns to it. A segment
/// index past the last segment (or a malformed name) 404s.
pub(super) async fn vod_hls_segment(
    State(s): State<AppState>,
    Path((network, id, seg)): Path<(String, String, String)>,
) -> Response {
    let Some(index) = seg.strip_suffix(".ts").and_then(|n| n.parse::<u64>().ok()) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let vod = match resolve_vod(&s, &network, &id).await {
        Ok(v) => v,
        Err(resp) => return *resp,
    };
    let layout = crate::hls::VodHlsLayout::new(vod.content_length(), s.manager.hls_config());
    let Some((start, end)) = layout.segment_range(index) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    match vod.open_range(start, end).await {
        Ok(source) => vod_hls_segment_response(source),
        Err(e) => (StatusCode::BAD_GATEWAY, format!("{e:?}")).into_response(),
    }
}

/// Build a streaming `200` response for one HLS VOD segment (MPEG-TS), advertising its length.
fn vod_hls_segment_response(source: Box<dyn VodByteSource>) -> Response {
    let len = source.content_length();
    axum::http::Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "video/mp2t")
        .header(header::CONTENT_LENGTH, len)
        .body(vod_body(source))
        .expect("valid vod segment response")
        .into_response()
}
