//! Finite guarded media requests, separate from the continuous source policy.
use super::*;
use reqwest::header::{self, HeaderMap};

/// Validated wire encoding. The caller still receives the original, encoded bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ResourceEncoding {
    Identity,
    Gzip,
}

#[derive(Debug)]
pub struct ResourceResponse {
    pub final_url: FetchUrl,
    pub status: u16,
    pub content_type: Option<String>,
    pub content_range: Option<String>,
    pub content_length: Option<u64>,
    pub content_encoding: ResourceEncoding,
    /// The original DNS/redirect/body deadline, also applicable to caller decoding.
    pub deadline: tokio::time::Instant,
    pub cache_ttl: Option<Duration>,
    pub body: GuardedBody,
}

/// Every redirect is admitted and DNS-pinned independently. DNS, all hops and body reads
/// share a deadline. Only a validated single Range header is forwarded to the origin.
pub async fn open_resource(
    url: &FetchUrl,
    policy: FetchPolicy,
    limits: FetchLimits,
    range: Option<&str>,
    cancel: watch::Receiver<bool>,
) -> Result<ResourceResponse, FetchError> {
    open_resource_inner(
        url,
        policy,
        limits,
        range,
        cancel,
        ResourceOptions::default(),
    )
    .await
}

/// Fetch a full HLS playlist, allowing one gzip content coding. No Range requests
/// are permitted. Wire size/length and bounded decoding remain the caller's job.
/// Other resources must use `open_resource`, which continues to require identity.
pub async fn open_playlist_resource(
    url: &FetchUrl,
    policy: FetchPolicy,
    limits: FetchLimits,
    cancel: watch::Receiver<bool>,
) -> Result<ResourceResponse, FetchError> {
    open_resource_inner(
        url,
        policy,
        limits,
        None,
        cancel,
        ResourceOptions {
            playlist: true,
            ..Default::default()
        },
    )
    .await
}

impl ResourceClientPool {
    /// Fetch a finite resource after fresh URL/DNS validation, reusing only a client
    /// pinned to the same permitted authority and address. Range/encoding rules are
    /// identical to the unpooled `open_resource` function.
    pub async fn open_resource(
        &self,
        url: &FetchUrl,
        policy: FetchPolicy,
        limits: FetchLimits,
        range: Option<&str>,
        cancel: watch::Receiver<bool>,
    ) -> Result<ResourceResponse, FetchError> {
        open_resource_inner(
            url,
            policy,
            limits,
            range,
            cancel,
            ResourceOptions {
                pool: Some(self.clone()),
                ..Default::default()
            },
        )
        .await
    }

    /// Fetch a full finite playlist, with the same guarded gzip policy as the
    /// unpooled `open_playlist_resource` function.
    pub async fn open_playlist_resource(
        &self,
        url: &FetchUrl,
        policy: FetchPolicy,
        limits: FetchLimits,
        cancel: watch::Receiver<bool>,
    ) -> Result<ResourceResponse, FetchError> {
        open_resource_inner(
            url,
            policy,
            limits,
            None,
            cancel,
            ResourceOptions {
                playlist: true,
                pool: Some(self.clone()),
                #[cfg(test)]
                hooks: None,
            },
        )
        .await
    }
}

#[derive(Default)]
struct ResourceOptions {
    playlist: bool,
    pool: Option<ResourceClientPool>,
    #[cfg(test)]
    hooks: Option<test_support::TestHooks>,
}

impl ResourceOptions {
    async fn resolve(
        &self,
        url: &FetchUrl,
        policy: FetchPolicy,
        cancel: watch::Receiver<bool>,
    ) -> Result<SocketAddr, FetchError> {
        #[cfg(test)]
        if let Some(hooks) = &self.hooks {
            return hooks.resolve(policy, cancel).await;
        }
        resolve_addr(
            normalized_host(&url.0)?,
            url.0.port_or_known_default().ok_or(FetchError::NoPort)?,
            policy,
            cancel,
        )
        .await
    }
}

async fn open_resource_inner(
    url: &FetchUrl,
    policy: FetchPolicy,
    limits: FetchLimits,
    range: Option<&str>,
    cancel: watch::Receiver<bool>,
    options: ResourceOptions,
) -> Result<ResourceResponse, FetchError> {
    if limits.connect_timeout.is_zero() || limits.headers_timeout.is_zero() {
        return Err(FetchError::InvalidSourceConfig);
    }
    if let Some(range) = range {
        Range::parse(range)?;
    }
    let deadline =
        tokio::time::Instant::now() + limits.headers_timeout.min(Duration::from_secs(30));
    let mut current = FetchUrl::parse_source(url.0.as_str(), policy)?;
    for hop in 0..=3 {
        let attempt = async {
            // Resolve every request and redirect before consulting the client pool.
            let addr = options.resolve(&current, policy, cancel.clone()).await?;
            open_pinned(
                &current,
                addr,
                FetchLimits {
                    source_response: true,
                    ..limits
                },
                cancel.clone(),
                AttemptOptions {
                    resource: true,
                    playlist: options.playlist,
                    range: range.map(str::to_owned),
                    deadline: Some(deadline),
                    pool: options.pool.clone(),
                    policy,
                    #[cfg(test)]
                    tls_root: None,
                },
            )
            .await
        };
        let mut cancellation = cancel.clone();
        let body = tokio::select! {
            biased;
            _ = cancelled(&mut cancellation) => return Err(FetchError::Cancelled),
            result = tokio::time::timeout_at(deadline, attempt) => result.map_err(|_| FetchError::HeadersTimeout)??,
        };
        let response = body.response.as_ref().ok_or(FetchError::BodyFailed)?;
        let status = response.status().as_u16();
        let headers = response.headers();
        if matches!(status, 301 | 302 | 303 | 307 | 308) {
            if hop == 3 {
                return Err(FetchError::RedirectRejected);
            }
            let location =
                header_text(headers, header::LOCATION)?.ok_or(FetchError::RedirectRejected)?;
            let next = current.join_reference(location, policy)?;
            if current.0.scheme() == "https" && next.0.scheme() != "https" {
                return Err(FetchError::RedirectRejected);
            }
            drop(body);
            current = next;
            continue;
        }
        let result = ResourceResponse {
            final_url: current,
            status,
            content_type: header_text(headers, header::CONTENT_TYPE)?.map(str::to_owned),
            content_range: header_text(headers, header::CONTENT_RANGE)?.map(str::to_owned),
            content_length: content_length(headers)?,
            content_encoding: content_encoding(headers, options.playlist)?,
            deadline,
            cache_ttl: if status == 200 && range.is_none() {
                cache_ttl(headers)
            } else {
                None
            },
            body,
        };
        if *cancel.borrow() {
            return Err(FetchError::Cancelled);
        }
        return Ok(result);
    }
    Err(FetchError::RedirectRejected)
}

fn header_text(headers: &HeaderMap, name: header::HeaderName) -> Result<Option<&str>, FetchError> {
    let mut values = headers.get_all(name).iter();
    let first = values.next();
    if values.next().is_some() {
        return Err(FetchError::UnsupportedResponse);
    }
    first
        .map(|v| v.to_str().map_err(|_| FetchError::UnsupportedResponse))
        .transpose()
}

fn decimal(value: &str) -> Option<u64> {
    (!value.is_empty() && value.bytes().all(|b| b.is_ascii_digit()))
        .then(|| value.parse().ok())
        .flatten()
}

fn content_length(headers: &HeaderMap) -> Result<Option<u64>, FetchError> {
    header_text(headers, header::CONTENT_LENGTH)?
        .map(|v| decimal(v).ok_or(FetchError::UnsupportedResponse))
        .transpose()
}

fn content_encoding(headers: &HeaderMap, playlist: bool) -> Result<ResourceEncoding, FetchError> {
    let value = header_text(headers, header::CONTENT_ENCODING)?;
    match value.map(|value| value.trim_matches([' ', '\t'])) {
        None => Ok(ResourceEncoding::Identity),
        Some(value) if value.eq_ignore_ascii_case("identity") => Ok(ResourceEncoding::Identity),
        Some(value) if playlist && value.eq_ignore_ascii_case("gzip") => Ok(ResourceEncoding::Gzip),
        _ => Err(FetchError::UnsupportedResponse),
    }
}

#[derive(Clone, Copy, Debug)]
enum Range {
    Bounded(u64, u64),
    From(u64),
    Suffix(u64),
}

impl Range {
    fn parse(raw: &str) -> Result<Self, FetchError> {
        let invalid = FetchError::InvalidSourceConfig;
        if raw.len() > 64 {
            return Err(invalid);
        }
        let (start, end) = raw
            .strip_prefix("bytes=")
            .and_then(|s| s.split_once('-'))
            .ok_or(invalid)?;
        match (decimal(start), decimal(end)) {
            (Some(start), Some(end))
                if start <= end
                    && end
                        .checked_sub(start)
                        .and_then(|n| n.checked_add(1))
                        .is_some() =>
            {
                Ok(Self::Bounded(start, end))
            }
            (Some(start), None) if end.is_empty() => Ok(Self::From(start)),
            (None, Some(size)) if start.is_empty() && size > 0 => Ok(Self::Suffix(size)),
            _ => Err(invalid),
        }
    }
    fn accepts(self, start: u64, end: u64, total: Option<u64>) -> bool {
        match self {
            Self::Bounded(a, b) => start == a && end == total.map_or(b, |n| b.min(n - 1)),
            Self::From(a) => start == a && total.is_none_or(|n| end == n - 1),
            Self::Suffix(size) => {
                total.is_some_and(|n| start == n.saturating_sub(size) && end == n - 1)
            }
        }
    }
}

pub(super) fn validate_response(
    response: &reqwest::Response,
    range: Option<&str>,
) -> Result<(), FetchError> {
    validate_resource_response(response, range, false)
}

pub(super) fn validate_playlist_response(response: &reqwest::Response) -> Result<(), FetchError> {
    validate_resource_response(response, None, true)
}

fn validate_resource_response(
    response: &reqwest::Response,
    range: Option<&str>,
    playlist: bool,
) -> Result<(), FetchError> {
    let headers = response.headers();
    if headers.len() > MAX_SOURCE_HEADERS
        || headers
            .iter()
            .map(|(n, v)| n.as_str().len() + v.as_bytes().len())
            .sum::<usize>()
            > MAX_SOURCE_HEADER_BYTES
        || headers.contains_key(header::UPGRADE)
        || headers.get_all(header::CONNECTION).iter().any(|v| {
            v.as_bytes()
                .split(|b| *b == b',')
                .any(|t| t.trim_ascii().eq_ignore_ascii_case(b"upgrade"))
        })
    {
        return Err(FetchError::UnsupportedResponse);
    }
    let status = response.status().as_u16();
    // Error representations are never consumed or decoded. Preserve their status
    // even when their content coding differs from the requested media encoding.
    if !matches!(status, 200 | 206 | 301 | 302 | 303 | 307 | 308) {
        return Err(FetchError::ResourceHttpStatus(status));
    }
    content_encoding(headers, playlist)?;
    match status {
        301 | 302 | 303 | 307 | 308 => {
            header_text(headers, header::LOCATION)?;
            return Ok(());
        }
        200 => {
            if headers.contains_key(header::CONTENT_RANGE) {
                return Err(FetchError::UnsupportedResponse);
            }
        }
        206 => {
            let requested = range
                .map(Range::parse)
                .transpose()?
                .ok_or(FetchError::UnsupportedResponse)?;
            let value = header_text(headers, header::CONTENT_RANGE)?
                .ok_or(FetchError::UnsupportedResponse)?;
            let (interval, total) = value
                .strip_prefix("bytes ")
                .and_then(|v| v.split_once('/'))
                .ok_or(FetchError::UnsupportedResponse)?;
            let (start, end) = interval
                .split_once('-')
                .ok_or(FetchError::UnsupportedResponse)?;
            let start = decimal(start).ok_or(FetchError::UnsupportedResponse)?;
            let end = decimal(end).ok_or(FetchError::UnsupportedResponse)?;
            let total = if total == "*" {
                None
            } else {
                Some(decimal(total).ok_or(FetchError::UnsupportedResponse)?)
            };
            let length = end
                .checked_sub(start)
                .and_then(|n| n.checked_add(1))
                .ok_or(FetchError::UnsupportedResponse)?;
            if total.is_some_and(|n| n == 0 || end >= n)
                || !requested.accepts(start, end, total)
                || content_length(headers)?.is_some_and(|n| n != length)
            {
                return Err(FetchError::UnsupportedResponse);
            }
        }
        _ => unreachable!("resource status was checked above"),
    }
    content_length(headers)?;
    header_text(headers, header::CONTENT_TYPE)?;
    Ok(())
}

fn cache_ttl(headers: &HeaderMap) -> Option<Duration> {
    if headers.contains_key(header::VARY) || headers.contains_key(header::SET_COOKIE) {
        return None;
    }
    let mut max_age = None;
    let mut shared_max_age = None;
    for value in headers.get_all(header::CACHE_CONTROL).iter() {
        for directive in value.to_str().ok()?.split(',') {
            let (name, value) = directive
                .trim()
                .split_once('=')
                .map_or((directive.trim(), None), |(n, v)| {
                    (n.trim(), Some(v.trim()))
                });
            if ["no-store", "no-cache", "private"]
                .iter()
                .any(|n| name.eq_ignore_ascii_case(n))
            {
                return None;
            }
            if name.eq_ignore_ascii_case("max-age") || name.eq_ignore_ascii_case("s-maxage") {
                let slot = if name.eq_ignore_ascii_case("s-maxage") {
                    &mut shared_max_age
                } else {
                    &mut max_age
                };
                if slot.is_some() {
                    return None;
                }
                let value = value?;
                let value = if value.starts_with('"') {
                    value.strip_prefix('"')?.strip_suffix('"')?
                } else {
                    value
                };
                *slot = Some(decimal(value)?);
            }
        }
    }
    let age = header_text(headers, header::AGE)
        .ok()?
        .map(decimal)
        .unwrap_or(Some(0))?;
    let apparent_age = match header_text(headers, header::DATE).ok()? {
        None => 0,
        Some(date) => {
            let date = httpdate::parse_http_date(date).ok()?;
            let elapsed = std::time::SystemTime::now()
                .duration_since(date)
                .unwrap_or_default();
            elapsed
                .as_secs()
                .saturating_add(u64::from(elapsed.subsec_nanos() != 0))
        }
    };
    let seconds = shared_max_age
        .or(max_age)?
        .saturating_sub(age.max(apparent_age))
        .min(30);
    (seconds > 0).then_some(Duration::from_secs(seconds))
}

#[cfg(test)]
mod tests;
