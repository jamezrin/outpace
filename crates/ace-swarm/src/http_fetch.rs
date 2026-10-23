//! Guarded, per-attempt HTTP fetching for sources and finite transport descriptors.
//!
//! URLs and client errors never appear in this module's public diagnostics. Each open
//! resolves afresh, selects one permitted address, and retains the original host for TLS.

use bytes::Bytes;
use std::error::Error;
use std::fmt;
use std::future::Future;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;

const DNS_TIMEOUT: Duration = Duration::from_secs(5);
const SOURCE_HEADERS_TIMEOUT: Duration = Duration::from_secs(15);
const MAX_SOURCE_URL: usize = 8_192;
const MAX_SOURCE_HEADERS: usize = 64;
const MAX_SOURCE_HEADER_BYTES: usize = 32 * 1024;
const MAX_SOURCE_FRAME: usize = 256 * 1024;

/// An explicit caller policy; this module never reads environment switches.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct FetchPolicy {
    pub allow_non_global: bool,
}

impl FetchPolicy {
    fn permits(self, ip: &IpAddr) -> bool {
        is_safe_ip(ip) || (self.allow_non_global && is_private_or_loopback(ip))
    }
}

/// A parsed, normalized URL. Its serialized form is deliberately private.
#[derive(Clone, PartialEq, Eq)]
pub struct FetchUrl(reqwest::Url);

impl fmt::Debug for FetchUrl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("FetchUrl([redacted])")
    }
}

impl FetchUrl {
    /// Admit a source without DNS or any other network side effect.
    pub fn parse_source(raw: &str, policy: FetchPolicy) -> Result<Self, FetchError> {
        if raw.len() > MAX_SOURCE_URL || raw.chars().any(char::is_control) {
            return Err(FetchError::InvalidSourceUrl);
        }
        // The WHATWG parser repairs missing authority slashes and erases empty userinfo.
        // Inspect the original authority too, before either repair can hide invalid syntax.
        let (scheme, rest) = raw.split_once("://").ok_or(FetchError::InvalidSourceUrl)?;
        if !scheme.eq_ignore_ascii_case("http") && !scheme.eq_ignore_ascii_case("https") {
            return Err(FetchError::InvalidSourceUrl);
        }
        let authority = rest.split(['/', '?', '#']).next().unwrap_or_default();
        if authority.is_empty() || authority.contains(['@', '\\']) {
            return Err(FetchError::InvalidSourceUrl);
        }
        let url = reqwest::Url::parse(raw).map_err(|_| FetchError::InvalidSourceUrl)?;
        if url.host_str().is_none()
            || url.port_or_known_default().is_none_or(|port| port == 0)
            || !url.username().is_empty()
            || url.password().is_some()
            || url.fragment().is_some()
        {
            return Err(FetchError::InvalidSourceUrl);
        }
        if let Ok(ip) = normalized_host(&url)?.parse::<IpAddr>() {
            if !policy.permits(&ip) {
                return Err(FetchError::SourceAddressBlocked);
            }
        }
        Ok(Self(url))
    }

    /// Preserve the transport wrapper's existing URL parser acceptance surface.
    pub(crate) fn parse_transport(raw: &str) -> Result<Self, FetchError> {
        let url = reqwest::Url::parse(raw).map_err(|_| FetchError::BadUrl)?;
        if !matches!(url.scheme(), "http" | "https") {
            return Err(FetchError::UnsupportedScheme);
        }
        normalized_host(&url)?;
        url.port_or_known_default().ok_or(FetchError::NoPort)?;
        Ok(Self(url))
    }
}

/// Open deadlines. Source mode enforces the fixed 15-second header ceiling;
/// transport mode retains a finite request/body deadline of `headers_timeout`.
#[derive(Clone, Copy, Debug)]
pub struct FetchLimits {
    pub connect_timeout: Duration,
    pub headers_timeout: Duration,
    pub source_response: bool,
}

/// Fixed, sanitized diagnostics. No variant retains URL, response, or client error data.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FetchError {
    InvalidSourceConfig,
    InvalidSourceUrl,
    SourceAddressBlocked,
    BadUrl,
    UnsupportedScheme,
    NoHost,
    NoPort,
    DnsTimeout,
    DnsFailed,
    AddressBlocked,
    ClientBuildFailed,
    ConnectTimeout,
    ConnectFailed,
    TlsFailed,
    HeadersTimeout,
    RedirectRejected,
    HttpStatus,
    UnsupportedResponse,
    BodyFailed,
    BodyFrameTooLarge,
    Cancelled,
}

impl FetchError {
    pub const fn code(self) -> &'static str {
        match self {
            Self::InvalidSourceConfig => "invalid_source_config",
            Self::InvalidSourceUrl => "invalid_source_url",
            Self::SourceAddressBlocked => "source_address_blocked",
            Self::BadUrl | Self::UnsupportedScheme | Self::NoHost | Self::NoPort => {
                "invalid_source_url"
            }
            Self::DnsTimeout => "dns_timeout",
            Self::DnsFailed => "dns_failed",
            Self::AddressBlocked => "address_blocked",
            Self::ClientBuildFailed | Self::ConnectFailed => "connect_failed",
            Self::ConnectTimeout => "connect_timeout",
            Self::TlsFailed => "tls_failed",
            Self::HeadersTimeout => "headers_timeout",
            Self::RedirectRejected => "redirect_rejected",
            Self::HttpStatus => "http_status",
            Self::UnsupportedResponse => "unsupported_response",
            Self::BodyFailed => "body_failed",
            Self::BodyFrameTooLarge => "body_frame_too_large",
            Self::Cancelled => "cancelled",
        }
    }
}

impl fmt::Display for FetchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.code())
    }
}

impl Error for FetchError {}

fn normalized_host(url: &reqwest::Url) -> Result<&str, FetchError> {
    url.host_str().map(unbracket_host).ok_or(FetchError::NoHost)
}

fn unbracket_host(host: &str) -> &str {
    host.strip_prefix('[')
        .and_then(|host| host.strip_suffix(']'))
        .unwrap_or(host)
}

// Extracted unchanged from the transport fetcher. This is the established denylist,
// not a claim to classify every IANA special-purpose range.
pub(crate) fn is_safe_ip(ip: &IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => {
            let o = v4.octets();
            !(v4.is_loopback()
                || v4.is_private()
                || v4.is_link_local()
                || v4.is_unspecified()
                || v4.is_broadcast()
                || v4.is_documentation()
                || v4.is_multicast()
                || (o[0] == 100 && (o[1] & 0b1100_0000) == 0b0100_0000))
        }
        IpAddr::V6(v6) => {
            if let Some(v4) = v6.to_ipv4_mapped() {
                return is_safe_ip(&IpAddr::V4(v4));
            }
            let seg = v6.segments();
            !(v6.is_loopback()
                || v6.is_unspecified()
                || v6.is_multicast()
                || (seg[0] & 0xfe00) == 0xfc00
                || (seg[0] & 0xffc0) == 0xfe80)
        }
    }
}

pub(crate) fn is_private_or_loopback(ip: &IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => {
            let o = v4.octets();
            v4.is_loopback()
                || v4.is_private()
                || (o[0] == 100 && (o[1] & 0b1100_0000) == 0b0100_0000)
        }
        IpAddr::V6(v6) => {
            if let Some(v4) = v6.to_ipv4_mapped() {
                return is_private_or_loopback(&IpAddr::V4(v4));
            }
            v6.is_loopback() || (v6.segments()[0] & 0xfe00) == 0xfc00
        }
    }
}

async fn cancelled(cancel: &mut watch::Receiver<bool>) {
    loop {
        if *cancel.borrow() {
            return;
        }
        if cancel.changed().await.is_err() {
            // A closed false channel does not signal cancellation or spin.
            std::future::pending::<()>().await;
        }
    }
}

pub(crate) async fn resolve_addr(
    host: &str,
    port: u16,
    policy: FetchPolicy,
    cancel: watch::Receiver<bool>,
) -> Result<SocketAddr, FetchError> {
    let host = unbracket_host(host);
    if *cancel.borrow() {
        return Err(FetchError::Cancelled);
    }
    if let Ok(ip) = host.parse::<IpAddr>() {
        return policy
            .permits(&ip)
            .then_some(SocketAddr::new(ip, port))
            .ok_or(FetchError::AddressBlocked);
    }
    resolve_candidates(
        async {
            tokio::net::lookup_host((host, port))
                .await
                .map_err(|_| FetchError::DnsFailed)
        },
        policy,
        cancel,
    )
    .await
}

async fn resolve_candidates<F, I>(
    lookup: F,
    policy: FetchPolicy,
    mut cancel: watch::Receiver<bool>,
) -> Result<SocketAddr, FetchError>
where
    F: Future<Output = Result<I, FetchError>>,
    I: Iterator<Item = SocketAddr>,
{
    tokio::select! {
        biased;
        _ = cancelled(&mut cancel) => Err(FetchError::Cancelled),
        result = tokio::time::timeout(DNS_TIMEOUT, lookup) => {
            result.map_err(|_| FetchError::DnsTimeout)??
                .find(|addr| policy.permits(&addr.ip()))
                .ok_or(FetchError::AddressBlocked)
        }
    }
}

// Overrides pin exactly one validated answer. Reject any unexpected resolver invocation
// instead of retaining a system resolver fallback inside reqwest.
struct NoFallbackDns;

impl reqwest::dns::Resolve for NoFallbackDns {
    fn resolve(&self, _name: reqwest::dns::Name) -> reqwest::dns::Resolving {
        Box::pin(async { Err(std::io::Error::other("unpinned DNS request").into()) })
    }
}

/// Open one fresh attempt. DNS, connection/TLS, and response headers are cancelable.
/// The caller selects cancellation around its ingestion work as well.
pub async fn open(
    url: &FetchUrl,
    policy: FetchPolicy,
    limits: FetchLimits,
    cancel: watch::Receiver<bool>,
) -> Result<GuardedBody, FetchError> {
    let host = normalized_host(&url.0)?;
    let port = url.0.port_or_known_default().ok_or(FetchError::NoPort)?;
    let addr = resolve_addr(host, port, policy, cancel.clone()).await?;
    open_pinned(url, addr, limits, cancel, AttemptOptions::default()).await
}

#[derive(Default)]
struct AttemptOptions {
    #[cfg(test)]
    tls_root: Option<Vec<u8>>,
}

async fn open_pinned(
    url: &FetchUrl,
    addr: SocketAddr,
    limits: FetchLimits,
    mut cancel: watch::Receiver<bool>,
    options: AttemptOptions,
) -> Result<GuardedBody, FetchError> {
    if limits.connect_timeout.is_zero() || limits.headers_timeout.is_zero() {
        return Err(FetchError::InvalidSourceConfig);
    }
    let header_deadline = tokio::time::Instant::now()
        + if limits.source_response {
            limits.headers_timeout.min(SOURCE_HEADERS_TIMEOUT)
        } else {
            limits.headers_timeout
        };
    if *cancel.borrow() {
        return Err(FetchError::Cancelled);
    }
    // Preserve the existing provider selection and bundled roots. Never replace an
    // embedding application's already-installed provider.
    let _ = rustls::crypto::ring::default_provider().install_default();
    let roots = webpki_root_certs::TLS_SERVER_ROOT_CERTS.iter().map(|cert| {
        reqwest::Certificate::from_der(cert.as_ref()).expect("bundled TLS root certificate")
    });
    let mut builder = reqwest::Client::builder()
        .tls_certs_only(roots)
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .retry(reqwest::retry::never())
        .referer(false)
        .no_gzip()
        .no_brotli()
        .no_deflate()
        .no_zstd()
        .pool_max_idle_per_host(0)
        .connect_timeout(limits.connect_timeout)
        .dns_resolver(Arc::new(NoFallbackDns))
        .resolve(normalized_host(&url.0)?, addr);
    if limits.source_response {
        // Hyper 1.11.1 also retains its finite default 417,792-byte HTTP/1 parse
        // buffer. The smaller parsed-byte acceptance bound is checked below.
        builder = builder.http1_only().http1_max_headers(MAX_SOURCE_HEADERS);
    } else {
        // As before extraction, the transport request budget starts after DNS and
        // remains active until the finite body has been collected.
        builder = builder.timeout(limits.headers_timeout);
    }
    #[cfg(test)]
    if let Some(root) = options.tls_root {
        builder =
            builder
                .tls_certs_only([reqwest::Certificate::from_der(&root)
                    .map_err(|_| FetchError::ClientBuildFailed)?]);
    }
    #[cfg(not(test))]
    let _ = options;
    let client = builder.build().map_err(|_| FetchError::ClientBuildFailed)?;
    // Transport parsing historically accepts userinfo. Retain that admission surface,
    // but do not let reqwest translate it into URL-derived Authorization on the wire.
    let mut request_url = url.0.clone();
    let _ = request_url.set_username("");
    let _ = request_url.set_password(None);
    let response = tokio::select! {
        biased;
        _ = cancelled(&mut cancel) => return Err(FetchError::Cancelled),
        result = tokio::time::timeout_at(header_deadline, client.get(request_url).send()) => {
            result.map_err(|_| FetchError::HeadersTimeout)?
                .map_err(classify_open_error)?
        }
    };
    validate_response(&response, limits.source_response)?;
    Ok(GuardedBody {
        response: Some(response),
        client: Some(client),
        source_response: limits.source_response,
        cancel,
        #[cfg(test)]
        frames: None,
    })
}

fn classify_open_error(error: reqwest::Error) -> FetchError {
    if error.is_timeout() {
        return if error.is_connect() {
            FetchError::ConnectTimeout
        } else {
            FetchError::HeadersTimeout
        };
    }
    // Inspect typed sources only; never stringify or retain a URL-bearing error chain.
    let mut source = error.source();
    while let Some(cause) = source {
        if cause.is::<rustls::Error>() {
            return FetchError::TlsFailed;
        }
        // tokio-rustls stores rustls::Error in an io::Error; hyper-rustls wraps
        // that I/O error again. io::Error::source() delegates to its payload's
        // source(), so follow get_ref() explicitly to retain the typed payloads.
        let mut nested_io = cause.downcast_ref::<std::io::Error>();
        while let Some(io) = nested_io {
            let Some(inner) = io.get_ref() else {
                break;
            };
            if inner.is::<rustls::Error>() {
                return FetchError::TlsFailed;
            }
            nested_io = inner.downcast_ref::<std::io::Error>();
        }
        if let Some(http) = cause.downcast_ref::<hyper::Error>() {
            if http.is_parse() {
                return FetchError::UnsupportedResponse;
            }
        }
        source = cause.source();
    }
    FetchError::ConnectFailed
}

fn validate_response(response: &reqwest::Response, source: bool) -> Result<(), FetchError> {
    let status = response.status();
    if !source {
        return if status.is_success() {
            Ok(())
        } else {
            Err(FetchError::HttpStatus)
        };
    }
    if status.is_redirection() {
        return Err(FetchError::RedirectRejected);
    }
    if status.as_u16() == 101 || matches!(status.as_u16(), 204..=206) {
        return Err(FetchError::UnsupportedResponse);
    }
    if !status.is_success() {
        return Err(FetchError::HttpStatus);
    }
    let headers = response.headers();
    if headers.contains_key(reqwest::header::UPGRADE)
        || headers.contains_key(reqwest::header::CONTENT_RANGE)
        || headers
            .get_all(reqwest::header::CONNECTION)
            .iter()
            .any(|value| {
                value
                    .as_bytes()
                    .split(|byte| *byte == b',')
                    .any(|token| token.trim_ascii().eq_ignore_ascii_case(b"upgrade"))
            })
        || headers
            .get_all(reqwest::header::CONTENT_ENCODING)
            .iter()
            .any(|value| !value.as_bytes().eq_ignore_ascii_case(b"identity"))
        || headers.len() > MAX_SOURCE_HEADERS
        || headers
            .iter()
            .map(|(name, value)| name.as_str().len() + value.as_bytes().len())
            .sum::<usize>()
            > MAX_SOURCE_HEADER_BYTES
    {
        return Err(FetchError::UnsupportedResponse);
    }
    Ok(())
}

/// One sequential, backpressured response. Dropping it closes the attempt; no pool
/// or client is shared with later opens. There is no source lifetime/byte ceiling.
pub struct GuardedBody {
    response: Option<reqwest::Response>,
    client: Option<reqwest::Client>,
    source_response: bool,
    cancel: watch::Receiver<bool>,
    #[cfg(test)]
    frames: Option<std::collections::VecDeque<Bytes>>,
}

impl fmt::Debug for GuardedBody {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("GuardedBody([redacted])")
    }
}

impl GuardedBody {
    /// Read at most one frame. Cancellation/error/EOF releases the response and client.
    /// The engine must also select cancellation around each caller-side body read.
    pub async fn next_chunk(&mut self) -> Result<Option<Bytes>, FetchError> {
        let mut cancel = self.cancel.clone();
        let result = tokio::select! {
            biased;
            _ = cancelled(&mut cancel) => Err(FetchError::Cancelled),
            result = self.read_frame() => result,
        };
        let result = match result {
            Ok(Some(frame)) if self.source_response && frame.len() > MAX_SOURCE_FRAME => {
                Err(FetchError::BodyFrameTooLarge)
            }
            result => result,
        };
        if !matches!(result, Ok(Some(_))) {
            self.response = None;
            self.client = None;
            #[cfg(test)]
            {
                self.frames = None;
            }
        }
        result
    }

    async fn read_frame(&mut self) -> Result<Option<Bytes>, FetchError> {
        #[cfg(test)]
        if let Some(frames) = &mut self.frames {
            return Ok(frames.pop_front());
        }
        match &mut self.response {
            Some(response) => response.chunk().await.map_err(|_| FetchError::BodyFailed),
            None => Ok(None),
        }
    }
}

#[cfg(test)]
pub(super) mod test_support {
    use super::*;
    use std::collections::VecDeque;
    use std::sync::Mutex;

    #[derive(Clone, Default)]
    pub(crate) struct TestHooks {
        answers: Arc<Mutex<VecDeque<Vec<SocketAddr>>>>,
        pending_dns: bool,
        tls_root: Option<Vec<u8>>,
    }

    impl TestHooks {
        pub(crate) fn new() -> Self {
            Self::default()
        }

        pub(crate) fn with_dns_answers(self, answers: Vec<SocketAddr>) -> Self {
            self.answers.lock().unwrap().push_back(answers);
            self
        }

        pub(crate) fn then_dns_answers(self, answers: Vec<SocketAddr>) -> Self {
            self.with_dns_answers(answers)
        }

        pub(crate) fn with_pending_dns(mut self) -> Self {
            self.pending_dns = true;
            self
        }

        pub(crate) fn with_tls_root(mut self, root: Vec<u8>) -> Self {
            self.tls_root = Some(root);
            self
        }
    }

    pub(crate) async fn open_with(
        url: &FetchUrl,
        policy: FetchPolicy,
        limits: FetchLimits,
        cancel: watch::Receiver<bool>,
        hooks: TestHooks,
    ) -> Result<GuardedBody, FetchError> {
        let lookup = async {
            if hooks.pending_dns {
                std::future::pending::<()>().await;
            }
            let answers = hooks
                .answers
                .lock()
                .unwrap()
                .pop_front()
                .ok_or(FetchError::DnsFailed)?;
            Ok(answers.into_iter())
        };
        let addr = resolve_candidates(lookup, policy, cancel.clone()).await?;
        open_pinned(
            url,
            addr,
            limits,
            cancel,
            AttemptOptions {
                tls_root: hooks.tls_root,
            },
        )
        .await
    }

    pub(super) fn body_with_frames(frames: Vec<Bytes>) -> GuardedBody {
        let (_tx, cancel) = watch::channel(false);
        GuardedBody {
            response: None,
            client: None,
            source_response: true,
            cancel,
            frames: Some(frames.into()),
        }
    }
}

#[cfg(test)]
mod tests;
