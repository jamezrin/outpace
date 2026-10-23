//! Private, process-local HTTP source attachments and their live ingest worker.
//!
//! An attempt owns one response and one ordinary authorized ingest. Dropping either on
//! reconnect discards its tail; only the shared session/cursor survives the attempt.

use crate::broadcast::{cancelled, valid_broadcast_name, ProducerSession};
use crate::broadcast_ingest::IngestError;
#[cfg(test)]
use crate::broadcast_ingest::IngestProgress;
use ace_swarm::http_fetch::{self, FetchError, FetchLimits, FetchPolicy, FetchUrl, GuardedBody};
use serde::{
    de::{MapAccess, Visitor},
    Deserialize, Deserializer, Serialize,
};
use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::watch;
use tokio::time::Instant;

pub const MAX_SOURCE_MAP_BYTES: usize = 256 * 1024;
pub const MAX_SOURCE_ENTRIES: usize = 64;
pub const MAX_SOURCE_REQUEST_BYTES: usize = 16 * 1024;
const PROGRESS_TIMEOUT: Duration = Duration::from_secs(30);
const INGEST_TIMEOUT: Duration = Duration::from_secs(15);
const HEALTHY_WINDOW: Duration = Duration::from_secs(30);
const MAX_FRAME: usize = 256 * 1024;
const FEED_SLICE: usize = 64 * 1024;

fn default_connect_timeout_secs() -> u64 {
    5
}
fn default_read_timeout_secs() -> u64 {
    15
}
fn default_reconnect_initial_secs() -> u64 {
    1
}
fn default_reconnect_max_secs() -> u64 {
    30
}

#[derive(Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct BroadcastSourceConfig {
    pub url: String,
    #[serde(default = "default_connect_timeout_secs")]
    pub connect_timeout_secs: u64,
    #[serde(default = "default_read_timeout_secs")]
    pub read_timeout_secs: u64,
    #[serde(default = "default_reconnect_initial_secs")]
    pub reconnect_initial_secs: u64,
    #[serde(default = "default_reconnect_max_secs")]
    pub reconnect_max_secs: u64,
}
impl fmt::Debug for BroadcastSourceConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BroadcastSourceConfig")
            .field("url", &"[redacted]")
            .field("connect_timeout_secs", &self.connect_timeout_secs)
            .field("read_timeout_secs", &self.read_timeout_secs)
            .field("reconnect_initial_secs", &self.reconnect_initial_secs)
            .field("reconnect_max_secs", &self.reconnect_max_secs)
            .finish()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ValidatedSourceConfig {
    pub(crate) url: FetchUrl,
    pub(crate) connect_timeout: Duration,
    pub(crate) read_timeout: Duration,
    pub(crate) reconnect_initial: Duration,
    pub(crate) reconnect_max: Duration,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SourceConfigError {
    InvalidSourceConfig,
    InvalidSourceUrl,
    SourceAddressBlocked,
}
impl SourceConfigError {
    pub const fn code(self) -> &'static str {
        match self {
            Self::InvalidSourceConfig => "invalid_source_config",
            Self::InvalidSourceUrl => "invalid_source_url",
            Self::SourceAddressBlocked => "source_address_blocked",
        }
    }
}
impl fmt::Display for SourceConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.code())
    }
}
impl std::error::Error for SourceConfigError {}

pub fn parse_non_global_source_opt_in(raw: Option<&str>) -> bool {
    raw == Some("1")
}

pub fn validate_source(
    config: BroadcastSourceConfig,
    policy: FetchPolicy,
) -> Result<ValidatedSourceConfig, SourceConfigError> {
    if !(1..=60).contains(&config.connect_timeout_secs)
        || !(1..=300).contains(&config.read_timeout_secs)
        || !(1..=300).contains(&config.reconnect_initial_secs)
        || !(1..=300).contains(&config.reconnect_max_secs)
        || config.reconnect_initial_secs > config.reconnect_max_secs
    {
        return Err(SourceConfigError::InvalidSourceConfig);
    }
    let url = FetchUrl::parse_source(&config.url, policy).map_err(|error| match error {
        FetchError::SourceAddressBlocked => SourceConfigError::SourceAddressBlocked,
        _ => SourceConfigError::InvalidSourceUrl,
    })?;
    Ok(ValidatedSourceConfig {
        url,
        connect_timeout: Duration::from_secs(config.connect_timeout_secs),
        read_timeout: Duration::from_secs(config.read_timeout_secs),
        reconnect_initial: Duration::from_secs(config.reconnect_initial_secs),
        reconnect_max: Duration::from_secs(config.reconnect_max_secs),
    })
}

// Reject duplicates while parsing, before a BTreeMap can silently overwrite a declaration.
// The derived source struct independently rejects duplicate request/config fields.
pub(crate) fn deserialize_source_map<'de, D>(
    deserializer: D,
) -> Result<BTreeMap<String, BroadcastSourceConfig>, D::Error>
where
    D: Deserializer<'de>,
{
    struct SourceMap;
    impl<'de> Visitor<'de> for SourceMap {
        type Value = BTreeMap<String, BroadcastSourceConfig>;
        fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("source map")
        }
        fn visit_map<M>(self, mut map: M) -> Result<Self::Value, M::Error>
        where
            M: MapAccess<'de>,
        {
            let mut result = BTreeMap::new();
            while let Some(name) = map.next_key::<String>()? {
                if result.len() >= MAX_SOURCE_ENTRIES
                    || !valid_broadcast_name(&name)
                    || result.contains_key(&name)
                {
                    return Err(serde::de::Error::custom("invalid_source_config"));
                }
                result.insert(name, map.next_value()?);
            }
            Ok(result)
        }
    }
    deserializer.deserialize_map(SourceMap)
}

#[derive(Deserialize)]
struct SourceMap(
    #[serde(deserialize_with = "deserialize_source_map")] BTreeMap<String, BroadcastSourceConfig>,
);

pub(crate) fn parse_source_configs(
    raw: Option<&str>,
) -> Result<BTreeMap<String, BroadcastSourceConfig>, SourceConfigError> {
    let raw = raw.unwrap_or("{}");
    if raw.len() > MAX_SOURCE_MAP_BYTES {
        return Err(SourceConfigError::InvalidSourceConfig);
    }
    serde_json::from_str::<SourceMap>(raw)
        .map(|map| map.0)
        .map_err(|_| SourceConfigError::InvalidSourceConfig)
}

pub fn parse_source_map(
    raw: Option<&str>,
    policy: FetchPolicy,
) -> Result<BTreeMap<String, ValidatedSourceConfig>, SourceConfigError> {
    validate_source_map(&parse_source_configs(raw)?, policy)
}

pub(crate) fn validate_source_map(
    configs: &BTreeMap<String, BroadcastSourceConfig>,
    policy: FetchPolicy,
) -> Result<BTreeMap<String, ValidatedSourceConfig>, SourceConfigError> {
    if configs.len() > MAX_SOURCE_ENTRIES {
        return Err(SourceConfigError::InvalidSourceConfig);
    }
    let validated = configs
        .iter()
        .map(|(name, config)| {
            if !valid_broadcast_name(name) {
                return Err(SourceConfigError::InvalidSourceConfig);
            }
            if config.url.len() > 8192 {
                return Err(SourceConfigError::InvalidSourceUrl);
            }
            Ok((name.clone(), validate_source(config.clone(), policy)?))
        })
        .collect::<Result<BTreeMap<_, _>, _>>()?;
    // A typed map has no original whitespace/omitted-field representation. Count its
    // compact representation with defaults omitted, so every admitted raw map remains
    // valid after default filling, and typed maps that cannot fit the input limit fail.
    struct AdmissionSize<'a>(&'a BroadcastSourceConfig);
    impl Serialize for AdmissionSize<'_> {
        fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
            use serde::ser::SerializeMap;
            let mut map = serializer.serialize_map(None)?;
            map.serialize_entry("url", &self.0.url)?;
            for (key, value, default) in [
                ("connect_timeout_secs", self.0.connect_timeout_secs, 5),
                ("read_timeout_secs", self.0.read_timeout_secs, 15),
                ("reconnect_initial_secs", self.0.reconnect_initial_secs, 1),
                ("reconnect_max_secs", self.0.reconnect_max_secs, 30),
            ] {
                if value != default {
                    map.serialize_entry(key, &value)?;
                }
            }
            map.end()
        }
    }
    struct BoundedCount(usize);
    impl std::io::Write for BoundedCount {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if bytes.len() > MAX_SOURCE_MAP_BYTES.saturating_sub(self.0) {
                return Err(std::io::Error::other("invalid_source_config"));
            }
            self.0 += bytes.len();
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let sizes: BTreeMap<_, _> = configs
        .iter()
        .map(|(name, config)| (name, AdmissionSize(config)))
        .collect();
    serde_json::to_writer(BoundedCount(0), &sizes)
        .map_err(|_| SourceConfigError::InvalidSourceConfig)?;
    Ok(validated)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SourceState {
    Connecting,
    Streaming,
    Backoff,
    Blocked,
    Failed,
    Stopping,
}
impl SourceState {
    pub const fn code(self) -> &'static str {
        match self {
            Self::Connecting => "connecting",
            Self::Streaming => "streaming",
            Self::Backoff => "backoff",
            Self::Blocked => "blocked",
            Self::Failed => "failed",
            Self::Stopping => "stopping",
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceErrorCode {
    DnsTimeout,
    DnsFailed,
    AddressBlocked,
    ConnectTimeout,
    ConnectFailed,
    TlsFailed,
    HeadersTimeout,
    RedirectRejected,
    HttpStatus,
    UnsupportedResponse,
    BodyFailed,
    BodyFrameTooLarge,
    ReadTimeout,
    ProgressTimeout,
    UpstreamEof,
    PersistenceFailed,
    PieceIndexExhausted,
    IngestFailed,
    WorkerFailed,
}
impl SourceErrorCode {
    pub const fn code(self) -> &'static str {
        match self {
            Self::DnsTimeout => "dns_timeout",
            Self::DnsFailed => "dns_failed",
            Self::AddressBlocked => "address_blocked",
            Self::ConnectTimeout => "connect_timeout",
            Self::ConnectFailed => "connect_failed",
            Self::TlsFailed => "tls_failed",
            Self::HeadersTimeout => "headers_timeout",
            Self::RedirectRejected => "redirect_rejected",
            Self::HttpStatus => "http_status",
            Self::UnsupportedResponse => "unsupported_response",
            Self::BodyFailed => "body_failed",
            Self::BodyFrameTooLarge => "body_frame_too_large",
            Self::ReadTimeout => "read_timeout",
            Self::ProgressTimeout => "progress_timeout",
            Self::UpstreamEof => "upstream_eof",
            Self::PersistenceFailed => "persistence_failed",
            Self::PieceIndexExhausted => "piece_index_exhausted",
            Self::IngestFailed => "ingest_failed",
            Self::WorkerFailed => "worker_failed",
        }
    }
    fn terminal(self) -> bool {
        matches!(
            self,
            Self::PersistenceFailed
                | Self::PieceIndexExhausted
                | Self::IngestFailed
                | Self::WorkerFailed
        )
    }
}
impl From<FetchError> for SourceErrorCode {
    fn from(error: FetchError) -> Self {
        match error {
            FetchError::DnsTimeout => Self::DnsTimeout,
            FetchError::DnsFailed => Self::DnsFailed,
            FetchError::AddressBlocked | FetchError::SourceAddressBlocked => Self::AddressBlocked,
            FetchError::ConnectTimeout => Self::ConnectTimeout,
            FetchError::ConnectFailed | FetchError::ClientBuildFailed => Self::ConnectFailed,
            FetchError::TlsFailed => Self::TlsFailed,
            FetchError::HeadersTimeout => Self::HeadersTimeout,
            FetchError::RedirectRejected => Self::RedirectRejected,
            FetchError::HttpStatus => Self::HttpStatus,
            FetchError::UnsupportedResponse => Self::UnsupportedResponse,
            FetchError::BodyFailed => Self::BodyFailed,
            FetchError::BodyFrameTooLarge => Self::BodyFrameTooLarge,
            _ => Self::WorkerFailed,
        }
    }
}
impl From<IngestError> for SourceErrorCode {
    fn from(error: IngestError) -> Self {
        match error {
            IngestError::PersistenceFailed => Self::PersistenceFailed,
            IngestError::PieceIndexExhausted => Self::PieceIndexExhausted,
            _ => Self::IngestFailed,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceStatus {
    pub state: SourceState,
    pub attempts: u64,
    pub last_error: Option<SourceErrorCode>,
    pub retry_in_ms: Option<u64>,
}
struct StatusSnapshot {
    status: SourceStatus,
    retry_at: Option<Instant>,
    last_log: Option<Instant>,
    terminal_logged: bool,
}

/// A worker holds only this private status cell, never the entry/registry/controller.
#[derive(Clone)]
pub struct SourceStatusWriter {
    cell: Arc<Mutex<StatusSnapshot>>,
    name: Arc<str>,
    #[cfg(test)]
    pub(crate) hooks: SourceTestHooks,
}
impl SourceStatusWriter {
    pub(crate) fn new(name: &str) -> Self {
        Self {
            cell: Arc::new(Mutex::new(StatusSnapshot {
                status: SourceStatus {
                    state: SourceState::Connecting,
                    attempts: 0,
                    last_error: None,
                    retry_in_ms: None,
                },
                retry_at: None,
                last_log: None,
                terminal_logged: false,
            })),
            name: name.into(),
            #[cfg(test)]
            hooks: SourceTestHooks::default(),
        }
    }
    fn now(&self) -> Instant {
        #[cfg(test)]
        if let Some(now) = &self.hooks.now {
            return now();
        }
        Instant::now()
    }
    pub(crate) fn snapshot(&self) -> SourceStatus {
        let cell = self.cell.lock().unwrap();
        let mut status = cell.status.clone();
        status.retry_in_ms = cell.retry_at.map(|at| {
            let left = at.saturating_duration_since(self.now());
            u64::try_from(left.as_millis()).unwrap_or(u64::MAX)
        });
        status
    }
    #[cfg(test)]
    fn notify(&self) {
        if let Some(observer) = &self.hooks.on_status {
            observer(self.snapshot());
        }
    }
    pub(crate) fn stopping(&self) {
        let mut cell = self.cell.lock().unwrap();
        cell.status.state = SourceState::Stopping;
        cell.retry_at = None;
        drop(cell);
        #[cfg(test)]
        self.notify();
    }
    fn attempt(&self) {
        let mut cell = self.cell.lock().unwrap();
        if cell.status.state == SourceState::Stopping {
            return;
        }
        cell.status.state = SourceState::Connecting;
        cell.status.attempts = cell.status.attempts.saturating_add(1);
        cell.retry_at = None;
        drop(cell);
        #[cfg(test)]
        self.notify();
    }
    fn streaming(&self) {
        let mut cell = self.cell.lock().unwrap();
        if cell.status.state == SourceState::Stopping {
            return;
        }
        cell.status.state = SourceState::Streaming;
        cell.status.last_error = None;
        cell.retry_at = None;
        drop(cell);
        #[cfg(test)]
        self.notify();
    }
    fn failure(&self, error: SourceErrorCode, retry_at: Option<Instant>) {
        let now = self.now();
        let mut cell = self.cell.lock().unwrap();
        if cell.status.state == SourceState::Stopping {
            return;
        }
        cell.status.state = if error.terminal() {
            SourceState::Failed
        } else if error == SourceErrorCode::AddressBlocked {
            SourceState::Blocked
        } else {
            SourceState::Backoff
        };
        cell.status.last_error = Some(error);
        cell.retry_at = retry_at;
        let log = if error.terminal() {
            let first = !cell.terminal_logged;
            cell.terminal_logged = true;
            first
        } else {
            cell.last_log
                .is_none_or(|last| now.saturating_duration_since(last) >= Duration::from_secs(30))
        };
        if log {
            cell.last_log = Some(now);
            crate::alog!(
                "[broadcast] {}: {} {}",
                self.name,
                cell.status.state.code(),
                error.code()
            );
        }
        drop(cell);
        #[cfg(test)]
        {
            self.notify();
            if log {
                if let Some(observer) = &self.hooks.on_log {
                    observer(self.snapshot());
                }
            }
        }
    }
}

/// Test seams stay local to a registry/attachment and run the real worker and real ingest.
/// `now` uses the same monotonic domain as Tokio's paused clock/sleep_until.
/// Synchronous observers only notify tests; they must not block or reenter the registry.
#[cfg(test)]
#[derive(Clone, Default)]
pub(crate) struct SourceTestHooks {
    pub now: Option<Arc<dyn Fn() -> Instant + Send + Sync>>,
    pub jitter: Option<Arc<dyn Fn(u64, u64) -> u64 + Send + Sync>>,
    pub open: Option<TestSourceOpen>,
    pub before_ingest: Option<TestIngestGate>,
    /// Owned admission initializer, immediately before its first source-gate lock poll.
    pub before_source_gate: Option<TestCommitGate>,
    pub after_commit: Option<TestCommitGate>,
    pub on_status: Option<Arc<dyn Fn(SourceStatus) + Send + Sync>>,
    pub on_log: Option<Arc<dyn Fn(SourceStatus) + Send + Sync>>,
    pub on_ingest: Option<Arc<dyn Fn(IngestProgress) + Send + Sync>>,
}
#[cfg(test)]
pub(crate) type TestSourceOpen = Arc<
    dyn Fn(
            FetchUrl,
            FetchPolicy,
            FetchLimits,
            watch::Receiver<bool>,
        )
            -> futures::future::BoxFuture<'static, Result<Box<dyn TestSourceBody>, FetchError>>
        + Send
        + Sync,
>;
#[cfg(test)]
pub(crate) type TestIngestGate = Arc<
    dyn Fn(usize) -> futures::future::BoxFuture<'static, Result<(), IngestError>> + Send + Sync,
>;
#[cfg(test)]
pub(crate) type TestCommitGate =
    Arc<dyn Fn() -> futures::future::BoxFuture<'static, ()> + Send + Sync>;
#[cfg(test)]
#[async_trait::async_trait]
pub(crate) trait TestSourceBody: Send {
    async fn next_chunk(&mut self) -> Result<Option<bytes::Bytes>, FetchError>;
}
enum AttemptBody {
    Network(GuardedBody),
    #[cfg(test)]
    Test(Box<dyn TestSourceBody>),
}
impl AttemptBody {
    async fn next_chunk(&mut self) -> Result<Option<bytes::Bytes>, FetchError> {
        match self {
            Self::Network(body) => body.next_chunk().await,
            #[cfg(test)]
            Self::Test(body) => body.next_chunk().await,
        }
    }
}

fn retry_base(config: &ValidatedSourceConfig, failures: u32) -> u64 {
    let initial = config.reconnect_initial.as_millis() as u64;
    let max = config.reconnect_max.as_millis() as u64;
    initial
        .saturating_mul(1u64.checked_shl(failures).unwrap_or(u64::MAX))
        .min(max)
}
fn retry_delay(
    config: &ValidatedSourceConfig,
    failures: u32,
    status: &SourceStatusWriter,
) -> Duration {
    let base = retry_base(config, failures);
    let low = base - base / 10; // ceil(0.9 * base), without multiplication overflow
    #[cfg(test)]
    if let Some(jitter) = &status.hooks.jitter {
        return Duration::from_millis(jitter(low, base).clamp(low, base));
    }
    let _ = status;
    use rand::RngExt;
    Duration::from_millis(rand::rng().random_range(low..=base))
}

// A panic/abort cannot turn an attached source into an invisible/free producer. The entry
// retains the attachment, and exposes a fixed terminal code until explicit detach.
struct WorkerExit {
    status: SourceStatusWriter,
    cancel: watch::Receiver<bool>,
}
impl Drop for WorkerExit {
    fn drop(&mut self) {
        if *self.cancel.borrow() {
            self.status.stopping();
        } else if self.status.snapshot().state != SourceState::Failed {
            self.status.failure(SourceErrorCode::WorkerFailed, None);
        }
    }
}

pub async fn run_source(
    mut session: ProducerSession,
    config: ValidatedSourceConfig,
    policy: FetchPolicy,
    status: SourceStatusWriter,
) {
    let mut cancel = session.cancellation();
    let _exit = WorkerExit {
        status: status.clone(),
        cancel: cancel.clone(),
    };
    let mut failures = 0u32;
    loop {
        if *cancel.borrow() {
            return;
        }
        status.attempt();
        let (outcome, healthy) =
            source_attempt(&mut session, &config, policy, &status, &mut cancel).await;
        if *cancel.borrow() {
            return;
        }
        let Some(error) = outcome else {
            return;
        };
        if error.terminal() {
            status.failure(error, None);
            // Even terminal errors retain the noncloneable session/claim until detach.
            cancelled(&mut cancel).await;
            return;
        }
        if healthy {
            failures = 0;
        }
        let delay = retry_delay(&config, failures, &status);
        failures = failures.saturating_add(1);
        let until = status.now() + delay;
        status.failure(error, Some(until));
        tokio::select! { biased; () = cancelled(&mut cancel) => return, () = tokio::time::sleep_until(until) => {} }
    }
}

async fn source_attempt(
    session: &mut ProducerSession,
    config: &ValidatedSourceConfig,
    policy: FetchPolicy,
    status: &SourceStatusWriter,
    cancel: &mut watch::Receiver<bool>,
) -> (Option<SourceErrorCode>, bool) {
    let limits = FetchLimits {
        connect_timeout: config.connect_timeout,
        headers_timeout: Duration::from_secs(15),
        source_response: true,
    };
    let open_cancel = cancel.clone();
    let open = async {
        #[cfg(test)]
        if let Some(open) = &status.hooks.open {
            return open(config.url.clone(), policy, limits, open_cancel.clone())
                .await
                .map(AttemptBody::Test);
        }
        http_fetch::open(&config.url, policy, limits, open_cancel)
            .await
            .map(AttemptBody::Network)
    };
    let opened = tokio::select! { biased; () = cancelled(cancel) => return (None, false), result = open => result };
    let mut body = match opened {
        Ok(body) => body,
        Err(FetchError::Cancelled) => return (None, false),
        Err(error) => return (Some(error.into()), false),
    };
    let headers_at = status.now();
    let mut ingest = match session.open_ingest() {
        Ok(ingest) => ingest,
        Err(error) => return (Some(error.into()), false),
    };
    let mut idle_at = headers_at + config.read_timeout;
    let mut progress_at = headers_at + PROGRESS_TIMEOUT;
    let mut streaming_since: Option<Instant> = None;
    let mut committed = false;
    let mut healthy = false;
    let result = async {
        loop {
            let frame = tokio::select! {
                biased;
                () = cancelled(cancel) => return None,
                () = tokio::time::sleep_until(progress_at) => return Some(SourceErrorCode::ProgressTimeout),
                () = tokio::time::sleep_until(idle_at) => return Some(SourceErrorCode::ReadTimeout),
                () = async {
                    if committed && !healthy {
                        if let Some(since) = streaming_since {
                            tokio::time::sleep_until(since + HEALTHY_WINDOW).await;
                            return;
                        }
                    }
                    std::future::pending::<()>().await;
                } => { healthy = true; continue; },
                result = body.next_chunk() => result,
            };
            let frame = match frame {
                Ok(Some(frame)) => frame, Ok(None) => return Some(SourceErrorCode::UpstreamEof),
                Err(FetchError::Cancelled) => return None, Err(error) => return Some(error.into()),
            };
            if frame.len() > MAX_FRAME { return Some(SourceErrorCode::BodyFrameTooLarge); }
            if frame.is_empty() { tokio::task::yield_now().await; continue; }
            for slice in frame.chunks(FEED_SLICE) {
                // Progress deadline also runs across slice feeds; local stalls use their
                // own terminal deadline rather than reconnecting into a stuck store.
                let push = async {
                    #[cfg(test)]
                    if let Some(gate) = &status.hooks.before_ingest { gate(slice.len()).await?; }
                    ingest.push_bytes(slice).await
                };
                let progress = tokio::select! {
                    biased;
                    () = cancelled(cancel) => return None,
                    result = tokio::time::timeout(INGEST_TIMEOUT, push) => result,
                };
                let progress = match progress {
                    Ok(Ok(progress)) => progress, Ok(Err(error)) => return Some(error.into()),
                    Err(_) => return Some(if session.check() == Err(crate::broadcast::BroadcastError::PersistenceFailed) {
                        SourceErrorCode::PersistenceFailed
                    } else { SourceErrorCode::IngestFailed }),
                };
                #[cfg(test)]
                if let Some(observer) = &status.hooks.on_ingest { observer(progress); }
                if progress.aligned_bytes > 0 {
                    let now = status.now();
                    progress_at = now + PROGRESS_TIMEOUT;
                    streaming_since.get_or_insert(now);
                    status.streaming();
                }
                committed |= progress.committed_pieces > 0;
            }
            // Time spent in local ingestion isn't upstream idle time.
            idle_at = status.now() + config.read_timeout;
        }
    }.await;
    // Deliberately no finish: all response/resync/signing tails die here.
    (result, healthy)
}
