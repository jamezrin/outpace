//! Peer drivers: attach to and sample engine/outpace consumers and sources.
//!
//! The JSON/URL parsing is factored into pure functions ([`parse_engine_getstream`],
//! [`parse_engine_stat`], [`parse_outpace_status`], [`encode_transport_url`]) that are
//! unit-tested against the exact response shapes the real engine and outpace emit. The
//! async methods that actually speak HTTP are thin wrappers over them and are exercised
//! only by a live docker run.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use base64ct::{Base64UrlUnpadded, Encoding};
use futures_util::StreamExt;

use crate::assertions::PeerStats;

/// Prefix marking a base64url-encoded transport-file URL id (mirrors ace-engine's
/// `transport_url` module so an outpace consumer resolves the same descriptor an
/// engine consumer attaches to).
const TURL_PREFIX: &str = "turl-";

/// Encode a descriptor URL into outpace's `turl-<base64url(url)>` stream id.
///
/// Identical scheme to `ace_engine::transport_url::encode_transport_url`: base64url
/// (unpadded) of the raw URL bytes behind a `turl-` prefix, yielding a single
/// URL-path-safe segment usable as `/streams/ace/<id>.ts`.
pub fn encode_transport_url(url: &str) -> Result<String> {
    let url = url.trim();
    if !(url.starts_with("http://") || url.starts_with("https://")) {
        bail!("transport url must be http or https, got {url:?}");
    }
    Ok(format!(
        "{TURL_PREFIX}{}",
        Base64UrlUnpadded::encode_string(url.as_bytes())
    ))
}

/// The control URLs an engine `/ace/getstream?...&format=json` hands back.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EngineAttach {
    /// URL that yields the playback MPEG-TS body.
    pub playback_url: String,
    /// URL that yields the periodic stat JSON.
    pub stat_url: String,
    /// URL that accepts control commands (e.g. `?method=stop`).
    pub command_url: String,
}

/// Parse an engine `/ace/getstream?format=json` envelope into its control URLs.
///
/// The engine wraps results as `{ "response": { ... }, "error": null }`; a non-null
/// `error` (or a missing `response`) is surfaced as an error.
pub fn parse_engine_getstream(v: &serde_json::Value) -> Result<EngineAttach> {
    if let Some(err) = v.get("error").filter(|e| !e.is_null()) {
        bail!("engine getstream error: {err}");
    }
    let resp = v
        .get("response")
        .filter(|r| !r.is_null())
        .ok_or_else(|| anyhow!("engine getstream missing response object"))?;
    let field = |name: &str| -> Result<String> {
        resp.get(name)
            .and_then(|x| x.as_str())
            .map(str::to_string)
            .ok_or_else(|| anyhow!("engine getstream response missing {name}"))
    };
    Ok(EngineAttach {
        playback_url: field("playback_url")?,
        stat_url: field("stat_url")?,
        command_url: field("command_url")?,
    })
}

/// Parse an engine `/ace/stat` envelope into normalized [`PeerStats`], keeping the raw
/// JSON for human audit. Missing numeric fields default to 0 and a missing status to
/// `"unknown"` (so an odd real-engine payload degrades rather than panicking), but each
/// absent field is also recorded in `missing_fields` so a field-name mismatch is visible
/// rather than silently read as 0.
pub fn parse_engine_stat(v: &serde_json::Value) -> PeerStats {
    let resp = v.get("response").filter(|r| !r.is_null()).unwrap_or(v);
    let num = |name: &str| {
        resp.get(name)
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0)
    };
    let missing = absent_fields(resp, &["status", "peers", "downloaded", "uploaded"]);
    PeerStats {
        status: resp
            .get("status")
            .and_then(|x| x.as_str())
            .unwrap_or("unknown")
            .to_string(),
        peers: num("peers"),
        downloaded: num("downloaded"),
        uploaded: num("uploaded"),
        missing_fields: missing,
        raw: v.clone(),
    }
}

/// Names in `expected` that have no key in the `obj` JSON object (order-preserving).
fn absent_fields(obj: &serde_json::Value, expected: &[&str]) -> Vec<String> {
    expected
        .iter()
        .filter(|name| obj.get(*name).is_none())
        .map(|name| name.to_string())
        .collect()
}

/// Parse an outpace `/streams/<net>/<id>/status` body into normalized [`PeerStats`].
///
/// Outpace status has no cumulative `downloaded` field, so the caller supplies the
/// playback byte counter as the download proxy. `status` is synthesized: `"dl"` when
/// the node is actively moving data (a peer is seen or bytes are flowing), else
/// `"idle"`, so the shared swarm-health verdict applies uniformly.
pub fn parse_outpace_status(v: &serde_json::Value, downloaded_bytes: u64) -> PeerStats {
    let num = |name: &str| v.get(name).and_then(serde_json::Value::as_u64).unwrap_or(0);
    let peers = num("peers");
    let bitrate = num("bitrate");
    let active = peers >= 1 || bitrate > 0 || downloaded_bytes > 0;
    // `downloaded` is a caller-supplied proxy, not a JSON field, so it is never "missing".
    let missing = absent_fields(v, &["peers", "uploaded"]);
    PeerStats {
        status: if active { "dl" } else { "idle" }.to_string(),
        peers,
        downloaded: downloaded_bytes,
        uploaded: num("uploaded"),
        missing_fields: missing,
        raw: v.clone(),
    }
}

/// Result of pulling a consumer's playback body for a bounded window.
#[derive(Debug, Default, Clone)]
pub struct PlaybackPull {
    /// Total payload bytes read.
    pub total_bytes: u64,
    /// The first bytes read, capped at the requested head size (for TS-contiguity checks).
    pub head: Vec<u8>,
    /// Per-second byte buckets across the pull window (for stability scoring).
    pub per_second: Vec<u64>,
    /// Number of times the playback connection was (re)established.
    pub connects: u32,
    /// The last transport/stream error observed, if any (for diagnostics; not fatal).
    pub last_error: Option<String>,
}

/// Stream `url` for `duration`, counting bytes, capturing up to `head_cap` head bytes,
/// and bucketing throughput per wall-clock second. The running total is mirrored into
/// `progress` so a concurrent stats poller can read it as the download proxy.
///
/// Docker-glue: not unit-tested (needs a live HTTP source). The bucketing/accounting is
/// intentionally trivial so the tested [`crate::assertions`] functions do the judging.
///
/// Resilient by design: a live playback stream naturally hiccups (the engine reaps or
/// resets a session, prebuffering drops the connection, the swarm is not yet delivering).
/// Rather than fail the scenario on the first such error, this keeps (re)connecting to
/// `url` until `duration` elapses, accumulating whatever bytes flow. Continuously holding
/// a connection is also what keeps the engine's playback session alive. Per-second buckets
/// are indexed by absolute elapsed second so a caller can discard a leading warmup span.
pub async fn pull_playback(
    client: &reqwest::Client,
    url: &str,
    duration: Duration,
    head_cap: usize,
    progress: Arc<AtomicU64>,
) -> Result<PlaybackPull> {
    const RECONNECT_BACKOFF: Duration = Duration::from_secs(1);
    let start = Instant::now();
    // Zero-fill one bucket per second of the whole window up front. Buckets are filled only
    // when a chunk arrives, so without this a stream that dies mid-window would simply have
    // no buckets for its dead tail — and the stall check (which scores `expected = bps ×
    // buckets.len()`) would shrink its own bar and never see the outage. Pre-sizing makes
    // those dead seconds real zeros. The dynamic resize below stays as a safeguard.
    let mut out = PlaybackPull {
        per_second: vec![0; duration.as_secs() as usize],
        ..Default::default()
    };

    while start.elapsed() < duration {
        let resp = match client
            .get(url)
            .send()
            .await
            .and_then(|r| r.error_for_status())
        {
            Ok(resp) => resp,
            Err(e) => {
                out.last_error = Some(format!("connect: {e}"));
                sleep_remaining(start, duration, RECONNECT_BACKOFF).await;
                continue;
            }
        };
        out.connects += 1;
        let mut stream = resp.bytes_stream();
        loop {
            let elapsed = start.elapsed();
            if elapsed >= duration {
                return Ok(out);
            }
            let remaining = duration - elapsed;
            match tokio::time::timeout(remaining, stream.next()).await {
                Ok(Some(Ok(chunk))) => {
                    let sec = start.elapsed().as_secs() as usize;
                    if out.per_second.len() <= sec {
                        out.per_second.resize(sec + 1, 0);
                    }
                    out.per_second[sec] += chunk.len() as u64;
                    out.total_bytes += chunk.len() as u64;
                    progress.store(out.total_bytes, Ordering::Relaxed);
                    if out.head.len() < head_cap {
                        let take = (head_cap - out.head.len()).min(chunk.len());
                        out.head.extend_from_slice(&chunk[..take]);
                    }
                }
                Ok(Some(Err(e))) => {
                    out.last_error = Some(format!("stream: {e}"));
                    break; // reconnect
                }
                Ok(None) => break,        // server closed the body; reconnect
                Err(_) => return Ok(out), // window elapsed mid-wait
            }
        }
        sleep_remaining(start, duration, RECONNECT_BACKOFF).await;
    }
    Ok(out)
}

/// Sleep `backoff`, but never past the pull deadline `start + duration`.
async fn sleep_remaining(start: Instant, duration: Duration, backoff: Duration) {
    let elapsed = start.elapsed();
    if elapsed >= duration {
        return;
    }
    tokio::time::sleep(backoff.min(duration - elapsed)).await;
}

/// Poll `url` until `predicate` accepts a 2xx JSON body or `timeout` elapses.
///
/// Used as a readiness gate for both engine and outpace HTTP surfaces. Returns the
/// accepted body, or an error describing the last failure.
pub async fn wait_for_json<F>(
    client: &reqwest::Client,
    url: &str,
    timeout: Duration,
    mut predicate: F,
) -> Result<serde_json::Value>
where
    F: FnMut(&serde_json::Value) -> bool,
{
    let start = Instant::now();
    let mut last = String::from("no attempt made");
    while start.elapsed() < timeout {
        match client.get(url).send().await {
            Ok(r) if r.status().is_success() => match r.json::<serde_json::Value>().await {
                Ok(v) if predicate(&v) => return Ok(v),
                Ok(_) => last = "predicate rejected body".into(),
                Err(e) => last = format!("bad json: {e}"),
            },
            Ok(r) => last = format!("status {}", r.status()),
            Err(e) => last = format!("request error: {e}"),
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    bail!("timed out waiting for {url} ({last})")
}

/// Poll a plain-text `url` (e.g. outpace `/healthz`) until it returns 2xx or times out.
pub async fn wait_for_ok(client: &reqwest::Client, url: &str, timeout: Duration) -> Result<()> {
    let start = Instant::now();
    let mut last = String::from("no attempt made");
    while start.elapsed() < timeout {
        match client.get(url).send().await {
            Ok(r) if r.status().is_success() => return Ok(()),
            Ok(r) => last = format!("status {}", r.status()),
            Err(e) => last = format!("request error: {e}"),
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    bail!("timed out waiting for {url} ({last})")
}

/// Driver for one real-engine consumer (HTTP API on `:6878`).
pub struct EngineConsumer {
    /// Human-readable service name (e.g. `engine-consumer-1`).
    pub name: String,
    /// Base URL, e.g. `http://172.28.0.21:6878`.
    pub base: String,
    /// Control URLs, populated once [`Self::attach`] succeeds.
    pub attach: Option<EngineAttach>,
    client: reqwest::Client,
}

impl EngineConsumer {
    /// Create a driver for the engine reachable at `base` (no `/` suffix).
    pub fn new(name: impl Into<String>, base: impl Into<String>, client: reqwest::Client) -> Self {
        Self {
            name: name.into(),
            base: base.into(),
            attach: None,
            client,
        }
    }

    /// Attach to `descriptor_url` via `/ace/getstream?...&format=json`, recording the
    /// returned control URLs. Idempotent per instance.
    pub async fn attach(&mut self, descriptor_url: &str) -> Result<()> {
        let url = format!(
            "{}/ace/getstream?url={}&format=json",
            self.base,
            urlencode(descriptor_url)
        );
        let v: serde_json::Value = self
            .client
            .get(&url)
            .send()
            .await
            .with_context(|| format!("GET {url}"))?
            .json()
            .await
            .context("parsing getstream json")?;
        self.attach = Some(parse_engine_getstream(&v)?);
        Ok(())
    }

    /// Fetch the current stat sample from the attached stat URL.
    pub async fn poll_stats(&self) -> Result<PeerStats> {
        let attach = self
            .attach
            .as_ref()
            .ok_or_else(|| anyhow!("{} not attached", self.name))?;
        let v: serde_json::Value = self
            .client
            .get(&attach.stat_url)
            .send()
            .await
            .context("GET stat_url")?
            .json()
            .await
            .context("parsing stat json")?;
        Ok(parse_engine_stat(&v))
    }

    /// The URL whose body is this consumer's playback MPEG-TS (once attached).
    pub fn playback_url(&self) -> Option<&str> {
        self.attach.as_ref().map(|a| a.playback_url.as_str())
    }
}

/// Driver for one outpace consumer (`serve`, HTTP API on `:6878`).
pub struct OutpaceConsumer {
    /// Human-readable service name (e.g. `outpace-consumer-1`).
    pub name: String,
    /// Base URL, e.g. `http://172.28.0.31:6878`.
    pub base: String,
    /// Network segment of the stream route (always `ace` for these swarms).
    pub network: String,
    /// The `turl-...` id derived from the descriptor URL.
    pub id: String,
    client: reqwest::Client,
}

impl OutpaceConsumer {
    /// Create a driver that will attach to `descriptor_url` on `network` (`ace`).
    pub fn new(
        name: impl Into<String>,
        base: impl Into<String>,
        network: impl Into<String>,
        descriptor_url: &str,
        client: reqwest::Client,
    ) -> Result<Self> {
        Ok(Self {
            name: name.into(),
            base: base.into(),
            network: network.into(),
            id: encode_transport_url(descriptor_url)?,
            client,
        })
    }

    /// The `.ts` playback URL that starts/continues this consumer's stream session.
    pub fn playback_url(&self) -> String {
        format!("{}/streams/{}/{}.ts", self.base, self.network, self.id)
    }

    /// The `/status` URL for this consumer's stream session.
    pub fn status_url(&self) -> String {
        format!("{}/streams/{}/{}/status", self.base, self.network, self.id)
    }

    /// Fetch a normalized stat sample, using `downloaded_bytes` (the running playback
    /// byte counter) as the download proxy. A 404 (session not yet active) is reported
    /// as an idle sample rather than an error.
    pub async fn poll_stats(&self, downloaded_bytes: u64) -> Result<PeerStats> {
        let resp = self
            .client
            .get(self.status_url())
            .send()
            .await
            .context("GET outpace status")?;
        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(PeerStats {
                status: "idle".into(),
                downloaded: downloaded_bytes,
                raw: serde_json::json!({ "http_status": 404 }),
                ..Default::default()
            });
        }
        let v: serde_json::Value = resp
            .error_for_status()
            .context("outpace status status-code")?
            .json()
            .await
            .context("parsing outpace status json")?;
        Ok(parse_outpace_status(&v, downloaded_bytes))
    }
}

/// Percent-encode a URL so it can ride inside another URL's `?url=` query parameter.
fn urlencode(s: &str) -> String {
    let mut out = String::with_capacity(s.len() * 3);
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn turl_encoding_matches_ace_engine_scheme() {
        // Same vector shape ace_engine::transport_url round-trips.
        let url = "http://172.28.0.1:7002/baseline.acelive";
        let id = encode_transport_url(url).unwrap();
        assert!(id.starts_with("turl-"));
        assert!(!id.contains('/') && !id.contains('?'));
        // base64url-unpadded decode returns the original bytes.
        let decoded = Base64UrlUnpadded::decode_vec(id.strip_prefix("turl-").unwrap()).unwrap();
        assert_eq!(String::from_utf8(decoded).unwrap(), url);
    }

    #[test]
    fn turl_rejects_non_http() {
        assert!(encode_transport_url("ftp://h/x").is_err());
    }

    #[test]
    fn parses_engine_getstream_urls() {
        let v = json!({
            "response": {
                "infohash": "abcd",
                "playback_url": "http://e/ace/r/abcd/tok",
                "stat_url": "http://e/ace/stat/abcd/tok",
                "command_url": "http://e/ace/cmd/abcd/tok",
                "is_live": 1
            },
            "error": null
        });
        let a = parse_engine_getstream(&v).unwrap();
        assert_eq!(a.playback_url, "http://e/ace/r/abcd/tok");
        assert_eq!(a.stat_url, "http://e/ace/stat/abcd/tok");
        assert_eq!(a.command_url, "http://e/ace/cmd/abcd/tok");
    }

    #[test]
    fn engine_getstream_error_is_reported() {
        let v = json!({ "response": null, "error": "no such content" });
        assert!(parse_engine_getstream(&v).is_err());
    }

    #[test]
    fn parses_engine_stat_envelope() {
        let v = json!({
            "response": {
                "status": "dl",
                "peers": 3,
                "downloaded": 123456,
                "uploaded": 789,
                "is_live": 1
            },
            "error": null
        });
        let s = parse_engine_stat(&v);
        assert_eq!(s.status, "dl");
        assert_eq!(s.peers, 3);
        assert_eq!(s.downloaded, 123456);
        assert_eq!(s.uploaded, 789);
        assert!(!s.raw.is_null(), "raw json retained for audit");
    }

    #[test]
    fn engine_stat_missing_fields_degrade_to_defaults() {
        let s = parse_engine_stat(&json!({ "response": { "status": "prebuf" } }));
        assert_eq!(s.status, "prebuf");
        assert_eq!(s.peers, 0);
        assert_eq!(s.downloaded, 0);
    }

    #[test]
    fn engine_stat_records_absent_fields_distinctly_from_zero() {
        // `uploaded` present-but-zero must NOT be flagged; `downloaded`/`peers` absent must.
        let s = parse_engine_stat(&json!({ "response": { "status": "dl", "uploaded": 0 } }));
        assert_eq!(s.uploaded, 0);
        assert!(!s.missing_fields.contains(&"uploaded".to_string()));
        assert_eq!(s.missing_fields, vec!["peers", "downloaded"]);
        // A complete payload flags nothing.
        let full = parse_engine_stat(&json!({
            "response": { "status": "dl", "peers": 1, "downloaded": 2, "uploaded": 3 }
        }));
        assert!(full.missing_fields.is_empty());
    }

    #[test]
    fn outpace_status_flags_absent_upload_field() {
        let s = parse_outpace_status(&json!({ "peers": 2 }), 100);
        assert_eq!(s.missing_fields, vec!["uploaded"]);
    }

    #[test]
    fn parses_outpace_status_with_download_proxy() {
        let v = json!({
            "network": "ace",
            "id": "turl-xyz",
            "clients": 1,
            "peers": 2,
            "bitrate": 1_400_000,
            "buffer_ms": 3000,
            "uploaded": 4096,
            "peers_served": 1
        });
        let s = parse_outpace_status(&v, 999_999);
        assert_eq!(s.status, "dl"); // peers>=1 -> active
        assert_eq!(s.peers, 2);
        assert_eq!(s.uploaded, 4096);
        assert_eq!(
            s.downloaded, 999_999,
            "playback bytes used as download proxy"
        );
    }

    #[test]
    fn outpace_status_idle_when_no_activity() {
        let v = json!({ "clients": 0, "peers": 0, "bitrate": 0, "uploaded": 0 });
        let s = parse_outpace_status(&v, 0);
        assert_eq!(s.status, "idle");
    }

    #[test]
    fn urlencode_escapes_reserved_chars() {
        assert_eq!(
            urlencode("http://h:7002/a.acelive?x=1&y=2"),
            "http%3A%2F%2Fh%3A7002%2Fa.acelive%3Fx%3D1%26y%3D2"
        );
    }
}
