//! `/server/api` — the official-engine JSON control API (targeted compatibility subset).
//!
//! Acestream's `/server/api` dispatches on a `?method=` query parameter and answers with a
//! `{ "result": <value>, "error": <null|message> }` envelope — note the `result` key, distinct
//! from the `/ace/*` playback routes' `response` key (a real engine quirk; see
//! `docs/protocol/notes/10-interop.md`, which proves `analyze_content` → `.result.infohash`).
//!
//! This module is the pure half: method/selector parsing and envelope/result shaping, with no
//! I/O. The async handler in [`crate::http`] performs catalog/transport resolution and calls
//! these builders. Only a high-value method subset is served — the full contract, including the
//! routes we intentionally reject, is documented in `docs/protocol/compat-matrix.md`.

use ace_swarm::resolve::{hex20, infohash_hex};
use serde_json::{json, Value};
use std::collections::HashMap;

/// A recognized `/server/api?method=` value. Anything unrecognized is [`Method::Unknown`], which
/// the handler answers with an error envelope (still HTTP 200, matching the engine).
#[derive(Debug, PartialEq, Eq)]
pub enum Method {
    /// `get_version` — engine version, for client capability negotiation.
    GetVersion,
    /// `get_status` — daemon/session status.
    GetStatus,
    /// `get_network_connection_status` — network reachability status.
    GetNetworkConnectionStatus,
    /// `analyze_content` — resolve a content selector to its infohash and basic metadata.
    AnalyzeContent,
    /// `get_content_id` — the acestream content id for a selector.
    GetContentId,
    /// `get_media_files` — the media file(s) behind a selector.
    GetMediaFiles,
    /// Any other (unsupported) method name, preserved for the error message.
    Unknown(String),
}

/// Parse the `method=` value into a [`Method`].
pub fn parse_method(raw: &str) -> Method {
    match raw {
        "get_version" => Method::GetVersion,
        "get_status" => Method::GetStatus,
        "get_network_connection_status" => Method::GetNetworkConnectionStatus,
        "analyze_content" => Method::AnalyzeContent,
        "get_content_id" => Method::GetContentId,
        "get_media_files" => Method::GetMediaFiles,
        other => Method::Unknown(other.to_string()),
    }
}

/// Build a success envelope `{ "result": <result>, "error": null }`.
pub fn ok(result: Value) -> Value {
    json!({ "result": result, "error": Value::Null })
}

/// Build an error envelope `{ "result": null, "error": <message> }`. The engine returns HTTP 200
/// with the error in-band rather than an HTTP error status, so callers always parse the body.
pub fn err(message: impl Into<String>) -> Value {
    json!({ "result": Value::Null, "error": message.into() })
}

/// Which content parameter the caller used to select content. Mirrors the `/ace/getstream`
/// precedence: `content_id`/`query` (catalog-resolved) win over a direct `infohash`, then `url`,
/// then `magnet`. `query` is the param proven in note 10 and is treated as a content id.
#[derive(Debug, PartialEq, Eq)]
pub enum Selector {
    /// An acestream content id (40-hex), resolved to an infohash via the catalog.
    ContentId(String),
    /// A BitTorrent infohash (40-hex), used directly.
    Infohash(String),
    /// A transport-file URL, resolved by fetching the descriptor.
    Url(String),
    /// A magnet link, parsed for its infohash with no network.
    Magnet(String),
    /// No usable content parameter was supplied.
    Missing,
}

/// Extract the content [`Selector`] from the query parameters, honoring the precedence above.
pub fn selector(params: &HashMap<String, String>) -> Selector {
    let nonempty = |k: &str| {
        params
            .get(k)
            .map(String::as_str)
            .filter(|v| !v.is_empty())
            .map(str::to_string)
    };
    if let Some(v) = nonempty("content_id").or_else(|| nonempty("query")) {
        Selector::ContentId(v)
    } else if let Some(v) = nonempty("infohash") {
        Selector::Infohash(v)
    } else if let Some(v) = nonempty("url") {
        Selector::Url(v)
    } else if let Some(v) = nonempty("magnet") {
        Selector::Magnet(v)
    } else {
        Selector::Missing
    }
}

/// A content selector resolved to the fields the `analyze_content`/`get_content_id`/
/// `get_media_files` methods report.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedContent {
    /// Canonical lowercase 40-hex infohash.
    pub infohash: String,
    /// The acestream content id, when the selector carried or resolved one.
    pub content_id: Option<String>,
    /// Whether this is a live stream (the primary outpace path). A bare infohash is assumed live.
    pub is_live: bool,
}

/// Resolve the selectors that need no network — a direct `infohash` or a `magnet` link.
///
/// Returns `None` for selectors that require catalog/transport resolution (`content_id`, `url`);
/// the handler performs those asynchronously. `Some(Err(_))` is a selector we own but can't
/// accept (e.g. a malformed infohash/magnet).
pub fn resolve_offline(sel: &Selector) -> Option<Result<ResolvedContent, String>> {
    match sel {
        Selector::Infohash(h) => Some(match hex20(h) {
            Ok(bytes) => Ok(ResolvedContent {
                infohash: infohash_hex(&bytes),
                content_id: None,
                is_live: true,
            }),
            Err(_) => Err(format!("invalid infohash: {h}")),
        }),
        Selector::Magnet(m) => Some(match crate::magnet::parse_magnet_infohash(m) {
            Ok(hex) => Ok(ResolvedContent {
                infohash: hex,
                content_id: None,
                is_live: true,
            }),
            Err(e) => Err(format!("invalid magnet: {e:?}")),
        }),
        Selector::ContentId(_) | Selector::Url(_) | Selector::Missing => None,
    }
}

/// `get_version` result: the crate version plus a monotonic integer code some clients compare.
pub fn version_result() -> Value {
    let version = env!("CARGO_PKG_VERSION");
    json!({ "version": version, "code": version_code(version) })
}

/// Encode `MAJOR.MINOR.PATCH` as `MAJOR*10000 + MINOR*100 + PATCH` for clients that gate on an
/// integer version code. Non-numeric or missing components contribute 0.
fn version_code(version: &str) -> u64 {
    let mut parts = version.split('.').map(|p| p.parse::<u64>().unwrap_or(0));
    let major = parts.next().unwrap_or(0);
    let minor = parts.next().unwrap_or(0);
    let patch = parts.next().unwrap_or(0);
    major * 10_000 + minor * 100 + patch
}

/// `get_status` result: engine liveness plus how many sessions are currently active.
pub fn status_result(active_sessions: usize) -> Value {
    let status = if active_sessions > 0 { "dl" } else { "idle" };
    json!({ "status": status, "active_sessions": active_sessions })
}

/// `get_network_connection_status` result: whether the daemon has usable networks registered.
pub fn network_status_result(networks: &[String]) -> Value {
    let connected = !networks.is_empty();
    json!({
        "status": if connected { "connected" } else { "disconnected" },
        "connected": connected,
        "networks": networks,
    })
}

/// `analyze_content` result: at minimum the resolved `infohash` (the field proven in note 10),
/// plus the content id when known and live/encrypted flags matching the `/ace/*` surface.
pub fn analyze_result(c: &ResolvedContent) -> Value {
    json!({
        "infohash": c.infohash,
        "content_id": c.content_id,
        "is_live": bool_flag(c.is_live),
        "is_encrypted": 0,
        "status": "complete",
    })
}

/// `get_content_id` result: the acestream content id for the selector.
pub fn content_id_result(content_id: &str) -> Value {
    json!({ "content_id": content_id })
}

/// `get_media_files` result: outpace transports are single-file, so this reports one media file
/// keyed by the infohash. `dump_transport_file` is intentionally unsupported (see the matrix).
pub fn media_files_result(c: &ResolvedContent) -> Value {
    json!({
        "infohash": c.infohash,
        "files": [{
            "infohash": c.infohash,
            "index": 0,
            "is_live": bool_flag(c.is_live),
        }],
    })
}

/// The engine reports booleans as `0`/`1` integers in these envelopes.
fn bool_flag(v: bool) -> u8 {
    v as u8
}

#[cfg(test)]
mod tests {
    use super::*;

    fn params(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn parses_the_supported_methods_and_flags_others_unknown() {
        assert_eq!(parse_method("get_version"), Method::GetVersion);
        assert_eq!(parse_method("analyze_content"), Method::AnalyzeContent);
        assert_eq!(parse_method("get_media_files"), Method::GetMediaFiles);
        assert_eq!(
            parse_method("get_available_channels"),
            Method::Unknown("get_available_channels".to_string())
        );
    }

    #[test]
    fn ok_and_err_use_the_result_error_envelope() {
        assert_eq!(
            ok(json!({ "a": 1 })),
            json!({ "result": { "a": 1 }, "error": null })
        );
        assert_eq!(err("nope"), json!({ "result": null, "error": "nope" }));
    }

    #[test]
    fn selector_precedence_matches_getstream() {
        // content_id wins over infohash.
        assert_eq!(
            selector(&params(&[("content_id", "cid"), ("infohash", "ih")])),
            Selector::ContentId("cid".to_string())
        );
        // query is an alias for content_id (the param proven in note 10).
        assert_eq!(
            selector(&params(&[("query", "cid")])),
            Selector::ContentId("cid".to_string())
        );
        assert_eq!(
            selector(&params(&[("infohash", "ih")])),
            Selector::Infohash("ih".to_string())
        );
        // Empty values are ignored.
        assert_eq!(selector(&params(&[("infohash", "")])), Selector::Missing);
        assert_eq!(selector(&params(&[])), Selector::Missing);
    }

    #[test]
    fn offline_resolution_canonicalizes_a_direct_infohash() {
        let sel = Selector::Infohash("50E93529D3EB46A50506B14464185A15292D6E47".to_string());
        let resolved = resolve_offline(&sel).unwrap().unwrap();
        assert_eq!(
            resolved.infohash,
            "50e93529d3eb46a50506b14464185a15292d6e47"
        );
        assert_eq!(resolved.content_id, None);
        assert!(resolved.is_live);
    }

    #[test]
    fn offline_resolution_rejects_a_malformed_infohash() {
        let sel = Selector::Infohash("not-a-hash".to_string());
        assert!(resolve_offline(&sel).unwrap().is_err());
    }

    #[test]
    fn content_id_and_url_selectors_defer_to_async_resolution() {
        assert!(resolve_offline(&Selector::ContentId("x".to_string())).is_none());
        assert!(resolve_offline(&Selector::Url("http://x".to_string())).is_none());
        assert!(resolve_offline(&Selector::Missing).is_none());
    }

    #[test]
    fn version_result_carries_the_crate_version_and_an_integer_code() {
        let v = version_result();
        assert_eq!(v["version"], env!("CARGO_PKG_VERSION"));
        assert!(v["code"].as_u64().is_some());
    }

    #[test]
    fn version_code_packs_semver_components() {
        assert_eq!(version_code("0.1.0"), 100);
        assert_eq!(version_code("1.2.3"), 10_203);
        assert_eq!(version_code("2.0"), 20_000);
    }

    #[test]
    fn status_result_reflects_active_session_count() {
        assert_eq!(status_result(0)["status"], "idle");
        assert_eq!(status_result(2)["status"], "dl");
        assert_eq!(status_result(2)["active_sessions"], 2);
    }

    #[test]
    fn network_status_result_reports_connectivity() {
        let up = network_status_result(&["ace".to_string()]);
        assert_eq!(up["status"], "connected");
        assert_eq!(up["connected"], true);
        let down = network_status_result(&[]);
        assert_eq!(down["status"], "disconnected");
        assert_eq!(down["connected"], false);
    }

    #[test]
    fn analyze_result_exposes_infohash_and_flags() {
        let c = ResolvedContent {
            infohash: "aa".repeat(20),
            content_id: Some("cid".to_string()),
            is_live: true,
        };
        let v = analyze_result(&c);
        assert_eq!(v["infohash"], "aa".repeat(20));
        assert_eq!(v["content_id"], "cid");
        assert_eq!(v["is_live"], 1);
        assert_eq!(v["is_encrypted"], 0);
    }

    #[test]
    fn media_files_result_lists_the_single_file_by_infohash() {
        let c = ResolvedContent {
            infohash: "bb".repeat(20),
            content_id: None,
            is_live: false,
        };
        let v = media_files_result(&c);
        assert_eq!(v["infohash"], "bb".repeat(20));
        assert_eq!(v["files"][0]["infohash"], "bb".repeat(20));
        assert_eq!(v["files"][0]["is_live"], 0);
    }

    #[test]
    fn content_id_result_echoes_the_id() {
        assert_eq!(content_id_result("cid")["content_id"], "cid");
    }
}
