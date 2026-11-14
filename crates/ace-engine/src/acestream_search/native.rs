//! Native search v4 codec; see protocol note 74 for recovered evidence and limits.
//! These shared-key checks are protocol compatibility, not descriptor provenance.

use super::*;
use base64ct::{Base64, Encoding};
use sha1::{Digest, Sha1};
use std::collections::BTreeMap;

// Fixed, global client protocol constants, like ace-wire's TRANSPORT_KEY.
// They are not operator/account credentials or server-only authentication keys.
const REQUEST_KEY: &[u8] = &[
    120, 88, 122, 104, 84, 100, 65, 100, 49, 112, 79, 106, 76, 49, 110, 68, 65, 86, 73, 57, 84, 88,
    108, 73, 66, 118, 112, 121, 51, 68, 86, 101, 53, 99, 119, 77, 97, 111, 48, 111, 66, 105, 118,
    83, 114, 120, 76, 121, 115, 97, 71, 119, 83, 71, 69, 73,
];
pub(super) const RESPONSE_KEY: &[u8] = &[
    70, 69, 118, 53, 81, 72, 84, 75, 122, 111, 84, 90, 71, 53, 111, 87, 84, 88, 85, 112, 109, 101,
    116, 100, 106, 102, 52, 99, 102, 83, 89, 86, 115, 56, 115, 90, 68, 107, 98, 122, 66, 100, 117,
    112, 82, 54, 108, 97, 55, 120, 97, 72, 51,
];

pub(super) struct NativeRequest {
    params: BTreeMap<String, String>,
    pub nonce: String,
}
impl NativeRequest {
    /// A request labelled as outpace, or, under the AceStream identity, shaped exactly as
    /// the official engine sends it.
    pub fn for_client(
        query: &SearchQuery,
        timestamp: u64,
        nonce: String,
        identity: Option<&ace_swarm::engine_profile::EngineIdentity>,
    ) -> Self {
        match identity {
            Some(identity) => Self::as_engine(query, timestamp, nonce, identity),
            None => Self::new(query, timestamp, nonce),
        }
    }

    /// The engine's parameter set: `_a _b _n _p _r _t _v a [c] [h] i m`. It omits `h` for
    /// the first page and never sends `s` or `t`.
    fn as_engine(
        query: &SearchQuery,
        timestamp: u64,
        nonce: String,
        identity: &ace_swarm::engine_profile::EngineIdentity,
    ) -> Self {
        let mut params: BTreeMap<String, String> = [
            ("_a", hex::encode(identity.device_id)),
            ("_b", hex::encode(identity.hardware_key)),
            ("_n", identity.version_name.clone()),
            ("_p", identity.platform.name().to_owned()),
            ("_r", nonce.clone()),
            ("_t", timestamp.to_string()),
            ("_v", identity.version_code.to_string()),
            ("a", query.query.clone()),
            ("i", query.page_size.to_string()),
            ("m", "4".into()),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_owned(), v))
        .collect();
        if query.page > 0 {
            params.insert("h".into(), query.page.to_string());
        }
        if let Some(category) = &query.category {
            params.insert("c".into(), category.clone());
        }
        Self::signed(params, nonce)
    }

    pub fn new(query: &SearchQuery, timestamp: u64, nonce: String) -> Self {
        let major: u64 = env!("CARGO_PKG_VERSION_MAJOR").parse().unwrap();
        let minor: u64 = env!("CARGO_PKG_VERSION_MINOR").parse().unwrap();
        let patch: u64 = env!("CARGO_PKG_VERSION_PATCH").parse().unwrap();
        let mut params: BTreeMap<String, String> = [
            ("_n", env!("CARGO_PKG_VERSION").to_owned()),
            (
                "_v",
                (major * 1_000_000 + minor * 10_000 + patch * 100).to_string(),
            ),
            ("_p", "outpace".into()),
            ("_t", timestamp.to_string()),
            ("_r", nonce.clone()),
            ("a", query.query.clone()),
            ("h", query.page.to_string()),
            ("i", query.page_size.to_string()),
            ("m", "4".into()),
            ("s", String::new()),
            ("t", String::new()),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_owned(), v))
        .collect();
        if let Some(category) = &query.category {
            params.insert("c".into(), category.clone());
        }
        Self::signed(params, nonce)
    }

    fn signed(mut params: BTreeMap<String, String>, nonce: String) -> Self {
        let canonical = params
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join("#");
        let mut hash = Sha1::new();
        hash.update(canonical.as_bytes());
        hash.update(REQUEST_KEY);
        params.insert("_s".into(), hex::encode(hash.finalize()));
        Self { params, nonce }
    }
    pub fn url(&self, host: &str) -> String {
        format!("http://{host}{}", self.path_and_query())
    }

    /// `/a?<params>` with `_s` last, as the engine orders it.
    pub fn path_and_query(&self) -> String {
        let mut form = form_urlencoded::Serializer::new(String::new());
        form.extend_pairs(self.params.iter().filter(|(k, _)| k.as_str() != "_s"));
        form.extend_pairs(self.params.get_key_value("_s"));
        format!("/a?{}", form.finish())
    }
}

/// Authenticate the envelope against this request nonce before decoding metadata.
/// Deliberately retain only infohash-backed rows and the existing metadata allowlist.
pub(super) fn decode_response(bytes: &[u8], nonce: &str) -> Result<Vec<u8>, SearchError> {
    if bytes.len() > MAX_RESPONSE_BYTES {
        return Err(SearchError::TooLarge);
    }
    let envelope: Value =
        serde_json::from_slice(bytes).map_err(|_| SearchError::InvalidResponse)?;
    let data = envelope
        .get("data")
        .and_then(Value::as_str)
        .ok_or(SearchError::InvalidResponse)?;
    let sign = envelope
        .get("sign")
        .and_then(Value::as_str)
        .ok_or(SearchError::InvalidResponse)?;
    let mut hash = Sha1::new();
    hash.update(data.as_bytes());
    hash.update(nonce.as_bytes());
    hash.update(RESPONSE_KEY);
    if sign != hex::encode(hash.finalize()) {
        return Err(SearchError::InvalidResponse);
    }
    let decoded = Base64::decode_vec(data).map_err(|_| SearchError::InvalidResponse)?;
    if decoded.len() > MAX_RESPONSE_BYTES {
        return Err(SearchError::TooLarge);
    }
    let mut root: Value =
        serde_json::from_slice(&decoded).map_err(|_| SearchError::InvalidResponse)?;
    if root
        .get("time")
        .and_then(Value::as_f64)
        .is_none_or(|n| n < 0.0)
    {
        return Err(SearchError::InvalidResponse);
    }
    let groups = root
        .get_mut("results")
        .and_then(Value::as_array_mut)
        .ok_or(SearchError::InvalidResponse)?;
    if groups.len() > MAX_PAGE_SIZE {
        return Err(SearchError::InvalidResponse);
    }
    let mut count = 0usize;
    for group in groups {
        let items = group
            .get_mut("items")
            .and_then(Value::as_array_mut)
            .ok_or(SearchError::InvalidResponse)?;
        count = count.saturating_add(items.len());
        if items.is_empty() || count > MAX_PAGE_SIZE {
            return Err(SearchError::InvalidResponse);
        }
        for item in items {
            let compact = item.as_object().ok_or(SearchError::InvalidResponse)?;
            let mut expanded = Map::new();
            for (from, to) in [
                ("n", "name"),
                ("i", "infohash"),
                ("b", "bitrate"),
                ("c", "categories"),
                ("a", "availability"),
                ("au", "availability_updated_at"),
                ("s", "status"),
                ("countries", "countries"),
                ("languages", "languages"),
            ] {
                if let Some(value) = compact.get(from) {
                    expanded.insert(to.into(), value.clone());
                }
            }
            // The native service emits integer flags as well as JSON booleans.
            let disabled = match compact.get("d") {
                None => false,
                Some(Value::Bool(value)) => *value,
                Some(value) if value.as_u64() == Some(0) => false,
                Some(value) if value.as_u64() == Some(1) => true,
                _ => return Err(SearchError::InvalidResponse),
            };
            expanded.insert("disabled".into(), json!(disabled));
            *item = Value::Object(expanded);
        }
    }
    let output = serde_json::to_vec(&root).map_err(|_| SearchError::InvalidResponse)?;
    if output.len() > MAX_RESPONSE_BYTES {
        return Err(SearchError::TooLarge);
    }
    Ok(output)
}

#[cfg(test)]
mod tests;
