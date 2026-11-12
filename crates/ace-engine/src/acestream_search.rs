//! Bounded compatibility types for optional external stream-name search.
//!
//! Search metadata is discovery data only. Nothing here creates content IDs or
//! establishes descriptor provenance; playback must use its existing verifier.

use serde_json::{json, Map, Value};
use std::collections::HashSet;

pub const MAX_RESPONSE_BYTES: usize = 1_048_576;
const MAX_PAGE_SIZE: usize = 200;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct SearchQuery {
    pub query: String,
    pub page: u32,
    pub page_size: usize,
    pub category: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SearchError {
    InvalidQuery,
    InvalidResponse,
    Upstream,
    TooLarge,
    InvalidSettings,
    RateLimited,
    Busy,
    Timeout,
}

impl SearchQuery {
    /// Query extraction has already decoded form encoding. Reject ambiguous fields
    /// before making requests; unknown parameters must not control upstream access.
    pub fn parse(params: Vec<(String, String)>) -> Result<Self, SearchError> {
        let mut result = Self {
            query: String::new(),
            page: 0,
            page_size: 10,
            category: None,
        };
        let mut seen = HashSet::new();
        for (name, value) in params {
            if !seen.insert(name.clone()) {
                return Err(SearchError::InvalidQuery);
            }
            match name.as_str() {
                "query" if valid_text(&value, 256) => result.query = value,
                "category" if valid_text(&value, 64) => {
                    result.category = (!value.is_empty()).then_some(value);
                }
                "page" => {
                    result.page = value.parse().map_err(|_| SearchError::InvalidQuery)?;
                    if result.page > 10_000 {
                        return Err(SearchError::InvalidQuery);
                    }
                }
                "page_size" => {
                    result.page_size = value.parse().map_err(|_| SearchError::InvalidQuery)?;
                    if !(1..=MAX_PAGE_SIZE).contains(&result.page_size) {
                        return Err(SearchError::InvalidQuery);
                    }
                }
                _ => return Err(SearchError::InvalidQuery),
            }
        }
        Ok(result)
    }
}

/// Accept the current flat root object and the historical result envelope. The
/// rows in either variant must already be grouped: a single fetched page cannot
/// establish accurate channel totals by regrouping an arbitrary flat item list.
pub fn normalize_response(bytes: &[u8], query: &SearchQuery) -> Result<Value, SearchError> {
    if bytes.len() > MAX_RESPONSE_BYTES {
        return Err(SearchError::TooLarge);
    }
    let root: Value = serde_json::from_slice(bytes).map_err(|_| SearchError::InvalidResponse)?;
    if root.get("error").is_some_and(|error| !error.is_null()) {
        return Err(SearchError::Upstream);
    }
    let page = root.get("result").unwrap_or(&root);
    let total = page
        .get("total")
        .and_then(Value::as_u64)
        .ok_or(SearchError::InvalidResponse)?;
    let groups = page
        .get("results")
        .and_then(Value::as_array)
        .ok_or(SearchError::InvalidResponse)?;
    if !(1..=MAX_PAGE_SIZE).contains(&query.page_size)
        || groups.len() > query.page_size
        || total < groups.len() as u64
    {
        return Err(SearchError::InvalidResponse);
    }
    let mut results = Vec::with_capacity(groups.len());
    let mut item_count = 0usize;
    for group in groups {
        let name = text_field(group, "name", 256)?;
        let items = group
            .get("items")
            .and_then(Value::as_array)
            .ok_or(SearchError::InvalidResponse)?;
        item_count = item_count.saturating_add(items.len());
        if items.is_empty() || item_count > MAX_PAGE_SIZE {
            return Err(SearchError::InvalidResponse);
        }
        let items = items
            .iter()
            .map(normalize_item)
            .collect::<Result<Vec<_>, _>>()?;
        // Do not forward arbitrary upstream fields, EPG, content IDs or URLs.
        results.push(json!({"name":name,"items":items}));
    }
    Ok(json!({"result":{"total":total,"results":results},"error":null}))
}

fn valid_text(value: &str, limit: usize) -> bool {
    value.len() <= limit && !value.chars().any(char::is_control)
}

fn text_field<'a>(value: &'a Value, key: &str, limit: usize) -> Result<&'a str, SearchError> {
    value
        .get(key)
        .and_then(Value::as_str)
        .filter(|text| !text.is_empty() && valid_text(text, limit))
        .ok_or(SearchError::InvalidResponse)
}

fn normalize_item(item: &Value) -> Result<Value, SearchError> {
    let infohash = text_field(item, "infohash", 40)?;
    if infohash.len() != 40 || !infohash.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(SearchError::InvalidResponse);
    }
    let mut out = Map::new();
    out.insert("infohash".into(), json!(infohash.to_ascii_lowercase()));
    out.insert("name".into(), json!(text_field(item, "name", 256)?));
    for (key, limit) in [("categories", 64), ("countries", 16), ("languages", 16)] {
        if let Some(values) = item.get(key) {
            let values = values.as_array().ok_or(SearchError::InvalidResponse)?;
            if values.len() > 32
                || values.iter().any(|v| {
                    v.as_str()
                        .is_none_or(|s| s.is_empty() || !valid_text(s, limit))
                })
            {
                return Err(SearchError::InvalidResponse);
            }
            out.insert(key.into(), json!(values));
        } else if key == "categories" {
            out.insert(key.into(), json!([]));
        }
    }
    for key in ["bitrate", "availability_updated_at", "status"] {
        if let Some(value) = item.get(key) {
            let number = value.as_u64().ok_or(SearchError::InvalidResponse)?;
            if key == "status" && number > 2 {
                return Err(SearchError::InvalidResponse);
            }
            out.insert(key.into(), json!(number));
        }
    }
    if let Some(value) = item.get("availability") {
        let number = value.as_f64().ok_or(SearchError::InvalidResponse)?;
        if !(0.0..=1.0).contains(&number) {
            return Err(SearchError::InvalidResponse);
        }
        out.insert("availability".into(), json!(number));
    }
    Ok(Value::Object(out))
}

#[cfg(test)]
mod tests;

#[derive(Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct SearchSettings {
    pub enabled: bool,
    #[serde(skip)]
    key: Option<String>,
}
impl std::fmt::Debug for SearchSettings {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SearchSettings")
            .field("enabled", &self.enabled)
            .finish_non_exhaustive()
    }
}
impl SearchSettings {
    pub fn from_env(
        enabled: bool,
        key: Result<String, std::env::VarError>,
    ) -> Result<Self, String> {
        if !enabled {
            return Ok(Self::default());
        }
        let key = key.map_err(|_| {
            "OUTPACE_ACESTREAM_SEARCH_API_KEY is required when services compatibility is enabled"
        })?;
        if key.is_empty() || key.len() > 512 || !key.bytes().all(|b| b.is_ascii_graphic()) {
            return Err("OUTPACE_ACESTREAM_SEARCH_API_KEY must contain 1–512 visible ASCII bytes without spaces".into());
        }
        Ok(Self {
            enabled,
            key: Some(key),
        })
    }
    pub fn validate(&self, private: bool) -> Result<(), String> {
        if self.enabled && private {
            return Err(
                "OUTPACE_COMPAT_ACESTREAM_SERVICES conflicts with private peer mode".into(),
            );
        }
        if self.enabled {
            Self::from_env(true, self.key.clone().ok_or(std::env::VarError::NotPresent))?;
        }
        Ok(())
    }
}
mod service;
pub use service::SearchService;

#[cfg(test)]
pub(crate) use service::tests::offline_service;
