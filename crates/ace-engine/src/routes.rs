//! HTTP route parsing for the 6878-compatible engine surface (pure).
//!
//! Maps `(method, path, query)` to a [`Route`]. Mirrors the official engine's URL subset
//! documented in the design spec; serving/session logic lives elsewhere. No I/O.

/// A parsed engine HTTP route.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Route {
    /// `GET /ace/getstream` — start/attach a playback session. Carries the content
    /// selector from the query (`content_id=` for acestream ids, else `infohash=`/`id=`).
    GetStream { selector: ContentSelector },
    /// `GET /ace/manifest.m3u8` — HLS media playlist for a session.
    Manifest { selector: ContentSelector },
    /// `GET /ace/c/<session>/<seq>.ts` — one HLS segment.
    Segment { session: String, seq: u64 },
    /// `GET /ace/stat/<infohash>/<token>` — session stats.
    Stat { infohash: String, token: String },
    /// `GET /ace/cmd/<infohash>/<token>?method=<m>` — session command (e.g. stop).
    Command { infohash: String, token: String, method: String },
    /// `/server/api` — JSON control API.
    ServerApi,
    /// Anything unmatched.
    NotFound,
}

/// How the caller selected the content on `getstream`/`manifest`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ContentSelector {
    ContentId(String),
    Infohash(String),
    Id(String),
    Missing,
}

fn query_pairs(query: &str) -> Vec<(&str, &str)> {
    query
        .split('&')
        .filter(|s| !s.is_empty())
        .map(|kv| match kv.split_once('=') {
            Some((k, v)) => (k, v),
            None => (kv, ""),
        })
        .collect()
}

fn selector_from(query: &str) -> ContentSelector {
    let pairs = query_pairs(query);
    let get = |k: &str| pairs.iter().find(|(pk, _)| *pk == k).map(|(_, v)| v.to_string());
    // content_id wins (acestream ids), per the engine API quirk.
    if let Some(v) = get("content_id") {
        ContentSelector::ContentId(v)
    } else if let Some(v) = get("infohash") {
        ContentSelector::Infohash(v)
    } else if let Some(v) = get("id") {
        ContentSelector::Id(v)
    } else {
        ContentSelector::Missing
    }
}

/// Parse a request into a [`Route`]. `path` excludes the query string; `query` is the raw
/// query (without `?`). Non-GET methods only match where the engine expects them.
pub fn parse(method: &str, path: &str, query: &str) -> Route {
    if method != "GET" {
        return Route::NotFound;
    }
    if path == "/ace/getstream" {
        return Route::GetStream { selector: selector_from(query) };
    }
    if path == "/ace/manifest.m3u8" {
        return Route::Manifest { selector: selector_from(query) };
    }
    if path == "/server/api" {
        return Route::ServerApi;
    }
    let seg: Vec<&str> = path.trim_start_matches('/').split('/').collect();
    match seg.as_slice() {
        ["ace", "c", session, file] if file.ends_with(".ts") => {
            if let Ok(n) = file.trim_end_matches(".ts").parse::<u64>() {
                return Route::Segment { session: (*session).to_string(), seq: n };
            }
        }
        ["ace", "stat", ih, token] => {
            return Route::Stat { infohash: (*ih).to_string(), token: (*token).to_string() };
        }
        ["ace", "cmd", ih, token] => {
            let method_q = query_pairs(query)
                .into_iter()
                .find(|(k, _)| *k == "method")
                .map(|(_, v)| v.to_string())
                .unwrap_or_default();
            return Route::Command {
                infohash: (*ih).to_string(),
                token: (*token).to_string(),
                method: method_q,
            };
        }
        _ => {}
    }
    Route::NotFound
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn getstream_prefers_content_id() {
        assert_eq!(
            parse("GET", "/ace/getstream", "content_id=abc&infohash=def"),
            Route::GetStream { selector: ContentSelector::ContentId("abc".into()) }
        );
    }

    #[test]
    fn getstream_falls_back_to_infohash_then_id() {
        assert_eq!(
            parse("GET", "/ace/getstream", "infohash=def"),
            Route::GetStream { selector: ContentSelector::Infohash("def".into()) }
        );
        assert_eq!(
            parse("GET", "/ace/getstream", "id=xyz"),
            Route::GetStream { selector: ContentSelector::Id("xyz".into()) }
        );
        assert_eq!(
            parse("GET", "/ace/getstream", ""),
            Route::GetStream { selector: ContentSelector::Missing }
        );
    }

    #[test]
    fn parses_segment_path() {
        assert_eq!(
            parse("GET", "/ace/c/sess123/42.ts", ""),
            Route::Segment { session: "sess123".into(), seq: 42 }
        );
    }

    #[test]
    fn parses_stat_and_command() {
        assert_eq!(
            parse("GET", "/ace/stat/IH/TOK", ""),
            Route::Stat { infohash: "IH".into(), token: "TOK".into() }
        );
        assert_eq!(
            parse("GET", "/ace/cmd/IH/TOK", "method=stop"),
            Route::Command { infohash: "IH".into(), token: "TOK".into(), method: "stop".into() }
        );
    }

    #[test]
    fn manifest_and_server_api() {
        assert_eq!(
            parse("GET", "/ace/manifest.m3u8", "id=z"),
            Route::Manifest { selector: ContentSelector::Id("z".into()) }
        );
        assert_eq!(parse("GET", "/server/api", ""), Route::ServerApi);
    }

    #[test]
    fn unknown_and_non_get_are_not_found() {
        assert_eq!(parse("GET", "/nope", ""), Route::NotFound);
        assert_eq!(parse("POST", "/ace/getstream", "content_id=a"), Route::NotFound);
        assert_eq!(parse("GET", "/ace/c/sess/notanumber.ts", ""), Route::NotFound);
    }
}
