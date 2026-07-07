//! Encode a transport-file `url=` input as a **reversible, path-safe** provider id and decode it
//! back. Used by the CLI and the `/ace/getstream` compatibility route so the id that ends up in a
//! playback URL (`/ace/r/{id}`) is self-contained: it survives a daemon restart and a direct hit
//! without any server-side lookup table, and it dispatches in [`crate::ace_provider`] by decoding
//! back to the original URL. The http/https scheme check lives here so it is not duplicated across
//! the input surfaces.

use base64ct::{Base64UrlUnpadded, Encoding};

/// Prefix marking a base64url-encoded transport-file URL id. `-` (not `:`) keeps the whole id a
/// single URL-path-safe segment, and the base64url alphabet (`A-Za-z0-9-_`) never introduces `/`
/// or `?`, so the id round-trips cleanly through `/ace/r/{id}`.
const TURL_PREFIX: &str = "turl-";

/// Validate `url` is an http/https URL and encode it into a provider id `turl-<base64url(url)>`.
/// Trims surrounding whitespace so the CLI and HTTP surfaces agree on the same id for the same
/// input.
pub(crate) fn encode_transport_url(url: &str) -> Result<String, String> {
    let url = url.trim();
    if !(url.starts_with("http://") || url.starts_with("https://")) {
        return Err("transport url must be http or https".into());
    }
    Ok(format!(
        "{TURL_PREFIX}{}",
        Base64UrlUnpadded::encode_string(url.as_bytes())
    ))
}

/// Decode a `turl-<base64url>` provider id back to its URL. Returns `None` for any id that is not a
/// transport-url id or whose payload is not valid base64url/UTF-8 — the provider then treats it as
/// an unrecognized id.
pub(crate) fn decode_transport_url(id: &str) -> Option<String> {
    let b64 = id.strip_prefix(TURL_PREFIX)?;
    let bytes = Base64UrlUnpadded::decode_vec(b64).ok()?;
    String::from_utf8(bytes).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_a_url_through_a_path_safe_id() {
        let url = "https://cdn.example.com/a/b.acelive?token=abc&sig=xyz";
        let id = encode_transport_url(url).unwrap();
        // Path-safe: a single segment with no '/', ':' or '?'.
        assert!(id.starts_with("turl-"));
        assert!(!id.contains('/'));
        assert!(!id.contains(':'));
        assert!(!id.contains('?'));
        // Reversible with no side table.
        assert_eq!(decode_transport_url(&id).as_deref(), Some(url));
    }

    #[test]
    fn trims_whitespace_so_surfaces_agree() {
        assert_eq!(
            encode_transport_url("  https://h/x  ").unwrap(),
            encode_transport_url("https://h/x").unwrap()
        );
    }

    #[test]
    fn rejects_non_http_scheme() {
        assert!(encode_transport_url("file:///etc/passwd").is_err());
        assert!(encode_transport_url("ftp://h/x").is_err());
    }

    #[test]
    fn decode_ignores_non_transport_or_garbage_ids() {
        assert_eq!(decode_transport_url("cid:0123"), None);
        assert_eq!(
            decode_transport_url("0123456789abcdef0123456789abcdef01234567"),
            None
        );
        assert_eq!(decode_transport_url("turl-@@@not-base64@@@"), None);
    }
}
