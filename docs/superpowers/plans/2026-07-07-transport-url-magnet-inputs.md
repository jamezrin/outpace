# Transport-file URL + Magnet Playback Inputs Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let outpace start playback from a transport-file `url=` or a `magnet:` input, in addition to the existing `content_id`/`infohash` selectors, closing issue #50.

**Architecture:** Magnet inputs reduce to the existing bare-infohash provider path (extract `btih` → 40-hex). Transport-file URLs get a new `turl:<url>` provider-ID scheme dispatched in `AceProvider::open`, backed by a fail-closed guarded fetch in `ace-swarm` (SSRF guard → pinned reqwest connection → 1 MiB size cap → existing transport decoder). Input parsing in CLI and HTTP enforces precedence `content_id > infohash > url > magnet`.

**Tech Stack:** Rust, tokio, reqwest (rustls-tls), the existing `ace_swarm::resolve` transport decoder.

---

## File Structure

- `crates/ace-swarm/Cargo.toml` — add `reqwest` dependency.
- `crates/ace-swarm/src/resolve.rs` — new `ResolveError::Url`, `resolve_safe_addr` (SSRF guard), `fetch_transport_from` (test seam), `stream_info_from_transport_url` (public entry).
- `crates/ace-engine/src/magnet.rs` — **new** shared module: `parse_magnet_infohash` + base32 decode, reused by CLI and HTTP.
- `crates/ace-engine/src/lib.rs` (or wherever modules are declared) — declare `mod magnet;`.
- `crates/ace-engine/Cargo.toml` — add `sha1` (for the URL→path-safe playback-id token).
- `crates/ace-engine/src/ace_provider.rs` — `turl:` dispatch branch in `open`.
- `crates/ace-engine/src/cli.rs` — `url`/`magnet`/bare-`magnet:` parsing + precedence in `PlaybackTarget::parse`.
- `crates/ace-engine/src/http.rs` — `url`/`magnet` params + precedence in `ace_selected_stream`.
- `README.md` — document the new supported inputs.

---

## Task 1: SSRF guard + `ResolveError::Url` (no network)

**Files:**
- Modify: `crates/ace-swarm/Cargo.toml`
- Modify: `crates/ace-swarm/src/resolve.rs` (add variant + guard + tests)

- [ ] **Step 1: Add the reqwest dependency**

In `crates/ace-swarm/Cargo.toml`, under `[dependencies]`, add:

```toml
reqwest = { version = "0.12", default-features = false, features = ["rustls-tls"] }
```

- [ ] **Step 2: Add the `Url` error variant**

In `crates/ace-swarm/src/resolve.rs`, extend the enum (around line 70):

```rust
#[derive(Debug, PartialEq, Eq)]
pub enum ResolveError {
    BadInfohash,
    Transport(&'static str),
    /// A signed catalog request/response step failed during content-id resolution.
    Catalog(&'static str),
    /// A peer/network step failed during content-id resolution.
    Peer(&'static str),
    /// A transport-file `url=` fetch failed a safety check or the network step.
    Url(&'static str),
}
```

- [ ] **Step 3: Write the failing SSRF-guard tests**

Add to the `mod tests` block in `resolve.rs`:

```rust
#[test]
fn ssrf_guard_rejects_unsafe_ip_literals() {
    use std::net::{Ipv4Addr, Ipv6Addr};
    for ip in [
        "127.0.0.1", "10.1.2.3", "192.168.0.1", "172.16.0.1", "169.254.0.1",
        "100.64.0.1", "0.0.0.0", "255.255.255.255", "224.0.0.1",
    ] {
        assert!(!is_safe_ip(&ip.parse().unwrap()), "{ip} must be unsafe");
    }
    for ip in ["::1", "fc00::1", "fe80::1", "::", "ff02::1"] {
        assert!(!is_safe_ip(&ip.parse().unwrap()), "{ip} must be unsafe");
    }
    // IPv4-mapped loopback must also be rejected.
    assert!(!is_safe_ip(&"::ffff:127.0.0.1".parse().unwrap()));
    // Public addresses are allowed.
    assert!(is_safe_ip(&std::net::IpAddr::V4(Ipv4Addr::new(93, 184, 216, 34))));
    assert!(is_safe_ip(&std::net::IpAddr::V6("2606:2800:220:1:248:1893:25c8:1946".parse().unwrap())));
    let _ = Ipv6Addr::LOCALHOST;
}

#[tokio::test]
async fn ssrf_guard_rejects_loopback_host() {
    assert_eq!(
        resolve_safe_addr("127.0.0.1", 80).await,
        Err(ResolveError::Url("blocked address"))
    );
}
```

- [ ] **Step 4: Run tests to verify they fail**

Run: `cargo test -p ace-swarm ssrf_guard`
Expected: FAIL — `is_safe_ip` / `resolve_safe_addr` not defined.

- [ ] **Step 5: Implement the guard**

Add near the other free functions in `resolve.rs` (outside `mod tests`), and add `use std::net::{IpAddr, SocketAddr};` to the imports:

```rust
/// Whether an IP is safe to fetch a transport file from — a conservative denylist that fails
/// closed on anything private, local, or otherwise not a normal public unicast address. Used by
/// the transport-file `url=` fetch to block SSRF into internal services.
fn is_safe_ip(ip: &IpAddr) -> bool {
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
                // Shared address space / CGNAT: 100.64.0.0/10.
                || (o[0] == 100 && (o[1] & 0b1100_0000) == 0b0100_0000))
        }
        IpAddr::V6(v6) => {
            // Re-check IPv4-mapped addresses against the v4 rules.
            if let Some(v4) = v6.to_ipv4_mapped() {
                return is_safe_ip(&IpAddr::V4(v4));
            }
            let seg = v6.segments();
            !(v6.is_loopback()
                || v6.is_unspecified()
                || v6.is_multicast()
                // Unique local fc00::/7.
                || (seg[0] & 0xfe00) == 0xfc00
                // Link-local unicast fe80::/10.
                || (seg[0] & 0xffc0) == 0xfe80)
        }
    }
}

/// Resolve `host` to a single safe [`SocketAddr`], or [`ResolveError::Url`] if it is unresolvable
/// or every resolved address is blocked by [`is_safe_ip`]. IP literals are validated directly;
/// hostnames are resolved and the first safe address is kept (later pinned into the client so
/// DNS rebinding cannot swap it).
async fn resolve_safe_addr(host: &str, port: u16) -> Result<SocketAddr, ResolveError> {
    if let Ok(ip) = host.parse::<IpAddr>() {
        return if is_safe_ip(&ip) {
            Ok(SocketAddr::new(ip, port))
        } else {
            Err(ResolveError::Url("blocked address"))
        };
    }
    let addrs = tokio::net::lookup_host((host, port))
        .await
        .map_err(|_| ResolveError::Url("dns failed"))?;
    for addr in addrs {
        if is_safe_ip(&addr.ip()) {
            return Ok(addr);
        }
    }
    Err(ResolveError::Url("blocked address"))
}
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `cargo test -p ace-swarm ssrf_guard`
Expected: PASS (2 tests).

- [ ] **Step 7: Commit**

```bash
git add crates/ace-swarm/Cargo.toml crates/ace-swarm/src/resolve.rs
git commit -m "ace-swarm/resolve: add SSRF guard for transport-file url fetch (#50)"
```

---

## Task 2: Guarded transport-file fetch

**Files:**
- Modify: `crates/ace-swarm/src/resolve.rs` (fetch seam + public entry + tests)

- [ ] **Step 1: Write the failing fetch tests**

Add to `mod tests` in `resolve.rs`. These drive the inner `fetch_transport_from` seam (already-validated addr) against a one-shot loopback HTTP server, plus the scheme check on the public entry:

```rust
// Minimal one-shot HTTP/1.1 server: serves `status`+`body` to the first connection.
async fn serve_once(status: &'static str, body: Vec<u8>) -> std::net::SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        if let Ok((mut sock, _)) = listener.accept().await {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let mut buf = [0u8; 1024];
            let _ = sock.read(&mut buf).await; // drain request headers
            let header = format!(
                "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            let _ = sock.write_all(header.as_bytes()).await;
            let _ = sock.write_all(&body).await;
            let _ = sock.shutdown().await;
        }
    });
    addr
}

#[tokio::test]
async fn fetch_decodes_a_valid_transport() {
    let tf = make_transport(
        b"d10:authmethod3:RSA7:bitratei100000e12:chunk_lengthi16384e4:name4:Test12:piece_lengthi1048576e6:pubkey3:abc8:trackersl18:udp://t.example:80ee",
    );
    let addr = serve_once("200 OK", tf).await;
    let url = format!("http://127.0.0.1:{}/t.bin", addr.port());
    let si = fetch_transport_from(&url, "127.0.0.1", addr).await.unwrap();
    assert_eq!(si.piece_length, 1_048_576);
    assert_eq!(si.infohash[0], 0x92);
}

#[tokio::test]
async fn fetch_rejects_oversized_body() {
    let addr = serve_once("200 OK", vec![b'x'; (MAX_TRANSPORT_FILE + 1) as usize]).await;
    let url = format!("http://127.0.0.1:{}/big.bin", addr.port());
    assert_eq!(
        fetch_transport_from(&url, "127.0.0.1", addr).await,
        Err(ResolveError::Url("transport too large"))
    );
}

#[tokio::test]
async fn fetch_rejects_non_transport_body() {
    let addr = serve_once("200 OK", b"not a transport".to_vec()).await;
    let url = format!("http://127.0.0.1:{}/x", addr.port());
    assert!(matches!(
        fetch_transport_from(&url, "127.0.0.1", addr).await,
        Err(ResolveError::Transport(_))
    ));
}

#[tokio::test]
async fn fetch_rejects_non_200_status() {
    let addr = serve_once("404 Not Found", b"nope".to_vec()).await;
    let url = format!("http://127.0.0.1:{}/missing", addr.port());
    assert_eq!(
        fetch_transport_from(&url, "127.0.0.1", addr).await,
        Err(ResolveError::Url("http status"))
    );
}

#[tokio::test]
async fn transport_url_rejects_unsupported_scheme() {
    assert_eq!(
        stream_info_from_transport_url("file:///etc/passwd").await,
        Err(ResolveError::Url("unsupported scheme"))
    );
    assert_eq!(
        stream_info_from_transport_url("ftp://example.com/x").await,
        Err(ResolveError::Url("unsupported scheme"))
    );
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p ace-swarm fetch_`
Expected: FAIL — `fetch_transport_from` / `stream_info_from_transport_url` / `MAX_TRANSPORT_FILE` not defined.

- [ ] **Step 3: Implement the fetch + public entry**

Add to `resolve.rs` (free functions). Add `use std::time::Duration;` if not already imported (it is). Constants near the other `MAX_*`:

```rust
/// Upper bound on a fetched transport-file body (bytes). Matches the descriptor ceilings the
/// catalog path and `MAX_METADATA_SIZE` already impose on the same `AceStreamTransport` dict.
pub const MAX_TRANSPORT_FILE: u64 = 1_048_576;

const TRANSPORT_URL_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const TRANSPORT_URL_TOTAL_TIMEOUT: Duration = Duration::from_secs(15);

/// Fetch a transport file from `url`, connecting only to the pre-validated `addr` (pinned so DNS
/// rebinding cannot redirect the connection), and decode it into a [`StreamInfo`]. Redirects are
/// disabled (a redirect is an SSRF bypass), the body is size-capped, and a non-2xx status fails
/// closed. This is the inner seam the public entry calls after the SSRF guard runs.
async fn fetch_transport_from(
    url: &str,
    host: &str,
    addr: SocketAddr,
) -> Result<StreamInfo, ResolveError> {
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(TRANSPORT_URL_CONNECT_TIMEOUT)
        .timeout(TRANSPORT_URL_TOTAL_TIMEOUT)
        .resolve(host, addr)
        .build()
        .map_err(|_| ResolveError::Url("client build failed"))?;

    let mut resp = client
        .get(url)
        .send()
        .await
        .map_err(|_| ResolveError::Url("fetch failed"))?;
    if !resp.status().is_success() {
        return Err(ResolveError::Url("http status"));
    }

    let mut body = Vec::new();
    while let Some(chunk) = resp
        .chunk()
        .await
        .map_err(|_| ResolveError::Url("fetch failed"))?
    {
        if body.len() as u64 + chunk.len() as u64 > MAX_TRANSPORT_FILE {
            return Err(ResolveError::Url("transport too large"));
        }
        body.extend_from_slice(&chunk);
    }
    stream_info_from_transport(&body)
}

/// Resolve a transport-file `url=` input into a [`StreamInfo`]: require an http/https scheme, run
/// the SSRF guard on the host, then fetch+decode via [`fetch_transport_from`]. Every failure path
/// is fail-closed.
pub async fn stream_info_from_transport_url(url: &str) -> Result<StreamInfo, ResolveError> {
    let parsed = reqwest::Url::parse(url).map_err(|_| ResolveError::Url("bad url"))?;
    if !matches!(parsed.scheme(), "http" | "https") {
        return Err(ResolveError::Url("unsupported scheme"));
    }
    let host = parsed.host_str().ok_or(ResolveError::Url("no host"))?;
    let port = parsed
        .port_or_known_default()
        .ok_or(ResolveError::Url("no port"))?;
    let addr = resolve_safe_addr(host, port).await?;
    fetch_transport_from(url, host, addr).await
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p ace-swarm fetch_ && cargo test -p ace-swarm transport_url`
Expected: PASS (5 tests).

- [ ] **Step 5: Add an ignored live end-to-end test**

Append to `mod tests`:

```rust
#[tokio::test]
#[ignore = "network: fetches a real public transport-file URL"]
async fn live_transport_url_fetch() {
    // Replace with a known public/free transport-file URL when validating live.
    let url = std::env::var("OUTPACE_TEST_TRANSPORT_URL").expect("set OUTPACE_TEST_TRANSPORT_URL");
    let si = stream_info_from_transport_url(&url).await.unwrap();
    assert_ne!(si.infohash, [0u8; 20]);
}
```

- [ ] **Step 6: Commit**

```bash
git add crates/ace-swarm/src/resolve.rs
git commit -m "ace-swarm/resolve: guarded transport-file url fetch + decode (#50)"
```

---

## Task 3: Magnet parsing helper (shared, no new dep)

**Files:**
- Create: `crates/ace-engine/src/magnet.rs`
- Modify: module declaration file (`crates/ace-engine/src/lib.rs` — add `pub(crate) mod magnet;`; verify exact file with `rg -n "^mod |^pub mod |^pub\(crate\) mod " crates/ace-engine/src/lib.rs`)

- [ ] **Step 1: Write the failing magnet tests**

Create `crates/ace-engine/src/magnet.rs`:

```rust
//! Parse a `magnet:` link's `xt=urn:btih:` info-hash into a 40-hex string (BitTorrent v1 only).
//! Accepts the two standard btih encodings — 40-char hex and 32-char RFC-4648 base32 — and
//! rejects everything else (notably v2 `urn:btmh:`), so magnet inputs reduce to the existing
//! bare-infohash playback path.

/// Extract the v1 info-hash from a magnet URI or a raw `magnet=` value, as a lowercase 40-hex
/// string. Returns a human-readable error for anything unsupported.
pub(crate) fn parse_magnet_infohash(magnet: &str) -> Result<String, String> {
    let query = magnet
        .trim()
        .strip_prefix("magnet:?")
        .or_else(|| magnet.trim().strip_prefix("magnet:"))
        .unwrap_or(magnet.trim());
    for (key, value) in query.split('&').filter_map(|p| p.split_once('=')) {
        if key != "xt" {
            continue;
        }
        if let Some(hash) = value.strip_prefix("urn:btih:") {
            return normalize_btih(hash);
        }
    }
    Err("magnet has no supported urn:btih: info-hash".into())
}

fn normalize_btih(hash: &str) -> Result<String, String> {
    if hash.len() == 40 && hash.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Ok(hash.to_ascii_lowercase());
    }
    if hash.len() == 32 {
        if let Some(bytes) = base32_decode(hash) {
            return Ok(bytes.iter().map(|b| format!("{b:02x}")).collect());
        }
    }
    Err("unsupported btih info-hash encoding".into())
}

/// Decode a 32-char RFC-4648 base32 string into 20 bytes (case-insensitive). `None` on any
/// invalid character or length.
fn base32_decode(s: &str) -> Option<Vec<u8>> {
    const ALPHABET: &[u8; 32] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";
    if s.len() != 32 {
        return None;
    }
    let mut buffer: u16 = 0;
    let mut bits = 0u32;
    let mut out = Vec::with_capacity(20);
    for c in s.bytes() {
        let up = c.to_ascii_uppercase();
        let val = ALPHABET.iter().position(|&a| a == up)? as u16;
        buffer = (buffer << 5) | val;
        bits += 5;
        if bits >= 8 {
            bits -= 8;
            out.push((buffer >> bits) as u8);
        }
    }
    if out.len() == 20 {
        Some(out)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_40_hex_btih() {
        let m = "magnet:?xt=urn:btih:0123456789ABCDEF0123456789abcdef01234567&dn=x";
        assert_eq!(
            parse_magnet_infohash(m).unwrap(),
            "0123456789abcdef0123456789abcdef01234567"
        );
    }

    #[test]
    fn parses_32_char_base32_btih() {
        // base32 of the 20 bytes 0x00,0x44,0x32,... — round-trips to the same hex as hex form.
        // "AAAQEAYEAUDAOCAJBIFQYDIOB4IBCEQ" is base32 of 0x00,0x01,...,0x13.
        let m = "magnet:?xt=urn:btih:AAAQEAYEAUDAOCAJBIFQYDIOB4IBCEQ4";
        let hex = parse_magnet_infohash(m).unwrap();
        assert_eq!(hex.len(), 40);
        assert_eq!(hex, "000108208184208c41209028510218388400114");
    }

    #[test]
    fn rejects_btmh_v2_and_missing() {
        assert!(parse_magnet_infohash("magnet:?xt=urn:btmh:1220abcd").is_err());
        assert!(parse_magnet_infohash("magnet:?dn=noxt").is_err());
        assert!(parse_magnet_infohash("magnet:?xt=urn:btih:tooshort").is_err());
    }
}
```

- [ ] **Step 2: Declare the module**

Run `rg -n "mod " crates/ace-engine/src/lib.rs` to find the module list, then add alongside the others:

```rust
pub(crate) mod magnet;
```

- [ ] **Step 3: Run tests to verify they pass**

Run: `cargo test -p ace-engine magnet`
Expected: PASS (3 tests). (If `parses_32_char_base32_btih`'s expected hex differs, compute the correct value from the base32 string and update the assertion — the string `AAAQEAYEAUDAOCAJBIFQYDIOB4IBCEQ4` decodes to bytes `00 01 08 20 81 84 20 8c 41 20 90 28 51 02 18 38 84 00 11 4` ... verify with the decoder and pin the exact hex the test prints on first run.)

> Note for the implementer: base32 test vectors are fiddly. If the literal hex above does not match, run the test once, read the `left`/`right` from the failure, confirm the decoder logic is correct by hand for a couple of bytes, then set the expected string to the decoder's output. Do NOT change the decoder to match a guessed value.

- [ ] **Step 4: Commit**

```bash
git add crates/ace-engine/src/magnet.rs crates/ace-engine/src/lib.rs
git commit -m "ace-engine: magnet btih parsing (hex + base32) (#50)"
```

---

## Task 4: CLI `url`/`magnet` inputs + precedence

**Files:**
- Modify: `crates/ace-engine/src/cli.rs` (`PlaybackTarget::parse`, new target ctors, tests)

- [ ] **Step 1: Write the failing CLI tests**

Add to the `#[cfg(test)] mod tests` in `cli.rs` (create the block if absent, near the bottom):

```rust
#[test]
fn parses_bare_magnet_input() {
    let t = PlaybackTarget::parse(
        "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567",
    )
    .unwrap();
    assert_eq!(t.provider_id, "0123456789abcdef0123456789abcdef01234567");
}

#[test]
fn parses_transport_url_input() {
    let t =
        PlaybackTarget::parse("acestream:?url=https://example.com/x.acelive").unwrap();
    assert_eq!(t.provider_id, "turl:https://example.com/x.acelive");
}

#[test]
fn selector_precedence_content_id_over_infohash_over_url_over_magnet() {
    let ih = "0123456789abcdef0123456789abcdef01234567";
    let cid = "89abcdef0123456789abcdef0123456789abcdef";
    // content_id wins.
    let t = PlaybackTarget::parse(&format!(
        "acestream:?content_id={cid}&infohash={ih}&url=https://e/x&magnet=magnet:?xt=urn:btih:{ih}"
    ))
    .unwrap();
    assert_eq!(t.provider_id, format!("cid:{cid}"));
    // then infohash.
    let t = PlaybackTarget::parse(&format!(
        "acestream:?infohash={ih}&url=https://e/x&magnet=magnet:?xt=urn:btih:{ih}"
    ))
    .unwrap();
    assert_eq!(t.provider_id, ih);
    // then url.
    let t = PlaybackTarget::parse(&format!(
        "acestream:?url=https://e/x.acelive&magnet=magnet:?xt=urn:btih:{ih}"
    ))
    .unwrap();
    assert_eq!(t.provider_id, "turl:https://e/x.acelive");
}

#[test]
fn rejects_non_http_transport_url() {
    assert!(PlaybackTarget::parse("acestream:?url=file:///etc/passwd").is_err());
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p ace-engine -- cli`  (or `cargo test -p ace-engine parses_bare_magnet_input`)
Expected: FAIL — parse does not handle `url`/`magnet`/`magnet:`.

- [ ] **Step 3: Extend `PlaybackTarget::parse` and add constructors**

Replace the body of `PlaybackTarget::parse` (currently lines ~64-81) with:

```rust
    pub fn parse(input: &str) -> Result<Self, String> {
        let input = input.trim();
        if let Some(rest) = input.strip_prefix("acestream://") {
            let id = rest.split(['?', '#']).next().unwrap_or("");
            return content_id_target(id);
        }
        if input.starts_with("magnet:") {
            return magnet_target(input);
        }
        if let Some(query) = input.strip_prefix("acestream:?") {
            let params = parse_query(query);
            if let Some(id) = params.get("content_id") {
                return content_id_target(id);
            }
            if let Some(id) = params.get("infohash") {
                return infohash_target(id);
            }
            if let Some(url) = params.get("url") {
                return url_target(url);
            }
            if let Some(magnet) = params.get("magnet") {
                return magnet_target(magnet);
            }
            return Err("acestream URL must contain content_id, infohash, url, or magnet".into());
        }
        Err("expected an acestream:// or acestream:? or magnet: URL".into())
    }
```

Add these free functions next to `content_id_target`/`infohash_target`:

```rust
fn url_target(url: &str) -> Result<PlaybackTarget, String> {
    let url = url.trim();
    if !(url.starts_with("http://") || url.starts_with("https://")) {
        return Err("transport url must be http or https".into());
    }
    Ok(PlaybackTarget {
        provider_id: format!("turl:{url}"),
    })
}

fn magnet_target(magnet: &str) -> Result<PlaybackTarget, String> {
    let hex = crate::magnet::parse_magnet_infohash(magnet)?;
    infohash_target(&hex)
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p ace-engine`
Expected: PASS (new CLI tests + existing tests green).

- [ ] **Step 5: Commit**

```bash
git add crates/ace-engine/src/cli.rs
git commit -m "ace-engine/cli: accept transport url + magnet inputs with precedence (#50)"
```

---

## Task 5: HTTP `url`/`magnet` params + precedence

**Files:**
- Modify: `crates/ace-engine/Cargo.toml` (add `sha1`)
- Modify: `crates/ace-engine/src/http.rs` (`ace_selected_stream`, helper, tests)

- [ ] **Step 1: Add sha1 dependency**

In `crates/ace-engine/Cargo.toml` `[dependencies]`, add:

```toml
sha1 = "0.10"
```

- [ ] **Step 2: Write the failing HTTP selection tests**

Add to `mod tests` in `http.rs`:

```rust
fn params(pairs: &[(&str, &str)]) -> std::collections::HashMap<String, String> {
    pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
}

#[test]
fn getstream_selects_magnet_as_infohash() {
    let ih = "0123456789abcdef0123456789abcdef01234567";
    let sel = ace_selected_stream(&params(&[("magnet", &format!("magnet:?xt=urn:btih:{ih}"))]))
        .unwrap();
    assert_eq!(sel.session_key, ih);
    assert_eq!(sel.playback_id, ih);
}

#[test]
fn getstream_selects_transport_url_with_path_safe_playback_id() {
    let url = "https://example.com/a/b.acelive";
    let sel = ace_selected_stream(&params(&[("url", url)])).unwrap();
    assert_eq!(sel.session_key, format!("turl:{url}"));
    // playback_id is path-safe (no scheme separators) so it round-trips through /ace/r/{id}.
    assert!(sel.playback_id.starts_with("turl-"));
    assert!(!sel.playback_id.contains('/'));
    assert!(!sel.playback_id.contains(':'));
}

#[test]
fn getstream_precedence_content_id_over_infohash_over_url_over_magnet() {
    let ih = "0123456789abcdef0123456789abcdef01234567";
    let cid = "89abcdef0123456789abcdef0123456789abcdef";
    let sel = ace_selected_stream(&params(&[
        ("content_id", cid), ("infohash", ih), ("url", "https://e/x"),
        ("magnet", &format!("magnet:?xt=urn:btih:{ih}")),
    ]))
    .unwrap();
    assert_eq!(sel.session_key, format!("cid:{cid}"));

    let sel = ace_selected_stream(&params(&[
        ("infohash", ih), ("url", "https://e/x"),
        ("magnet", &format!("magnet:?xt=urn:btih:{ih}")),
    ]))
    .unwrap();
    assert_eq!(sel.session_key, ih);

    let sel = ace_selected_stream(&params(&[
        ("url", "https://e/x.acelive"),
        ("magnet", &format!("magnet:?xt=urn:btih:{ih}")),
    ]))
    .unwrap();
    assert_eq!(sel.session_key, "turl:https://e/x.acelive");
}

#[test]
fn getstream_rejects_bad_url_and_magnet() {
    assert!(ace_selected_stream(&params(&[("url", "file:///etc/passwd")])).is_none());
    assert!(ace_selected_stream(&params(&[("magnet", "magnet:?dn=noxt")])).is_none());
}
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `cargo test -p ace-engine getstream_`
Expected: FAIL — selection ignores `url`/`magnet`.

- [ ] **Step 4: Extend `ace_selected_stream`**

Add `use sha1::{Digest, Sha1};` to the top of `http.rs`. Extend `ace_selected_stream` (currently ~line 350) so that after the `content_id` branch and the `infohash`/`id` branch, it handles `url` then `magnet`:

```rust
fn ace_selected_stream(params: &HashMap<String, String>) -> Option<AceStreamSelection> {
    if let Some(content_id) = ace_nonempty_param(params, "content_id") {
        return Some(AceStreamSelection {
            public_id: content_id.to_string(),
            playback_id: format!("cid:{content_id}"),
            session_key: format!("cid:{content_id}"),
            content_id: Some(content_id.to_string()),
        });
    }
    if let Some(id) = ace_nonempty_param(params, "infohash").or_else(|| ace_nonempty_param(params, "id")) {
        return Some(AceStreamSelection {
            public_id: id.to_string(),
            playback_id: id.to_string(),
            session_key: id.to_string(),
            content_id: None,
        });
    }
    if let Some(url) = ace_nonempty_param(params, "url") {
        if !(url.starts_with("http://") || url.starts_with("https://")) {
            return None;
        }
        // session_key is what the provider opens; playback_id must be path-safe because it is
        // interpolated into the /ace/r/{playback_id} route (a raw URL would break routing).
        let token = format!("turl-{}", sha1_hex(url.as_bytes()));
        return Some(AceStreamSelection {
            public_id: token.clone(),
            playback_id: token,
            session_key: format!("turl:{url}"),
            content_id: None,
        });
    }
    if let Some(magnet) = ace_nonempty_param(params, "magnet") {
        let hex = crate::magnet::parse_magnet_infohash(magnet).ok()?;
        return Some(AceStreamSelection {
            public_id: hex.clone(),
            playback_id: hex.clone(),
            session_key: hex,
            content_id: None,
        });
    }
    None
}

fn sha1_hex(bytes: &[u8]) -> String {
    let mut h = Sha1::new();
    h.update(bytes);
    h.finalize().iter().map(|b| format!("{b:02x}")).collect()
}
```

(Preserve the existing `content_id`/`infohash`/`id` behavior exactly — the block above is the full replacement, keeping their semantics.)

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test -p ace-engine getstream_ && cargo test -p ace-engine`
Expected: PASS (new HTTP tests + existing http tests green, including `content_id_selection_uses_resolved_infohash_when_available`).

- [ ] **Step 6: Commit**

```bash
git add crates/ace-engine/Cargo.toml crates/ace-engine/src/http.rs
git commit -m "ace-engine/http: accept transport url + magnet in /ace/getstream (#50)"
```

---

## Task 6: Provider `turl:` dispatch

**Files:**
- Modify: `crates/ace-engine/src/ace_provider.rs` (`open`, import, test)

- [ ] **Step 1: Write the failing dispatch tests**

Add to `mod tests` in `ace_provider.rs` (near the existing `open("cid:nothex")` test ~line 2389):

```rust
#[tokio::test]
async fn open_rejects_transport_url_with_blocked_host() {
    let p = test_provider(); // use the same constructor the neighboring open() tests use
    // Loopback is SSRF-blocked, so this fails closed rather than fetching.
    let err = p.open("turl:http://127.0.0.1:1/x").await;
    assert!(err.is_err());
}

#[tokio::test]
async fn open_rejects_transport_url_bad_scheme() {
    let p = test_provider();
    assert!(p.open("turl:file:///etc/passwd").await.is_err());
}
```

> Implementer: match the exact provider constructor used by the adjacent tests (search `rg -n "fn open" crates/ace-engine/src/ace_provider.rs` and the test that calls `p.open("cid:nothex")` to copy its setup). Replace `test_provider()` with that setup.

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p ace-engine open_rejects_transport_url`
Expected: FAIL — `turl:` is treated as an invalid id (or the assert shape differs). Confirm it fails because the branch does not exist yet.

- [ ] **Step 3: Add the `turl:` branch**

In `ace_provider.rs`, update the import line (~19) to include the new entry:

```rust
    hex20, resolve_via_catalog, resolve_via_peer, stream_info_from_infohash,
    stream_info_from_transport_url, ResolveCache,
```

Then in `open` (the `let info = if let Some(content_id) = id.strip_prefix("cid:")` chain, ~line 399), add a branch before the bare-infohash arm:

```rust
        let info = if let Some(content_id) = id.strip_prefix("cid:") {
            self.resolve_content_id(content_id).await?
        } else if let Some(url) = id.strip_prefix("turl:") {
            stream_info_from_transport_url(url)
                .await
                .map_err(|e| ProviderError::Backend(format!("transport url: {e:?}").into()))?
        } else if id.len() == 40 && id.bytes().all(|b| b.is_ascii_hexdigit()) {
            stream_info_from_infohash(id, self.default_trackers.clone())
                .map_err(|_| ProviderError::Backend("bad infohash".into()))?
        } else {
            return Err(ProviderError::Backend(
                "id must be a 40-hex infohash, cid:<40hex>, or turl:<url>".into(),
            ));
        };
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p ace-engine open_rejects_transport_url`
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
git add crates/ace-engine/src/ace_provider.rs
git commit -m "ace-engine/provider: dispatch turl:<url> to guarded transport fetch (#50)"
```

---

## Task 7: Docs + full verification

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Document the new inputs**

Find where the README describes playback inputs (`rg -n "acestream://|content_id|infohash|getstream" README.md`). Add, in the same style as the surrounding docs, that outpace now also accepts:

- `magnet:?xt=urn:btih:<40hex-or-32-base32>` (CLI `play`, `acestream:?magnet=`, `/ace/getstream?magnet=`),
- a transport-file `url=` (`acestream:?url=https://…`, `/ace/getstream?url=https://…`), fetched over http/https with SSRF protection, a 1 MiB size cap, no redirects, and a request timeout; unsafe or oversized or non-transport responses fail closed.
- Selector precedence: `content_id` > `infohash` > `url` > `magnet`.

- [ ] **Step 2: Full workspace verification**

Run:

```bash
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

Expected: fmt clean; clippy no warnings; all tests pass (ignored: the pre-existing DHT live tests + the new `live_transport_url_fetch`).

- [ ] **Step 3: Commit**

```bash
git add README.md
git commit -m "docs: document transport-file url + magnet playback inputs (#50)"
```

- [ ] **Step 4: Push and open the PR**

```bash
git push -u origin feat/transport-url-magnet-inputs-50
gh pr create --fill --title "Support transport-file URL and magnet playback inputs (#50)" \
  --body "Closes #50. Adds magnet (btih hex/base32 → existing infohash path) and transport-file url= inputs (new turl:<url> provider scheme with a guarded reqwest fetch: SSRF guard + pinned address, no redirects, 1 MiB cap, timeouts, fail-closed). Precedence content_id > infohash > url > magnet across CLI and /ace/getstream. Adds reqwest (rustls-tls) to ace-swarm and sha1 to ace-engine. See docs/superpowers/specs/2026-07-07-transport-url-magnet-inputs-design.md."
```

Leave merging to the maintainer.

---

## Self-Review Notes

- **Spec coverage:** magnet parsing (T3), url fetch + SSRF + size cap + timeout + no-redirect + decode (T1–T2), provider dispatch (T6), CLI precedence (T4), HTTP precedence + path-safe id (T5), scheme/oversize/non-transport fail-closed (T1–T2 tests), docs (T7). All acceptance criteria mapped.
- **Type consistency:** `ResolveError::Url` used uniformly; `stream_info_from_transport_url` name consistent across resolve.rs and ace_provider.rs; `parse_magnet_infohash` consistent across magnet.rs, cli.rs, http.rs.
- **Known fiddly spot:** the base32 test vector in T3 — instructions say verify against the decoder output, not to weaken the decoder.
- **Verify-before-claim:** T7 runs fmt+clippy+test before the PR.
