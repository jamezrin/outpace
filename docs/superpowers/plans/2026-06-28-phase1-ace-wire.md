# Phase 1 — `ace-wire` Foundation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `ace-wire`, the pure (no-I/O) protocol-primitives crate for the Acestream/BitTorrent wire: infohash math, bencode, the `AceStreamProtocol` peer handshake, BitTorrent message framing, and the BEP-10 extended handshake — all validated against the Phase-0 capture vectors.

**Architecture:** A Rust **Cargo workspace** at the repo root with one library crate `crates/ace-wire`. The crate is pure functions/types (no sockets, no async) so it can be exhaustively unit-tested and later consumed by `ace-peer`/`ace-tracker`. Each protocol concern is one focused module with a clear encode/decode interface. Validated against committed binary vectors in `tests/vectors/`.

**Tech Stack:** Rust 2021, `sha1` (infohash), `rand` (peer_id), hand-rolled minimal bencode (no serde — we need raw byte-string dict keys). `hex` (dev-dependency, tests only).

**Spec references (already on `main`):** `docs/protocol/wire-protocol.md`, `docs/protocol/transport-file.md`. Ground-truth vectors:
- `tests/vectors/transport-01.bin` → infohash `34df422b80a4bd94ac1e51be9ede60364ec7a7dd`
- `tests/vectors/transport-02.bin` → infohash `ed2c05b3b022e9cc7b7c1ca46d20f10839dc4108`
- `tests/vectors/messages/encrypter-handshake-peer-in.bin` → 66-byte handshake: pstr `AceStreamProtocol`, infohash `50e93529d3eb46a50506b14464185a15292d6e47`, peer_id ASCII `R30------Ef2V8QOgmt4`.

---

## File / Directory Map

| Path | Responsibility |
|---|---|
| `Cargo.toml` (root) | Workspace manifest listing `crates/*` |
| `crates/ace-wire/Cargo.toml` | Crate manifest + deps |
| `crates/ace-wire/src/lib.rs` | Re-exports + `WireError` |
| `crates/ace-wire/src/infohash.rs` | `infohash_of_transport`, `is_transport_file` |
| `crates/ace-wire/src/bencode.rs` | `Bencode` value type: `parse` + `encode` |
| `crates/ace-wire/src/handshake.rs` | `Handshake` encode/decode + `random_peer_id` |
| `crates/ace-wire/src/message.rs` | `PeerMessage` enum + frame encode/decode |
| `crates/ace-wire/src/extended.rs` | BEP-10 `ExtendedHandshake` parse |
| `crates/ace-wire/tests/vectors.rs` | Integration tests vs `tests/vectors/` |

The crate reads the repo-root vectors via a path relative to `CARGO_MANIFEST_DIR` (`../../tests/vectors/...`).

---

## Task 1: Workspace + crate skeleton

**Files:**
- Create: `Cargo.toml`, `crates/ace-wire/Cargo.toml`, `crates/ace-wire/src/lib.rs`

- [ ] **Step 1: Create the workspace manifest**

Create `Cargo.toml` (repo root):
```toml
[workspace]
members = ["crates/ace-wire"]
resolver = "2"
```

- [ ] **Step 2: Create the crate manifest**

Create `crates/ace-wire/Cargo.toml`:
```toml
[package]
name = "ace-wire"
version = "0.1.0"
edition = "2021"

[dependencies]
sha1 = "0.10"
rand = "0.8"

[dev-dependencies]
hex = "0.4"
```

- [ ] **Step 3: Create the crate root with the shared error type**

Create `crates/ace-wire/src/lib.rs`:
```rust
//! ace-wire: pure protocol primitives for the Acestream (BitTorrent/BitTornado) wire.
//! No I/O. Encode/decode only. See docs/protocol/wire-protocol.md.

pub mod bencode;
pub mod extended;
pub mod handshake;
pub mod infohash;
pub mod message;

/// Errors produced while decoding untrusted wire bytes.
#[derive(Debug, PartialEq, Eq)]
pub enum WireError {
    /// Buffer ended before a complete structure was parsed.
    Truncated,
    /// Bytes did not match the expected format.
    Invalid(&'static str),
}

pub type Result<T> = std::result::Result<T, WireError>;
```

- [ ] **Step 4: Verify it builds**

Run: `cargo build -p ace-wire`
Expected: compiles. (Modules are declared but empty files do not yet exist; create empty module files so it builds.)

Create empty files so the `mod` declarations resolve:
`crates/ace-wire/src/bencode.rs`, `extended.rs`, `handshake.rs`, `infohash.rs`, `message.rs` (each containing only a `// placeholder` line for now).

Run: `cargo build -p ace-wire`
Expected: PASS (builds with warnings about unused).

- [ ] **Step 5: Commit**

```bash
git add Cargo.toml crates/ace-wire/
git commit -m "ace-wire: workspace + crate skeleton"
```

---

## Task 2: `infohash` module

**Files:**
- Modify: `crates/ace-wire/src/infohash.rs`
- Test: `crates/ace-wire/tests/vectors.rs`

- [ ] **Step 1: Write the failing test**

Create `crates/ace-wire/tests/vectors.rs`:
```rust
use ace_wire::infohash::{infohash_of_transport, is_transport_file};
use std::path::PathBuf;

fn vec_bytes(rel: &str) -> Vec<u8> {
    let p = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../tests/vectors").join(rel);
    std::fs::read(&p).unwrap_or_else(|e| panic!("read {:?}: {e}", p))
}

#[test]
fn infohash_matches_engine_ground_truth() {
    assert_eq!(hex::encode(infohash_of_transport(&vec_bytes("transport-01.bin"))),
               "34df422b80a4bd94ac1e51be9ede60364ec7a7dd");
    assert_eq!(hex::encode(infohash_of_transport(&vec_bytes("transport-02.bin"))),
               "ed2c05b3b022e9cc7b7c1ca46d20f10839dc4108");
}

#[test]
fn detects_transport_magic() {
    assert!(is_transport_file(&vec_bytes("transport-01.bin")));
    assert!(!is_transport_file(b"not a transport file"));
    assert!(!is_transport_file(b"AceStream")); // too short / partial
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p ace-wire --test vectors`
Expected: FAIL — `infohash_of_transport`/`is_transport_file` not found.

- [ ] **Step 3: Implement**

Write `crates/ace-wire/src/infohash.rs` (note: the transport magic is
`AceStreamTransport` — distinct from the handshake pstr `AceStreamProtocol`):
```rust
//! Acestream identifier math. infohash = SHA1(entire transport file bytes).
//! See docs/protocol/transport-file.md (validated against engine ground truth).

use sha1::{Digest, Sha1};

/// The magic header every Acestream transport file starts with.
pub const TRANSPORT_MAGIC: &[u8] = b"AceStreamTransport";

/// The BitTorrent infohash of an Acestream transport = SHA1 of the whole file,
/// including the `AceStreamTransport` magic.
pub fn infohash_of_transport(bytes: &[u8]) -> [u8; 20] {
    let mut h = Sha1::new();
    h.update(bytes);
    h.finalize().into()
}

/// True iff `bytes` begins with the Acestream transport magic.
pub fn is_transport_file(bytes: &[u8]) -> bool {
    bytes.starts_with(TRANSPORT_MAGIC)
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p ace-wire --test vectors`
Expected: PASS (both tests).

- [ ] **Step 5: Commit**

```bash
git add crates/ace-wire/src/infohash.rs crates/ace-wire/tests/vectors.rs
git commit -m "ace-wire: infohash = SHA1(transport file), validated vs vectors"
```

---

## Task 3: `bencode` module

**Files:**
- Modify: `crates/ace-wire/src/bencode.rs`
- Test: `crates/ace-wire/src/bencode.rs` (inline `#[cfg(test)]`)

- [ ] **Step 1: Write the failing test (inline)**

Put this at the bottom of `crates/ace-wire/src/bencode.rs`:
```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_simple() {
        // d3:bar4:spam3:fooi42ee  -> {bar: "spam", foo: 42}
        let raw = b"d3:bar4:spam3:fooi42ee";
        let v = Bencode::parse(raw).unwrap();
        let mut d = std::collections::BTreeMap::new();
        d.insert(b"bar".to_vec(), Bencode::Bytes(b"spam".to_vec()));
        d.insert(b"foo".to_vec(), Bencode::Int(42));
        assert_eq!(v, Bencode::Dict(d));
        assert_eq!(v.encode(), raw); // canonical re-encode (keys sorted)
    }

    #[test]
    fn parses_list_and_negative_int() {
        let v = Bencode::parse(b"li-3e1:ae").unwrap();
        assert_eq!(v, Bencode::List(vec![Bencode::Int(-3), Bencode::Bytes(b"a".to_vec())]));
    }

    #[test]
    fn rejects_trailing_and_truncated() {
        assert!(Bencode::parse(b"i42").is_err());      // truncated
        assert!(Bencode::parse(b"i42eX").is_err());     // trailing byte
        assert!(Bencode::parse(b"3:ab").is_err());      // short string
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p ace-wire bencode`
Expected: FAIL — `Bencode` not found.

- [ ] **Step 3: Implement**

Replace `crates/ace-wire/src/bencode.rs` (keeping the test module at the bottom):
```rust
//! Minimal bencode (BitTorrent encoding). Byte-string keys; canonical encode.
use crate::{Result, WireError};
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Bencode {
    Int(i64),
    Bytes(Vec<u8>),
    List(Vec<Bencode>),
    Dict(BTreeMap<Vec<u8>, Bencode>),
}

impl Bencode {
    /// Parse exactly one bencode value from the whole buffer; trailing bytes = error.
    pub fn parse(buf: &[u8]) -> Result<Bencode> {
        let (v, n) = parse_value(buf, 0)?;
        if n != buf.len() { return Err(WireError::Invalid("trailing bytes")); }
        Ok(v)
    }

    /// Parse one value from the front; return (value, bytes_consumed).
    pub fn parse_prefix(buf: &[u8]) -> Result<(Bencode, usize)> {
        parse_value(buf, 0)
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        self.encode_into(&mut out);
        out
    }

    fn encode_into(&self, out: &mut Vec<u8>) {
        match self {
            Bencode::Int(i) => { out.push(b'i'); out.extend_from_slice(i.to_string().as_bytes()); out.push(b'e'); }
            Bencode::Bytes(b) => { out.extend_from_slice(b.len().to_string().as_bytes()); out.push(b':'); out.extend_from_slice(b); }
            Bencode::List(l) => { out.push(b'l'); for e in l { e.encode_into(out); } out.push(b'e'); }
            Bencode::Dict(d) => { out.push(b'd'); for (k, v) in d { Bencode::Bytes(k.clone()).encode_into(out); v.encode_into(out); } out.push(b'e'); }
        }
    }

    /// Convenience: borrow a dict entry.
    pub fn get<'a>(&'a self, key: &[u8]) -> Option<&'a Bencode> {
        match self { Bencode::Dict(d) => d.get(key), _ => None }
    }
    pub fn as_int(&self) -> Option<i64> { if let Bencode::Int(i) = self { Some(*i) } else { None } }
    pub fn as_bytes(&self) -> Option<&[u8]> { if let Bencode::Bytes(b) = self { Some(b) } else { None } }
}

fn parse_value(buf: &[u8], pos: usize) -> Result<(Bencode, usize)> {
    match buf.get(pos).ok_or(WireError::Truncated)? {
        b'i' => parse_int(buf, pos),
        b'l' => parse_list(buf, pos),
        b'd' => parse_dict(buf, pos),
        b'0'..=b'9' => parse_bytes(buf, pos),
        _ => Err(WireError::Invalid("unexpected bencode token")),
    }
}

fn parse_int(buf: &[u8], pos: usize) -> Result<(Bencode, usize)> {
    // buf[pos] == 'i'
    let end = buf[pos + 1..].iter().position(|&b| b == b'e').ok_or(WireError::Truncated)? + pos + 1;
    let s = std::str::from_utf8(&buf[pos + 1..end]).map_err(|_| WireError::Invalid("int utf8"))?;
    let i = s.parse::<i64>().map_err(|_| WireError::Invalid("int parse"))?;
    Ok((Bencode::Int(i), end + 1))
}

fn parse_bytes(buf: &[u8], pos: usize) -> Result<(Bencode, usize)> {
    let colon = buf[pos..].iter().position(|&b| b == b':').ok_or(WireError::Truncated)? + pos;
    let len: usize = std::str::from_utf8(&buf[pos..colon])
        .ok().and_then(|s| s.parse().ok()).ok_or(WireError::Invalid("bad length"))?;
    let start = colon + 1;
    let end = start.checked_add(len).ok_or(WireError::Invalid("len overflow"))?;
    if end > buf.len() { return Err(WireError::Truncated); }
    Ok((Bencode::Bytes(buf[start..end].to_vec()), end))
}

fn parse_list(buf: &[u8], pos: usize) -> Result<(Bencode, usize)> {
    let mut i = pos + 1;
    let mut items = Vec::new();
    loop {
        match buf.get(i).ok_or(WireError::Truncated)? {
            b'e' => return Ok((Bencode::List(items), i + 1)),
            _ => { let (v, n) = parse_value(buf, i)?; items.push(v); i = n; }
        }
    }
}

fn parse_dict(buf: &[u8], pos: usize) -> Result<(Bencode, usize)> {
    let mut i = pos + 1;
    let mut map = BTreeMap::new();
    loop {
        match buf.get(i).ok_or(WireError::Truncated)? {
            b'e' => return Ok((Bencode::Dict(map), i + 1)),
            _ => {
                let (k, n) = parse_bytes(buf, i)?;
                let key = if let Bencode::Bytes(b) = k { b } else { unreachable!() };
                let (v, n2) = parse_value(buf, n)?;
                map.insert(key, v);
                i = n2;
            }
        }
    }
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p ace-wire bencode`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add crates/ace-wire/src/bencode.rs
git commit -m "ace-wire: minimal bencode parse/encode with canonical dict ordering"
```

---

## Task 4: `handshake` module

**Files:**
- Modify: `crates/ace-wire/src/handshake.rs`
- Test: append to `crates/ace-wire/tests/vectors.rs`

- [ ] **Step 1: Write the failing test (append)**

Append to `crates/ace-wire/tests/vectors.rs`:
```rust
use ace_wire::handshake::{Handshake, PSTR};

#[test]
fn decodes_captured_handshake() {
    let bytes = vec_bytes("messages/encrypter-handshake-peer-in.bin");
    let hs = Handshake::decode(&bytes).unwrap();
    assert_eq!(PSTR, b"AceStreamProtocol");
    assert_eq!(hex::encode(hs.infohash), "50e93529d3eb46a50506b14464185a15292d6e47");
    assert_eq!(&hs.peer_id, b"R30------Ef2V8QOgmt4");
    assert_eq!(hs.reserved, [0u8; 8]);
    // re-encode must be byte-identical to the captured 66 bytes
    assert_eq!(hs.encode().to_vec(), bytes);
}

#[test]
fn random_peer_id_has_acestream_prefix() {
    let id = ace_wire::handshake::random_peer_id();
    assert_eq!(&id[..9], b"R30------");
    assert_eq!(id.len(), 20);
}

#[test]
fn rejects_wrong_pstr() {
    let mut bytes = vec_bytes("messages/encrypter-handshake-peer-in.bin");
    bytes[1] = b'X'; // corrupt pstr
    assert!(Handshake::decode(&bytes).is_err());
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p ace-wire --test vectors`
Expected: FAIL — `Handshake` not found.

- [ ] **Step 3: Implement**

Write `crates/ace-wire/src/handshake.rs`:
```rust
//! Acestream peer handshake: 66 bytes, BitTorrent layout with a custom pstr.
//! `0x11 "AceStreamProtocol" + 8 reserved + infohash(20) + peer_id(20)`.
use crate::{Result, WireError};
use rand::Rng;

/// Protocol string (length 17). Replaces BitTorrent's "BitTorrent protocol".
pub const PSTR: &[u8] = b"AceStreamProtocol";
/// Total handshake length: 1 + 17 + 8 + 20 + 20.
pub const HANDSHAKE_LEN: usize = 66;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Handshake {
    pub reserved: [u8; 8],
    pub infohash: [u8; 20],
    pub peer_id: [u8; 20],
}

impl Handshake {
    /// Build an outgoing handshake with zero reserved bits.
    pub fn new(infohash: [u8; 20], peer_id: [u8; 20]) -> Self {
        Handshake { reserved: [0; 8], infohash, peer_id }
    }

    pub fn encode(&self) -> [u8; HANDSHAKE_LEN] {
        let mut out = [0u8; HANDSHAKE_LEN];
        out[0] = PSTR.len() as u8;
        out[1..18].copy_from_slice(PSTR);
        out[18..26].copy_from_slice(&self.reserved);
        out[26..46].copy_from_slice(&self.infohash);
        out[46..66].copy_from_slice(&self.peer_id);
        out
    }

    pub fn decode(buf: &[u8]) -> Result<Handshake> {
        if buf.len() < HANDSHAKE_LEN { return Err(WireError::Truncated); }
        if buf[0] as usize != PSTR.len() || &buf[1..18] != PSTR {
            return Err(WireError::Invalid("not an AceStreamProtocol handshake"));
        }
        let mut reserved = [0u8; 8];
        reserved.copy_from_slice(&buf[18..26]);
        let mut infohash = [0u8; 20];
        infohash.copy_from_slice(&buf[26..46]);
        let mut peer_id = [0u8; 20];
        peer_id.copy_from_slice(&buf[46..66]);
        Ok(Handshake { reserved, infohash, peer_id })
    }
}

/// Generate an ephemeral peer_id: `R30------` + 11 random ASCII alphanumerics.
pub fn random_peer_id() -> [u8; 20] {
    const ALPHANUM: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    let mut id = [0u8; 20];
    id[..9].copy_from_slice(b"R30------");
    let mut rng = rand::thread_rng();
    for b in &mut id[9..] {
        *b = ALPHANUM[rng.gen_range(0..ALPHANUM.len())];
    }
    id
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p ace-wire --test vectors`
Expected: PASS (all handshake + infohash tests).

- [ ] **Step 5: Commit**

```bash
git add crates/ace-wire/src/handshake.rs crates/ace-wire/tests/vectors.rs
git commit -m "ace-wire: AceStreamProtocol handshake encode/decode vs captured vector"
```

---

## Task 5: `message` module (framing)

**Files:**
- Modify: `crates/ace-wire/src/message.rs`
- Test: `crates/ace-wire/src/message.rs` (inline `#[cfg(test)]`)

- [ ] **Step 1: Write the failing test (inline)**

Put at the bottom of `crates/ace-wire/src/message.rs`:
```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keepalive_roundtrip() {
        assert_eq!(PeerMessage::KeepAlive.encode(), vec![0, 0, 0, 0]);
        assert_eq!(PeerMessage::decode(&[0, 0, 0, 0]).unwrap(), Some((PeerMessage::KeepAlive, 4)));
    }

    #[test]
    fn request_roundtrip() {
        let m = PeerMessage::Request { index: 1, begin: 16384, length: 16384 };
        let enc = m.encode();
        // len = 13 (1 id + 12 payload)
        assert_eq!(&enc[..5], &[0, 0, 0, 13, 6]);
        assert_eq!(PeerMessage::decode(&enc).unwrap(), Some((m, enc.len())));
    }

    #[test]
    fn extended_roundtrip() {
        let m = PeerMessage::Extended { ext_id: 0, payload: b"de".to_vec() };
        let enc = m.encode();
        assert_eq!(&enc[..6], &[0, 0, 0, 4, 20, 0]); // len=4, id=20, ext_id=0
        assert_eq!(PeerMessage::decode(&enc).unwrap(), Some((m, enc.len())));
    }

    #[test]
    fn partial_frame_returns_none() {
        // length prefix says 13 bytes follow, but we only have a few
        assert_eq!(PeerMessage::decode(&[0, 0, 0, 13, 6, 0, 0]).unwrap(), None);
        // not even a full length prefix
        assert_eq!(PeerMessage::decode(&[0, 0]).unwrap(), None);
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p ace-wire message`
Expected: FAIL — `PeerMessage` not found.

- [ ] **Step 3: Implement**

Replace `crates/ace-wire/src/message.rs` (keep the test module at the bottom):
```rust
//! BitTorrent peer-message framing: <u32 be length><u8 id><payload>; len 0 = keep-alive.
use crate::{Result, WireError};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PeerMessage {
    KeepAlive,
    Choke,
    Unchoke,
    Interested,
    NotInterested,
    Have(u32),
    Bitfield(Vec<u8>),
    Request { index: u32, begin: u32, length: u32 },
    Piece { index: u32, begin: u32, block: Vec<u8> },
    Cancel { index: u32, begin: u32, length: u32 },
    Extended { ext_id: u8, payload: Vec<u8> },
}

impl PeerMessage {
    pub fn encode(&self) -> Vec<u8> {
        let mut body = Vec::new();
        match self {
            PeerMessage::KeepAlive => {}
            PeerMessage::Choke => body.push(0),
            PeerMessage::Unchoke => body.push(1),
            PeerMessage::Interested => body.push(2),
            PeerMessage::NotInterested => body.push(3),
            PeerMessage::Have(i) => { body.push(4); body.extend_from_slice(&i.to_be_bytes()); }
            PeerMessage::Bitfield(b) => { body.push(5); body.extend_from_slice(b); }
            PeerMessage::Request { index, begin, length } => {
                body.push(6); body.extend_from_slice(&index.to_be_bytes());
                body.extend_from_slice(&begin.to_be_bytes()); body.extend_from_slice(&length.to_be_bytes());
            }
            PeerMessage::Piece { index, begin, block } => {
                body.push(7); body.extend_from_slice(&index.to_be_bytes());
                body.extend_from_slice(&begin.to_be_bytes()); body.extend_from_slice(block);
            }
            PeerMessage::Cancel { index, begin, length } => {
                body.push(8); body.extend_from_slice(&index.to_be_bytes());
                body.extend_from_slice(&begin.to_be_bytes()); body.extend_from_slice(&length.to_be_bytes());
            }
            PeerMessage::Extended { ext_id, payload } => {
                body.push(20); body.push(*ext_id); body.extend_from_slice(payload);
            }
        }
        let mut out = (body.len() as u32).to_be_bytes().to_vec();
        out.extend_from_slice(&body);
        out
    }

    /// Decode one frame from the front of `buf`.
    /// Returns Ok(None) if more bytes are needed (incomplete frame).
    pub fn decode(buf: &[u8]) -> Result<Option<(PeerMessage, usize)>> {
        if buf.len() < 4 { return Ok(None); }
        let len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
        let total = 4 + len;
        if buf.len() < total { return Ok(None); }
        let body = &buf[4..total];
        if len == 0 { return Ok(Some((PeerMessage::KeepAlive, total))); }
        let id = body[0];
        let p = &body[1..];
        let msg = match id {
            0 => PeerMessage::Choke,
            1 => PeerMessage::Unchoke,
            2 => PeerMessage::Interested,
            3 => PeerMessage::NotInterested,
            4 => PeerMessage::Have(be32(p, 0)?),
            5 => PeerMessage::Bitfield(p.to_vec()),
            6 => PeerMessage::Request { index: be32(p, 0)?, begin: be32(p, 4)?, length: be32(p, 8)? },
            7 => {
                if p.len() < 8 { return Err(WireError::Invalid("short piece")); }
                PeerMessage::Piece { index: be32(p, 0)?, begin: be32(p, 4)?, block: p[8..].to_vec() }
            }
            8 => PeerMessage::Cancel { index: be32(p, 0)?, begin: be32(p, 4)?, length: be32(p, 8)? },
            20 => {
                if p.is_empty() { return Err(WireError::Invalid("short extended")); }
                PeerMessage::Extended { ext_id: p[0], payload: p[1..].to_vec() }
            }
            _ => return Err(WireError::Invalid("unknown message id")),
        };
        Ok(Some((msg, total)))
    }
}

fn be32(p: &[u8], off: usize) -> Result<u32> {
    let s = p.get(off..off + 4).ok_or(WireError::Invalid("short u32"))?;
    Ok(u32::from_be_bytes([s[0], s[1], s[2], s[3]]))
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p ace-wire message`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
git add crates/ace-wire/src/message.rs
git commit -m "ace-wire: BitTorrent message framing encode/decode"
```

---

## Task 6: `extended` module (BEP-10 handshake)

**Files:**
- Modify: `crates/ace-wire/src/extended.rs`
- Test: `crates/ace-wire/src/extended.rs` (inline `#[cfg(test)]`)

- [ ] **Step 1: Write the failing test (inline)**

Put at the bottom of `crates/ace-wire/src/extended.rs`:
```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_acestream_extended_handshake() {
        // A minimal Acestream-style extended handshake payload (bencode dict):
        // {ace_metadata_version: 1, geoip_country: "ES", m: {ut_metadata: 2}}
        let payload = b"d20:ace_metadata_versioni1e13:geoip_country2:ES1:md11:ut_metadatai2eee";
        let eh = ExtendedHandshake::parse(payload).unwrap();
        assert_eq!(eh.ace_metadata_version, Some(1));
        assert_eq!(eh.geoip_country.as_deref(), Some("ES"));
        assert_eq!(eh.ut_metadata_id(), Some(2));
    }

    #[test]
    fn rejects_non_dict() {
        assert!(ExtendedHandshake::parse(b"i5e").is_err());
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p ace-wire extended`
Expected: FAIL — `ExtendedHandshake` not found.

- [ ] **Step 3: Implement**

Write `crates/ace-wire/src/extended.rs`:
```rust
//! BEP-10 extended handshake (the `Extended { ext_id: 0 }` payload), with the
//! Acestream metadata keys observed in capture (see docs/protocol/wire-protocol.md).
use crate::bencode::Bencode;
use crate::{Result, WireError};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExtendedHandshake {
    /// The full decoded dict, for access to keys not surfaced as fields.
    pub raw: Bencode,
    pub ace_metadata_version: Option<i64>,
    pub geoip_country: Option<String>,
}

impl ExtendedHandshake {
    /// Parse the bencoded dict that follows the `<ext_id>` byte in an extended message.
    pub fn parse(payload: &[u8]) -> Result<ExtendedHandshake> {
        let raw = Bencode::parse(payload)?;
        if !matches!(raw, Bencode::Dict(_)) {
            return Err(WireError::Invalid("extended handshake not a dict"));
        }
        let ace_metadata_version = raw.get(b"ace_metadata_version").and_then(Bencode::as_int);
        let geoip_country = raw.get(b"geoip_country")
            .and_then(Bencode::as_bytes)
            .and_then(|b| std::str::from_utf8(b).ok())
            .map(|s| s.to_string());
        Ok(ExtendedHandshake { raw, ace_metadata_version, geoip_country })
    }

    /// The peer's extension id for `ut_metadata` (BEP-9), from the `m` dict.
    pub fn ut_metadata_id(&self) -> Option<i64> {
        self.raw.get(b"m").and_then(|m| m.get(b"ut_metadata")).and_then(Bencode::as_int)
    }
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p ace-wire extended`
Expected: PASS (2 tests).

- [ ] **Step 5: Run the full suite + commit**

Run: `cargo test -p ace-wire`
Expected: ALL tests pass (infohash, bencode, handshake, message, extended).

```bash
git add crates/ace-wire/src/extended.rs
git commit -m "ace-wire: BEP-10 extended handshake parse with Acestream keys"
```

---

## Self-Review Notes (coverage vs spec)

- `wire-protocol.md` §1 identifiers → Task 2 (infohash). content_id derivation is OPEN
  upstream and intentionally not in this crate.
- §3.1 handshake (66-byte `AceStreamProtocol`) → Task 4, validated against the captured
  `encrypter-handshake-peer-in.bin` vector byte-for-byte.
- §3.2 message framing + BT message ids → Task 5.
- §3.3 BEP-10 extended handshake + bencode → Tasks 3 + 6.
- Transport-body inner layout (piece length, piece hashes) is OPEN upstream and is NOT
  in Phase-1 scope; it lands when the body format is reversed (carry-forward item).
- Type consistency check: `Handshake`/`PeerMessage`/`Bencode`/`ExtendedHandshake` and
  `WireError`/`Result` names are used identically across tasks. The `extended` module
  depends only on `bencode` (Task 3 precedes Task 6). Vectors paths use
  `CARGO_MANIFEST_DIR/../../tests/vectors`.
- Transport magic (`AceStreamTransport`) is distinct from the handshake pstr
  (`AceStreamProtocol`) — called out at Task 2 Step 3 to prevent the easy mix-up.
```
