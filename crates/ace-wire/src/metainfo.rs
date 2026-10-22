//! Strict, bounded decoding of exported single-file torrent metainfo.
//!
//! This is deliberately separate from encrypted `AceStreamTransport` decoding. Identity
//! covers the original encoded `info` value, never a reserialized dictionary.

use crate::bencode::Bencode;
use crate::{Result, WireError};
use sha1::{Digest, Sha1};
use std::collections::BTreeMap;
use std::ops::Range;

const MAX_BYTES: usize = 1_048_576;
const MAX_DEPTH: usize = 32;
const MAX_VALUES: usize = 65_536;
const BLOCK_LENGTH: u64 = 16_384;
const MAX_PIECE_LENGTH: u64 = 8 * 1_048_576;
const MAX_TRACKERS: usize = 128;
const MAX_TRACKER_BYTES: usize = 2_048;

/// Validated geometry, piece integrity and discovery data for a single-file VOD.
#[derive(Debug, PartialEq, Eq)]
pub struct Metainfo {
    pub infohash: [u8; 20],
    pub piece_length: u64,
    pub total_length: u64,
    pub piece_hashes: Vec<[u8; 20]>,
    pub trackers: Vec<String>,
}

/// Decode one complete exported metainfo file. No prefix recovery or framing guesses.
pub fn decode_metainfo(bytes: &[u8]) -> Result<Metainfo> {
    if bytes.len() > MAX_BYTES {
        return Err(WireError::Invalid("metainfo too large"));
    }
    let mut parser = Parser {
        bytes,
        pos: 0,
        values: 0,
        info_span: None,
    };
    let root = parser.value(0)?;
    if parser.pos != bytes.len() {
        return Err(WireError::Invalid("trailing metainfo bytes"));
    }
    let info = root
        .get(b"info")
        .filter(|v| matches!(v, Bencode::Dict(_)))
        .ok_or(WireError::Invalid("metainfo missing info dictionary"))?;
    if info.get(b"files").is_some() {
        return Err(WireError::Invalid("multi-file metainfo is not supported"));
    }
    let total_length = positive(info, b"length")?;
    let piece_length = positive(info, b"piece length")?;
    if piece_length > MAX_PIECE_LENGTH
        || !piece_length.is_multiple_of(BLOCK_LENGTH)
        || piece_length / BLOCK_LENGTH > u16::MAX as u64
    {
        return Err(WireError::Invalid("unsupported metainfo piece geometry"));
    }
    let pieces = info
        .get(b"pieces")
        .and_then(Bencode::as_bytes)
        .ok_or(WireError::Invalid("metainfo missing binary pieces"))?;
    if pieces.is_empty()
        || !pieces.len().is_multiple_of(20)
        || pieces.len() as u64 / 20 != total_length.div_ceil(piece_length)
    {
        return Err(WireError::Invalid(
            "metainfo piece count does not match length",
        ));
    }
    let span = parser
        .info_span
        .ok_or(WireError::Invalid("metainfo missing info span"))?;
    Ok(Metainfo {
        infohash: Sha1::digest(&bytes[span]).into(),
        piece_length,
        total_length,
        piece_hashes: pieces.as_chunks::<20>().0.to_vec(),
        trackers: trackers(&root)?,
    })
}

fn positive(info: &Bencode, key: &[u8]) -> Result<u64> {
    info.get(key)
        .and_then(Bencode::as_int)
        .filter(|value| *value > 0)
        .map(|value| value as u64)
        .ok_or(WireError::Invalid(
            "missing or non-positive metainfo length",
        ))
}

fn trackers(root: &Bencode) -> Result<Vec<String>> {
    let mut urls = Vec::new();
    let mut count = 0;
    if let Some(announce) = root.get(b"announce") {
        add_tracker(announce, &mut urls, &mut count)?;
    }
    if let Some(tiers) = root.get(b"announce-list") {
        let Bencode::List(tiers) = tiers else {
            return Err(WireError::Invalid("invalid announce-list"));
        };
        for tier in tiers {
            let Bencode::List(entries) = tier else {
                return Err(WireError::Invalid("invalid announce tier"));
            };
            for entry in entries {
                add_tracker(entry, &mut urls, &mut count)?;
            }
        }
    }
    Ok(urls)
}

fn add_tracker(value: &Bencode, urls: &mut Vec<String>, count: &mut usize) -> Result<()> {
    *count += 1;
    if *count > MAX_TRACKERS {
        return Err(WireError::Invalid("too many metainfo trackers"));
    }
    let bytes = value
        .as_bytes()
        .filter(|value| !value.is_empty() && value.len() <= MAX_TRACKER_BYTES)
        .ok_or(WireError::Invalid("invalid metainfo tracker"))?;
    let url = std::str::from_utf8(bytes).map_err(|_| WireError::Invalid("tracker is not utf-8"))?;
    let authority = ["http://", "https://", "udp://"]
        .iter()
        .find_map(|prefix| url.strip_prefix(prefix))
        .and_then(|rest| rest.split(['/', '?', '#']).next())
        .filter(|authority| !authority.is_empty());
    if authority.is_none()
        || bytes
            .iter()
            .any(|byte| byte.is_ascii_control() || byte.is_ascii_whitespace())
    {
        return Err(WireError::Invalid("invalid metainfo tracker URL"));
    }
    if !urls.iter().any(|existing| existing == url) {
        urls.push(url.to_owned());
    }
    Ok(())
}

/// A dedicated strict parser keeps the permissive peer-message bencode API unchanged.
/// Values and keys share a budget; all byte copies are bounded by the input limit.
struct Parser<'a> {
    bytes: &'a [u8],
    pos: usize,
    values: usize,
    info_span: Option<Range<usize>>,
}

impl Parser<'_> {
    fn value(&mut self, depth: usize) -> Result<Bencode> {
        self.values += 1;
        if self.values > MAX_VALUES {
            return Err(WireError::Invalid("too many metainfo values"));
        }
        match self.peek()? {
            b'i' => {
                self.pos += 1;
                let token = self.until(b'e')?;
                let digits = token.strip_prefix(b"-").unwrap_or(token);
                if !decimal(digits) || token == b"-0" {
                    return Err(WireError::Invalid("noncanonical metainfo integer"));
                }
                let value = std::str::from_utf8(token)
                    .ok()
                    .and_then(|token| token.parse::<i64>().ok())
                    .ok_or(WireError::Invalid("metainfo integer overflow"))?;
                Ok(Bencode::Int(value))
            }
            b'0'..=b'9' => {
                let token = self.until(b':')?;
                if !decimal(token) {
                    return Err(WireError::Invalid("noncanonical metainfo string length"));
                }
                let len = token.iter().try_fold(0usize, |len, digit| {
                    len.checked_mul(10)?.checked_add((digit - b'0') as usize)
                });
                let end = len
                    .and_then(|len| self.pos.checked_add(len))
                    .ok_or(WireError::Invalid("metainfo string length overflow"))?;
                let bytes = self.bytes.get(self.pos..end).ok_or(WireError::Truncated)?;
                self.pos = end;
                Ok(Bencode::Bytes(bytes.to_vec()))
            }
            b'l' | b'd' if depth >= MAX_DEPTH => {
                Err(WireError::Invalid("metainfo nesting too deep"))
            }
            b'l' => {
                self.pos += 1;
                let mut items = Vec::new();
                while self.peek()? != b'e' {
                    items.push(self.value(depth + 1)?);
                }
                self.pos += 1;
                Ok(Bencode::List(items))
            }
            b'd' => {
                self.pos += 1;
                let mut items = BTreeMap::new();
                while self.peek()? != b'e' {
                    if !self.peek()?.is_ascii_digit() {
                        return Err(WireError::Invalid("metainfo key is not a byte string"));
                    }
                    let Bencode::Bytes(key) = self.value(depth + 1)? else {
                        unreachable!("byte-string token checked above")
                    };
                    if items.contains_key(&key) {
                        return Err(WireError::Invalid("duplicate metainfo key"));
                    }
                    let start = self.pos;
                    let value = self.value(depth + 1)?;
                    if depth == 0 && key == b"info" {
                        self.info_span = Some(start..self.pos);
                    }
                    items.insert(key, value);
                }
                self.pos += 1;
                Ok(Bencode::Dict(items))
            }
            _ => Err(WireError::Invalid("unexpected metainfo token")),
        }
    }

    fn peek(&self) -> Result<u8> {
        self.bytes
            .get(self.pos)
            .copied()
            .ok_or(WireError::Truncated)
    }

    fn until(&mut self, delimiter: u8) -> Result<&[u8]> {
        let start = self.pos;
        let len = self.bytes[start..]
            .iter()
            .position(|byte| *byte == delimiter)
            .ok_or(WireError::Truncated)?;
        self.pos += len + 1;
        Ok(&self.bytes[start..start + len])
    }
}

fn decimal(bytes: &[u8]) -> bool {
    !bytes.is_empty()
        && (bytes.len() == 1 || bytes[0] != b'0')
        && bytes.iter().all(u8::is_ascii_digit)
}
