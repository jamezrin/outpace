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
        if n != buf.len() {
            return Err(WireError::Invalid("trailing bytes"));
        }
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
            Bencode::Int(i) => {
                out.push(b'i');
                out.extend_from_slice(i.to_string().as_bytes());
                out.push(b'e');
            }
            Bencode::Bytes(b) => {
                out.extend_from_slice(b.len().to_string().as_bytes());
                out.push(b':');
                out.extend_from_slice(b);
            }
            Bencode::List(l) => {
                out.push(b'l');
                for e in l {
                    e.encode_into(out);
                }
                out.push(b'e');
            }
            Bencode::Dict(d) => {
                out.push(b'd');
                for (k, v) in d {
                    Bencode::Bytes(k.clone()).encode_into(out);
                    v.encode_into(out);
                }
                out.push(b'e');
            }
        }
    }

    /// Convenience: borrow a dict entry.
    pub fn get<'a>(&'a self, key: &[u8]) -> Option<&'a Bencode> {
        match self {
            Bencode::Dict(d) => d.get(key),
            _ => None,
        }
    }
    pub fn as_int(&self) -> Option<i64> {
        if let Bencode::Int(i) = self {
            Some(*i)
        } else {
            None
        }
    }
    pub fn as_bytes(&self) -> Option<&[u8]> {
        if let Bencode::Bytes(b) = self {
            Some(b)
        } else {
            None
        }
    }
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
    let end = buf[pos + 1..]
        .iter()
        .position(|&b| b == b'e')
        .ok_or(WireError::Truncated)?
        + pos
        + 1;
    let s = std::str::from_utf8(&buf[pos + 1..end]).map_err(|_| WireError::Invalid("int utf8"))?;
    let i = s
        .parse::<i64>()
        .map_err(|_| WireError::Invalid("int parse"))?;
    Ok((Bencode::Int(i), end + 1))
}

fn parse_bytes(buf: &[u8], pos: usize) -> Result<(Bencode, usize)> {
    let colon = buf[pos..]
        .iter()
        .position(|&b| b == b':')
        .ok_or(WireError::Truncated)?
        + pos;
    let len: usize = std::str::from_utf8(&buf[pos..colon])
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or(WireError::Invalid("bad length"))?;
    let start = colon + 1;
    let end = start
        .checked_add(len)
        .ok_or(WireError::Invalid("len overflow"))?;
    if end > buf.len() {
        return Err(WireError::Truncated);
    }
    Ok((Bencode::Bytes(buf[start..end].to_vec()), end))
}

fn parse_list(buf: &[u8], pos: usize) -> Result<(Bencode, usize)> {
    let mut i = pos + 1;
    let mut items = Vec::new();
    loop {
        match buf.get(i).ok_or(WireError::Truncated)? {
            b'e' => return Ok((Bencode::List(items), i + 1)),
            _ => {
                let (v, n) = parse_value(buf, i)?;
                items.push(v);
                i = n;
            }
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
                let key = if let Bencode::Bytes(b) = k {
                    b
                } else {
                    unreachable!()
                };
                let (v, n2) = parse_value(buf, n)?;
                map.insert(key, v);
                i = n2;
            }
        }
    }
}

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
        assert_eq!(
            v,
            Bencode::List(vec![Bencode::Int(-3), Bencode::Bytes(b"a".to_vec())])
        );
    }

    #[test]
    fn rejects_trailing_and_truncated() {
        assert!(Bencode::parse(b"i42").is_err()); // truncated
        assert!(Bencode::parse(b"i42eX").is_err()); // trailing byte
        assert!(Bencode::parse(b"3:ab").is_err()); // short string
    }
}
