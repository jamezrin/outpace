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
