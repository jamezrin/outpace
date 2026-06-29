//! BEP-10 extended handshake (the `Extended { ext_id: 0 }` payload), with the
//! Acestream metadata keys observed in capture (see docs/protocol/wire-protocol.md).
use crate::bencode::Bencode;
use crate::identity::{handshake_digest, Identity};
use crate::{Result, WireError};
use std::collections::BTreeMap;

/// A live piece-window position advertised in the `mi` sub-dict of an outgoing
/// extended handshake. Mirrors the keys real peers send (note 11); lets us present
/// ourselves as a live participant near the head so a peer's picker will engage.
#[derive(Debug, Clone, Copy)]
pub struct LivePosition {
    pub min_piece: i64,
    pub max_piece: i64,
    pub position: i64,
    pub distance_from_source: i64,
}

/// The extended handshake we SEND (BEP-10 sub-id 0). [`encode_payload`] produces the
/// bencoded bytes that follow the `<ext_id>` byte in an `Extended` peer message.
///
/// [`encode_payload`]: OutgoingExtendedHandshake::encode_payload
/// Node identity / announce fields a real peer carries in its extended handshake
/// (see note 17). `v`/`pv`/`p`/`nt`/`platform` default to the values the engine sends;
/// `ts` is a per-connection counter — any self-consistent value works for us.
#[derive(Debug, Clone, Copy)]
pub struct NodeFields {
    pub ts: i64,
    pub v: i64,
    pub pv: i64,
    pub p: i64,
    pub nt: i64,
    pub platform: i64,
}

impl Default for NodeFields {
    fn default() -> Self {
        // Mirrors observed engine 3.2.11 handshakes.
        NodeFields { ts: 0, v: 3021100, pv: 2, p: 8621, nt: 1, platform: 2 }
    }
}

#[derive(Debug, Clone)]
pub struct OutgoingExtendedHandshake {
    pub ace_metadata_version: i64,
    /// The extension id we assign to `ut_metadata` (BEP-9) in our `m` dict.
    pub ut_metadata_id: i64,
    /// Optional live position; when present, emitted as the `mi` sub-dict.
    pub mi: Option<LivePosition>,
    /// Node identity/announce fields signed into the handshake.
    pub node: NodeFields,
}

impl OutgoingExtendedHandshake {
    /// The base BEP-10 fields (no node identity): `ace_metadata_version`, `m`, `mi`.
    fn base_fields(&self) -> BTreeMap<Vec<u8>, Bencode> {
        let mut root: BTreeMap<Vec<u8>, Bencode> = BTreeMap::new();
        root.insert(
            b"ace_metadata_version".to_vec(),
            Bencode::Int(self.ace_metadata_version),
        );

        let mut m: BTreeMap<Vec<u8>, Bencode> = BTreeMap::new();
        m.insert(b"ut_metadata".to_vec(), Bencode::Int(self.ut_metadata_id));
        root.insert(b"m".to_vec(), Bencode::Dict(m));

        if let Some(p) = self.mi {
            let mut mi: BTreeMap<Vec<u8>, Bencode> = BTreeMap::new();
            mi.insert(b"min_piece".to_vec(), Bencode::Int(p.min_piece));
            mi.insert(b"max_piece".to_vec(), Bencode::Int(p.max_piece));
            mi.insert(b"position".to_vec(), Bencode::Int(p.position));
            mi.insert(
                b"distance_from_source".to_vec(),
                Bencode::Int(p.distance_from_source),
            );
            root.insert(b"mi".to_vec(), Bencode::Dict(mi));
        }
        root
    }

    /// Build the bencoded extended-handshake payload WITHOUT a node identity (BEP-10
    /// base only). Use [`sign_and_encode`](Self::sign_and_encode) for a handshake peers
    /// will accept.
    pub fn encode_payload(&self) -> Vec<u8> {
        Bencode::Dict(self.base_fields()).encode()
    }

    /// Build the payload carrying our node identity and a valid signature (note 17):
    /// add `node_id` + announce fields, compute `SHA256(bencode(dict, signature=zeros))`,
    /// Ed25519-sign it, and emit the dict with the real `signature`.
    pub fn sign_and_encode(&self, id: &Identity) -> Vec<u8> {
        let mut f = self.base_fields();
        f.insert(b"node_id".to_vec(), Bencode::Bytes(id.node_id().to_vec()));
        f.insert(b"ts".to_vec(), Bencode::Int(self.node.ts));
        f.insert(b"v".to_vec(), Bencode::Int(self.node.v));
        f.insert(b"pv".to_vec(), Bencode::Int(self.node.pv));
        f.insert(b"p".to_vec(), Bencode::Int(self.node.p));
        f.insert(b"nt".to_vec(), Bencode::Int(self.node.nt));
        f.insert(b"platform".to_vec(), Bencode::Int(self.node.platform));
        let digest = handshake_digest(&f);
        let sig = id.sign(&digest);
        f.insert(b"signature".to_vec(), Bencode::Bytes(sig.to_vec()));
        Bencode::Dict(f).encode()
    }
}

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
    fn encodes_minimal_handshake_that_reparses() {
        let payload = OutgoingExtendedHandshake {
            ace_metadata_version: 1,
            ut_metadata_id: 2,
            mi: None,
            node: NodeFields::default(),
        }
        .encode_payload();
        let parsed = ExtendedHandshake::parse(&payload).unwrap();
        assert_eq!(parsed.ace_metadata_version, Some(1));
        assert_eq!(parsed.ut_metadata_id(), Some(2));
        // No live position advertised.
        assert!(parsed.raw.get(b"mi").is_none());
    }

    #[test]
    fn signed_handshake_verifies_against_our_node_id() {
        use crate::identity::{verify_handshake, Identity};
        let id = Identity::generate();
        let payload = OutgoingExtendedHandshake {
            ace_metadata_version: 1,
            ut_metadata_id: 2,
            mi: Some(LivePosition {
                min_piece: 100,
                max_piece: 200,
                position: 200,
                distance_from_source: 0,
            }),
            node: NodeFields { ts: 12345, ..NodeFields::default() },
        }
        .sign_and_encode(&id);

        let parsed = ExtendedHandshake::parse(&payload).unwrap();
        let dict = match &parsed.raw {
            Bencode::Dict(d) => d.clone(),
            _ => panic!(),
        };
        let node_id: [u8; 32] =
            dict[b"node_id".as_slice()].as_bytes().unwrap().try_into().unwrap();
        let sig: [u8; 64] =
            dict[b"signature".as_slice()].as_bytes().unwrap().try_into().unwrap();
        assert_eq!(node_id, id.node_id());
        assert!(verify_handshake(&node_id, &sig, &dict));
    }

    #[test]
    fn encodes_mi_live_position() {
        let payload = OutgoingExtendedHandshake {
            ace_metadata_version: 1,
            ut_metadata_id: 2,
            mi: Some(LivePosition {
                min_piece: 100,
                max_piece: 200,
                position: 200,
                distance_from_source: 5,
            }),
            node: NodeFields::default(),
        }
        .encode_payload();
        let parsed = ExtendedHandshake::parse(&payload).unwrap();
        let mi = parsed.raw.get(b"mi").expect("mi present");
        assert_eq!(mi.get(b"min_piece").and_then(Bencode::as_int), Some(100));
        assert_eq!(mi.get(b"max_piece").and_then(Bencode::as_int), Some(200));
        assert_eq!(mi.get(b"position").and_then(Bencode::as_int), Some(200));
        assert_eq!(
            mi.get(b"distance_from_source").and_then(Bencode::as_int),
            Some(5)
        );
    }

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
