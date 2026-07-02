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
        NodeFields {
            ts: 0,
            v: 3021100,
            pv: 2,
            p: 8621,
            nt: 1,
            platform: 2,
        }
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
    /// The recipient peer's IP as 4 bytes (the `yourip` field; anti-spoof). None to omit.
    pub peer_ip: Option<[u8; 4]>,
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

    /// Build the payload carrying our node identity and a valid signature over the FULL
    /// accepted field set (note 19): identity + announce fields + rich `mi` + `yourip`,
    /// signed as `SHA256(bencode(dict, signature=zeros))` then Ed25519.
    pub fn sign_and_encode(&self, id: &Identity) -> Vec<u8> {
        let bi = |n: i64| Bencode::Int(n);
        let bb = |b: &[u8]| Bencode::Bytes(b.to_vec());
        let mut f = self.base_fields(); // ace_metadata_version, m, mi(min/max/pos/dist)
                                        // Promote `mi` to the full live-position dict peers expect.
        if let Some(p) = self.mi {
            let mut mi: BTreeMap<Vec<u8>, Bencode> = BTreeMap::new();
            let has_live_head = p.position >= 0 && p.max_piece >= p.min_piece && p.max_piece >= 0;
            let source_end = if has_live_head { p.max_piece } else { -1 };
            let live_window_size = if has_live_head {
                115
            } else {
                (p.max_piece - p.min_piece + 1).max(0)
            };
            for (k, v) in [
                ("distance_from_source", p.distance_from_source),
                ("down_rate", 0),
                ("download_window_end", source_end),
                ("is_accessible", 0),
                ("live_window_size", live_window_size),
                ("lsp", source_end),
                ("mam", -1),
                ("max_piece", p.max_piece),
                ("min_piece", p.min_piece),
                ("peer_type", 0),
                ("ping_from_source", -1),
                ("position", p.position),
                ("time_from_source", -1),
                ("top_session_up_rate", 0),
                ("top_up_rate", 0),
                ("up_rate", 0),
                ("upload_rating", 0),
            ] {
                mi.insert(k.as_bytes().to_vec(), bi(v));
            }
            f.insert(b"mi".to_vec(), Bencode::Dict(mi));
        }
        f.insert(b"asn".to_vec(), bi(0));
        f.insert(b"asn_country".to_vec(), bb(b""));
        f.insert(b"geoip_country".to_vec(), bb(b""));
        let root_lsp = self
            .mi
            .filter(|p| p.position >= 0 && p.max_piece >= p.min_piece && p.max_piece >= 0)
            .map(|p| p.max_piece)
            .unwrap_or(-1);
        f.insert(b"lsp".to_vec(), bi(root_lsp));
        if root_lsp >= 0 {
            f.insert(b"node_state".to_vec(), bi(1));
        }
        f.insert(b"node_id".to_vec(), bb(&id.node_id()));
        f.insert(b"nt".to_vec(), bi(self.node.nt));
        f.insert(b"p".to_vec(), bi(self.node.p));
        f.insert(b"platform".to_vec(), bi(self.node.platform));
        f.insert(b"pv".to_vec(), bi(self.node.pv));
        f.insert(b"stream_statuses".to_vec(), Bencode::Dict(BTreeMap::new()));
        f.insert(b"ts".to_vec(), bi(self.node.ts));
        f.insert(b"tt".to_vec(), bb(b"bt"));
        f.insert(b"v".to_vec(), bi(self.node.v));
        if let Some(ip) = self.peer_ip {
            f.insert(b"yourip".to_vec(), bb(&ip));
        }
        let digest = handshake_digest(&f);
        f.insert(
            b"signature".to_vec(),
            Bencode::Bytes(id.sign(&digest).to_vec()),
        );
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
        let geoip_country = raw
            .get(b"geoip_country")
            .and_then(Bencode::as_bytes)
            .and_then(|b| std::str::from_utf8(b).ok())
            .map(|s| s.to_string());
        Ok(ExtendedHandshake {
            raw,
            ace_metadata_version,
            geoip_country,
        })
    }

    /// The peer's extension id for `ut_metadata` (BEP-9), from the `m` dict.
    pub fn ut_metadata_id(&self) -> Option<i64> {
        self.raw
            .get(b"m")
            .and_then(|m| m.get(b"ut_metadata"))
            .and_then(Bencode::as_int)
    }

    /// The advertised total size (bytes) of the metadata blob (BEP-9 `metadata_size`),
    /// needed to know how many 16 KiB pieces to request.
    pub fn metadata_size(&self) -> Option<i64> {
        self.raw.get(b"metadata_size").and_then(Bencode::as_int)
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
            peer_ip: None,
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
            node: NodeFields {
                ts: 12345,
                ..NodeFields::default()
            },
            peer_ip: None,
        }
        .sign_and_encode(&id);

        let parsed = ExtendedHandshake::parse(&payload).unwrap();
        let dict = match &parsed.raw {
            Bencode::Dict(d) => d.clone(),
            _ => panic!(),
        };
        let node_id: [u8; 32] = dict[b"node_id".as_slice()]
            .as_bytes()
            .unwrap()
            .try_into()
            .unwrap();
        let sig: [u8; 64] = dict[b"signature".as_slice()]
            .as_bytes()
            .unwrap()
            .try_into()
            .unwrap();
        assert_eq!(node_id, id.node_id());
        assert!(verify_handshake(&node_id, &sig, &dict));
    }

    #[test]
    fn full_signed_handshake_has_all_accepted_fields_and_verifies() {
        use crate::identity::{verify_handshake, Identity};
        let id = Identity::generate();
        let hs = OutgoingExtendedHandshake {
            ace_metadata_version: 1,
            ut_metadata_id: 2,
            mi: Some(LivePosition {
                min_piece: 100,
                max_piece: 163,
                position: -1,
                distance_from_source: -1,
            }),
            node: NodeFields {
                ts: 5000,
                ..NodeFields::default()
            },
            peer_ip: Some([95, 17, 44, 10]),
        };
        let payload = hs.sign_and_encode(&id);
        let dict = match Bencode::parse(&payload).unwrap() {
            Bencode::Dict(d) => d,
            _ => panic!(),
        };
        for k in [
            b"ace_metadata_version".as_slice(),
            b"asn",
            b"asn_country",
            b"geoip_country",
            b"lsp",
            b"m",
            b"mi",
            b"node_id",
            b"nt",
            b"p",
            b"platform",
            b"pv",
            b"signature",
            b"stream_statuses",
            b"ts",
            b"tt",
            b"v",
            b"yourip",
        ] {
            assert!(
                dict.contains_key(k),
                "missing key {:?}",
                std::str::from_utf8(k)
            );
        }
        assert_eq!(dict[b"tt".as_slice()].as_bytes(), Some(b"bt".as_slice()));
        assert_eq!(
            dict[b"yourip".as_slice()].as_bytes(),
            Some([95u8, 17, 44, 10].as_slice())
        );
        let mi = match &dict[b"mi".as_slice()] {
            Bencode::Dict(d) => d,
            _ => panic!(),
        };
        assert_eq!(mi[b"min_piece".as_slice()].as_int(), Some(100));
        assert_eq!(mi[b"max_piece".as_slice()].as_int(), Some(163));
        let node_id: [u8; 32] = dict[b"node_id".as_slice()]
            .as_bytes()
            .unwrap()
            .try_into()
            .unwrap();
        let sig: [u8; 64] = dict[b"signature".as_slice()]
            .as_bytes()
            .unwrap()
            .try_into()
            .unwrap();
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
            peer_ip: None,
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
        assert_eq!(eh.metadata_size(), None);
    }

    #[test]
    fn reads_metadata_size_when_present() {
        // {m: {ut_metadata: 3}, metadata_size: 40000}
        let payload = b"d1:md11:ut_metadatai3ee13:metadata_sizei40000ee";
        let eh = ExtendedHandshake::parse(payload).unwrap();
        assert_eq!(eh.ut_metadata_id(), Some(3));
        assert_eq!(eh.metadata_size(), Some(40000));
    }

    #[test]
    fn rejects_non_dict() {
        assert!(ExtendedHandshake::parse(b"i5e").is_err());
    }
}
