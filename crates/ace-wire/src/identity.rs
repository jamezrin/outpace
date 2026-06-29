//! Node identity: the Ed25519 keypair + handshake-signature scheme that peers require
//! before they will unchoke us (see `docs/protocol/notes/17-node-signature-cracked.md`).
//!
//! Scheme (verified 6/6 against live engine handshakes):
//! ```text
//! digest32  = SHA256( bencode(handshake_dict, signature := 64 × 0x00) )   # canonical
//! signature = Ed25519_detached_sign(secret_key, digest32)
//! node_id   = Ed25519 public key
//! ```
//! `node_id` is self-generated — we mint our own keypair, no engine key needed.

use crate::bencode::Bencode;
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

/// Length of the `signature` field placeholder used while computing the digest.
pub const SIGNATURE_LEN: usize = 64;

/// A node identity (Ed25519 keypair). `node_id` is the public key.
pub struct Identity {
    key: SigningKey,
}

impl Identity {
    /// Deterministically derive an identity from a 32-byte seed (the engine's
    /// `device.key` is such a seed; ours can be any 32 random bytes).
    pub fn from_seed(seed: [u8; 32]) -> Self {
        Identity { key: SigningKey::from_bytes(&seed) }
    }

    /// Mint a fresh random identity.
    pub fn generate() -> Self {
        Identity { key: SigningKey::generate(&mut rand::rngs::OsRng) }
    }

    /// The `node_id` peers see: the 32-byte Ed25519 public key.
    pub fn node_id(&self) -> [u8; 32] {
        self.key.verifying_key().to_bytes()
    }

    /// Ed25519 detached signature over `msg` (we feed it the 32-byte digest).
    pub fn sign(&self, msg: &[u8]) -> [u8; 64] {
        self.key.sign(msg).to_bytes()
    }
}

/// Compute the 32-byte digest that gets signed: canonical bencode of the handshake dict
/// with the `signature` key forced to 64 zero bytes, then SHA-256.
pub fn handshake_digest(fields: &BTreeMap<Vec<u8>, Bencode>) -> [u8; 32] {
    let mut d = fields.clone();
    d.insert(b"signature".to_vec(), Bencode::Bytes(vec![0u8; SIGNATURE_LEN]));
    let encoded = Bencode::Dict(d).encode();
    let mut h = Sha256::new();
    h.update(&encoded);
    h.finalize().into()
}

/// Verify a node handshake: recompute the digest from `fields` (its own `signature` value
/// is ignored — replaced with zeros) and check `signature` against `node_id`.
pub fn verify_handshake(
    node_id: &[u8; 32],
    signature: &[u8; 64],
    fields: &BTreeMap<Vec<u8>, Bencode>,
) -> bool {
    let digest = handshake_digest(fields);
    let vk = match VerifyingKey::from_bytes(node_id) {
        Ok(v) => v,
        Err(_) => return false,
    };
    vk.verify(&digest, &Signature::from_bytes(signature)).is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dict_of(b: &Bencode) -> BTreeMap<Vec<u8>, Bencode> {
        match b {
            Bencode::Dict(d) => d.clone(),
            _ => panic!("not a dict"),
        }
    }

    /// Known-answer against a real engine extended handshake (note 17). Proves the
    /// digest formula + node_id-as-pubkey by Ed25519-verifying the captured signature.
    #[test]
    fn verifies_live_engine_handshake() {
        let hexstr = include_str!(
            "../../../tests/vectors/node-identity/engine-extended-handshake.hex"
        )
        .trim();
        let raw = hex::decode(hexstr).unwrap();
        let dict = dict_of(&Bencode::parse(&raw).unwrap());

        let node_id: [u8; 32] = dict[b"node_id".as_slice()].as_bytes().unwrap().try_into().unwrap();
        let signature: [u8; 64] =
            dict[b"signature".as_slice()].as_bytes().unwrap().try_into().unwrap();

        assert!(verify_handshake(&node_id, &signature, &dict));
        // tampering a signed field must break verification
        let mut tampered = dict.clone();
        tampered.insert(b"ts".to_vec(), Bencode::Int(999999));
        assert!(!verify_handshake(&node_id, &signature, &tampered));
    }

    #[test]
    fn from_seed_is_deterministic() {
        let seed = [7u8; 32];
        assert_eq!(Identity::from_seed(seed).node_id(), Identity::from_seed(seed).node_id());
    }

    #[test]
    fn sign_then_verify_roundtrip() {
        let id = Identity::generate();
        let mut fields: BTreeMap<Vec<u8>, Bencode> = BTreeMap::new();
        fields.insert(b"node_id".to_vec(), Bencode::Bytes(id.node_id().to_vec()));
        fields.insert(b"ts".to_vec(), Bencode::Int(749872));
        fields.insert(b"p".to_vec(), Bencode::Int(8621));

        let digest = handshake_digest(&fields);
        let sig = id.sign(&digest);
        fields.insert(b"signature".to_vec(), Bencode::Bytes(sig.to_vec()));

        assert!(verify_handshake(&id.node_id(), &sig, &fields));
    }
}
