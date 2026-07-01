//! Live-source authentication (B0): the RSA identity + per-piece signing scheme a broadcast
//! source uses to let consumers verify pieces genuinely come from it.
//!
//! **Signing scheme — CRACKED (docs/protocol/notes/27-b0-signing-cracked.md), not just the
//! identity.** Recovered by hooking the official engine's own PyCryptodome hash calls
//! (`SHA1_update`/`SHA1_digest` — the engine uses PyCryptodome, not OpenSSL, for this) while
//! it signed real pieces, then confirming the recovered preimage/signature against a real
//! captured piece with `sig.pow(e) mod n`:
//!
//! - **Preimage:** `SHA1(piece_bytes[0 .. piece_length - sig_len])` — i.e. the piece's own
//!   payload with its trailing signature-sized region excluded.
//! - **Signature:** standard **RSASSA-PKCS1-v1_5 with SHA1** (`sig = pow(pad(digest), d, n)`)
//!   — textbook, no custom padding. `sig_len` = the RSA modulus's byte length (96 for a
//!   768-bit key).
//! - **Placement:** the signature is **embedded in-band as the last `sig_len` bytes of the
//!   piece itself** — not a separate wire message. This is why the pure P2P relay path (S1/S2,
//!   `PieceStore`/`SeederSession`) never needed to know about it: a piece's bytes already
//!   *are* `payload || signature` end to end; relaying them verbatim (as we already do)
//!   carries the signature along for free. Only the **source** (origination, B1) needs this
//!   module to actually produce a valid signature instead of the old zero/placeholder tail.
//!
//! Confirmed against a real engine-produced `.sauth` file + two real captured pieces
//! (`tests/vectors/live-source-auth/piece-{0,1}.bin`) — `verify_piece` passes on both, and
//! re-signing the same payload with the same key reproduces the *exact* captured signature
//! bytes (PKCS#1 v1.5 has no randomized padding, so this is a legitimate determinism check,
//! not a coincidence).
//!
//! Identity/transport details (from note 25, still accurate):
//! - The private key is plain **PKCS#1 PEM** (`-----BEGIN RSA PRIVATE KEY-----`).
//! - The transport descriptor's `pubkey` field is the matching **X.509 SubjectPublicKeyInfo
//!   DER** encoding (124 bytes for a 768-bit key) — verified byte-for-byte against
//!   `openssl rsa -pubout -outform DER` on a real captured `.sauth`.
//! - `authmethod` = literal ASCII `"RSA"`.

use rsa::pkcs1::{DecodeRsaPrivateKey, EncodeRsaPrivateKey};
use rsa::pkcs8::{DecodePublicKey, EncodePublicKey};
use rsa::pkcs1v15::Pkcs1v15Sign;
use rsa::traits::PublicKeyParts;
use rsa::{RsaPrivateKey, RsaPublicKey};
use sha1::{Digest, Sha1};

/// Key size matching the real engine's own source-node-generated keys (note 25: a captured
/// `.sauth` was 768 bits). Not a protocol requirement — any RSA key verifies fine — but
/// matching it avoids being fingerprinted as an obviously-different implementation.
const KEY_BITS: usize = 768;

/// A broadcast source's live-auth RSA identity.
pub struct LiveSourceAuth {
    key: RsaPrivateKey,
}

impl LiveSourceAuth {
    /// Mint a fresh keypair.
    pub fn generate() -> Self {
        let key = RsaPrivateKey::new(&mut rand::thread_rng(), KEY_BITS)
            .expect("RSA key generation");
        LiveSourceAuth { key }
    }

    /// Load an identity from its PKCS#1 PEM private-key text (the `.sauth` format).
    pub fn from_pkcs1_pem(pem: &str) -> Result<Self, crate::WireError> {
        let key = RsaPrivateKey::from_pkcs1_pem(pem)
            .map_err(|_| crate::WireError::Invalid("bad PKCS#1 RSA private key PEM"))?;
        Ok(LiveSourceAuth { key })
    }

    /// Serialize to PKCS#1 PEM text — the same format as a real engine's `.sauth` file.
    pub fn to_pkcs1_pem(&self) -> String {
        self.key
            .to_pkcs1_pem(rsa::pkcs8::LineEnding::LF)
            .expect("PKCS#1 PEM encode")
            .to_string()
    }

    /// The DER-encoded X.509 SubjectPublicKeyInfo — exactly what the transport descriptor's
    /// `pubkey` field carries.
    pub fn pubkey_der(&self) -> Vec<u8> {
        let pubkey = RsaPublicKey::from(&self.key);
        pubkey
            .to_public_key_der()
            .expect("DER SPKI encode")
            .as_bytes()
            .to_vec()
    }

    /// The signature length in bytes — the RSA modulus's byte length (96 for a 768-bit key).
    /// This is exactly how many trailing bytes of each *wire piece* the signature occupies.
    pub fn signature_len(&self) -> usize {
        self.key.size()
    }

    /// Sign `payload` (the piece's real content, i.e. everything except where the signature
    /// will go): `SHA1(payload)` then RSASSA-PKCS1-v1_5. Returns exactly `signature_len()`
    /// bytes, appended as-is to `payload` to form the wire piece — see the module docs.
    pub fn sign(&self, payload: &[u8]) -> Vec<u8> {
        let digest = Sha1::digest(payload);
        self.key
            .sign(Pkcs1v15Sign::new::<Sha1>(), &digest)
            .expect("RSA-PKCS1v15-SHA1 sign")
    }
}

/// Verify a live-source signature: `payload` is the piece's real content (everything before
/// the signature tail), `signature` is the trailing bytes, `pubkey_der` is the broadcaster's
/// DER SubjectPublicKeyInfo (the transport descriptor's `pubkey` field). Scheme: RSASSA-
/// PKCS1-v1_5 with SHA1 — see the module docs for how this was recovered.
pub fn verify_piece(pubkey_der: &[u8], payload: &[u8], signature: &[u8]) -> bool {
    let Ok(pubkey) = RsaPublicKey::from_public_key_der(pubkey_der) else {
        return false;
    };
    let digest = Sha1::digest(payload);
    pubkey.verify(Pkcs1v15Sign::new::<Sha1>(), &digest, signature).is_ok()
}

/// Split a wire piece into `(payload, signature)` given the signer's signature length —
/// the inverse of `sign`'s "append to payload" convention. `piece` must be at least
/// `sig_len` bytes.
pub fn split_piece(piece: &[u8], sig_len: usize) -> Option<(&[u8], &[u8])> {
    if piece.len() < sig_len {
        return None;
    }
    Some(piece.split_at(piece.len() - sig_len))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_pubkey_der_round_trips_through_pem_reload() {
        let a = LiveSourceAuth::generate();
        let pem = a.to_pkcs1_pem();
        let b = LiveSourceAuth::from_pkcs1_pem(&pem).unwrap();
        assert_eq!(a.pubkey_der(), b.pubkey_der(), "same key reloaded from PEM -> same pubkey");
    }

    #[test]
    fn pem_matches_real_engine_pkcs1_format() {
        let a = LiveSourceAuth::generate();
        let pem = a.to_pkcs1_pem();
        assert!(pem.starts_with("-----BEGIN RSA PRIVATE KEY-----"));
        assert!(pem.trim_end().ends_with("-----END RSA PRIVATE KEY-----"));
    }

    #[test]
    fn rejects_garbage_pem() {
        assert!(LiveSourceAuth::from_pkcs1_pem("not a key").is_err());
    }

    /// Cross-check against a REAL engine-produced `.sauth` file + its transport's `pubkey`
    /// field, captured in note 25 (docs/protocol/notes/25-source-node-b0-groundtruth.md).
    /// This is the exact key/pubkey pair confirmed byte-for-byte via `openssl rsa -pubout`.
    #[test]
    fn matches_captured_real_engine_key_and_pubkey() {
        let pem = "-----BEGIN RSA PRIVATE KEY-----\n\
MIIByQIBAAJhANWhf9+R3J9ahrC4TgA8INMYGRPcqH7duMMzjPujeKe0VNwkoFbO\n\
zuqnTj0SVG4G71zFOUHjkQaZqOyj+tlB+gX8VXIyCwa4qYWgqG8WecY/KEGqw3u3\n\
ttsqNZtdYP3SowIBAwJgI5rqpUL6Go8WcsliVV9azdlZg09xanpJdd3s1Js+xp4O\n\
JLDFY80ifHE3tNhjZ6vSQUZhcWWa8rgGmXUKb5DV26lbwBmXIZqDThFraVC0TUlg\n\
VRYz2NZBpUmrFU/QIXTPAjEA2GtgXaalo/gXRyTqHGKvHir3Y0qzQ3/X+Mk9XJA6\n\
DkW9qfeCCfpRXQd69VZ2oRDlAjEA/LOQO9tJslFqDMDSH3pHwdk3jk3M+Zm9uG7m\n\
mqIF6EEomS4KXLfbnmi4JigJlATnAjEAkEeVk8RubVAPhMNGvZcfaXH6QjHM16qP\n\
+zDTkwrRXtkpG/pWsVGLk1pR+ORPFgtDAjEAqHe1fTzbzDZGsys2v6bagTt6Xt6I\n\
pmZ+evSZvGwD8CtwZh6xkyU9FEXQGXAGYq3vAjAU/ftYqb721J8ZV++r45NvFtWA\n\
lFnyUtHCJzkA4A01o5sLQecDwC9Zvq0jlUltCeM=\n\
-----END RSA PRIVATE KEY-----\n";
        let expected_pubkey_der = "307a300d06092a864886f70d01010105000369003066026100d5a17fdf91dc\
9f5a86b0b84e003c20d3181913dca87eddb8c3338cfba378a7b454dc24a056ceceeaa74e3d12546e06ef5cc53941e\
3910699a8eca3fad941fa05fc5572320b06b8a985a0a86f1679c63f2841aac37bb7b6db2a359b5d60fdd2a3020103";

        let auth = LiveSourceAuth::from_pkcs1_pem(pem).unwrap();
        let der_hex: String = auth.pubkey_der().iter().map(|b| format!("{b:02x}")).collect();
        assert_eq!(der_hex, expected_pubkey_der);
    }

    #[test]
    fn split_piece_splits_off_the_trailing_signature() {
        let piece = [1u8, 2, 3, 4, 5];
        let (payload, sig) = split_piece(&piece, 2).unwrap();
        assert_eq!(payload, &[1, 2, 3]);
        assert_eq!(sig, &[4, 5]);
    }

    #[test]
    fn split_piece_rejects_a_piece_shorter_than_the_signature() {
        assert!(split_piece(&[1, 2], 5).is_none());
    }

    // Ground truth for the signing-scheme crack (note 27): a real official-engine source
    // node's private key, captured by hooking its own PyCryptodome SHA1 calls while it
    // signed real pieces (docs/protocol/notes/27-b0-signing-cracked.md), plus two real
    // signed pieces it produced (tests/vectors/live-source-auth/piece-{0,1}.bin). This is
    // NOT outpace's own key — it's the official engine's, used here purely to prove our
    // sign/verify implementation matches the real one byte-for-byte.
    const CAPTURED_SOURCE_PEM: &str = "-----BEGIN RSA PRIVATE KEY-----\n\
MIIByQIBAAJhALTKEtVQMXdmmIC0nHQmq4Htj0hiqqTCUmcUXdE7CKBd/c4KH/Cj\n\
lbBIKcaX+eDlSnMsdfNkR3t8z5VISA2NejZJGBGgvnroMiJzMJg5ZwGOvICoxuwX\n\
b0IBpo2Vic9s/QIBAwJgHiGtzjgIPpEZasjEvgZx6vztNrsccMsNu9i6TYnWxWT/\n\
olcFUsXuSAwG9m6ppXuMIDpHjqjd9xuuK5uHesYQDFgmEeBOooVyC50WjezWByXZ\n\
fh3C6Hiors46ytmwcDK1AjEAvT3G6vodnmK5DPUXYqGpxfe6NCuC/tzH+dwsNWu7\n\
rz/6oOuxL13DXF3bYe32Bo0TAjEA9JEBsHT2EnQBgq4DykdwJkB5cjNjrOq94uh9\n\
D0CnJ2uo6wqESeW3zM5qao1xJ6+vAjB+KS9HUWkUQdCzTg+XFnEupSbNcldUky/7\n\
6B148n0ff/xrR8t06SzoPpJBSU6vCLcCMQCjC1Z1o062+AEByVfcL6AZgFD2zO0d\n\
8dPsmv4KKxoaR8XyBwLb7nqIiZxHCPYadR8CMQCDtIDEcZ3dZEQkotxtIbErQSCr\n\
w7burR2QqD9OS/n92TGQ2S3yyv5k6oG00re0J44=\n\
-----END RSA PRIVATE KEY-----\n";

    fn captured_piece(idx: u32) -> Vec<u8> {
        let path = format!(
            concat!(env!("CARGO_MANIFEST_DIR"), "/../../tests/vectors/live-source-auth/piece-{}.bin"),
            idx
        );
        std::fs::read(path).expect("real captured piece vector")
    }

    #[test]
    fn signature_len_is_96_for_the_captured_768_bit_key() {
        let auth = LiveSourceAuth::from_pkcs1_pem(CAPTURED_SOURCE_PEM).unwrap();
        assert_eq!(auth.signature_len(), 96);
    }

    #[test]
    fn verify_piece_passes_on_real_captured_pieces() {
        let auth = LiveSourceAuth::from_pkcs1_pem(CAPTURED_SOURCE_PEM).unwrap();
        let pubkey_der = auth.pubkey_der();
        for idx in [0, 1] {
            let piece = captured_piece(idx);
            let (payload, sig) = split_piece(&piece, auth.signature_len()).unwrap();
            assert!(
                verify_piece(&pubkey_der, payload, sig),
                "piece {idx}: real captured signature must verify"
            );
        }
    }

    #[test]
    fn verify_piece_rejects_a_tampered_payload() {
        let auth = LiveSourceAuth::from_pkcs1_pem(CAPTURED_SOURCE_PEM).unwrap();
        let pubkey_der = auth.pubkey_der();
        let piece = captured_piece(0);
        let (payload, sig) = split_piece(&piece, auth.signature_len()).unwrap();
        let mut tampered = payload.to_vec();
        tampered[0] ^= 0xFF;
        assert!(!verify_piece(&pubkey_der, &tampered, sig));
    }

    #[test]
    fn resigning_the_real_payload_reproduces_the_exact_captured_signature() {
        // PKCS#1 v1.5 has no randomized padding, so signing the same payload with the same
        // key is deterministic: our implementation must reproduce the *exact* bytes the real
        // engine produced, not just "a" signature that happens to verify.
        let auth = LiveSourceAuth::from_pkcs1_pem(CAPTURED_SOURCE_PEM).unwrap();
        for idx in [0, 1] {
            let piece = captured_piece(idx);
            let (payload, real_sig) = split_piece(&piece, auth.signature_len()).unwrap();
            let our_sig = auth.sign(payload);
            assert_eq!(our_sig, real_sig, "piece {idx}: re-signing must match the real engine byte-for-byte");
        }
    }
}
