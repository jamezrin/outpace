# Synthetic live transport vector

`synthetic-live-512k.bin` is an encrypted `AceStreamTransport` v2 descriptor
generated specifically for the #164 acceptance test. Its fields are:

| Field | Value |
| --- | --- |
| `name` | `Synthetic Live` (invented test label) |
| `piece_length` | 524,288 bytes |
| `chunk_length` | 16,384 bytes |
| `bitrate` | 1,000,000 |
| `authmethod` | `RSA` |
| `pubkey` | Generated 768-bit RSA SubjectPublicKeyInfo DER |
| `trackers` | `udp://tracker.invalid:80` |

The fixture contains no content id or recorded infohash. Tests derive its
infohash at runtime. OpenSSL generated the public key; its private key was
discarded. The sorted bencode dictionary was encrypted with the protocol's
AES-128-CBC key/IV and PKCS#7 padding, then prefixed with the transport magic
and version. No live descriptor, stream metadata, or source key was used.

`warm_infohash_vector_preserves_non_default_geometry` decodes this vector and
exercises the catalog-cache-to-infohash path. A separate generated-key test,
`warm_infohash_continuity_authenticates_pieces_before_emitting`, verifies that
the resolved descriptor configures RSA rejection, successful retry, and signature
tail removal in the actual download continuity constructor.
