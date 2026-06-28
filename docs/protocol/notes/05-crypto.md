# Task 5 — Encrypter / peer handshake (RESOLVED for public content)

Captured live via Frida socket-level hooks (`re/harness/sockcap.js`) on the
official engine streaming content_id `f8b0…` (infohash `50e935…2d6e47`), across
two fresh sessions. Cross-checked against the raw pcap.

## Headline result
**For public content (`is_encrypted=0`) the peer link is PLAINTEXT standard
BitTorrent** — there is no encryption to reverse-engineer. The `Encrypter`/AES
path (`m2_AES_*`, `block_encrypt`, `xor_encrypt`) is only exercised for
`is_encrypted=1` (premium/protected) content, which is OUT OF SCOPE.

Evidence: `AceStreamProtocol` pstr appears 138× in `session.pcap`; post-handshake
bytes are readable bencode (`d20:ace_metadata_versioni…`, `d11:ut_metadatai…`,
`d20:distance_from_sourcei…`) — not ciphertext.

## Peer handshake — exact wire format (66 bytes)
Identical layout to the BitTorrent handshake, with a custom protocol string:

| Offset | Len | Field | Value observed |
|---|---|---|---|
| 0 | 1 | pstrlen | `0x11` = 17 |
| 1 | 17 | pstr | `"AceStreamProtocol"` |
| 18 | 8 | reserved | `00 00 00 00 00 00 00 00` (all zero) |
| 26 | 20 | infohash | `50e93529d3eb46a50506b14464185a15292d6e47` |
| 46 | 20 | peer_id | e.g. `R30------Ef2V8QOgmt4` |

Vectors: `tests/vectors/messages/encrypter-handshake-{pstr,peer-in,full-out}.bin`.
(The `pstr` file is the 46-byte prefix through the infohash; the 66-byte files
include the trailing peer_id.)

### peer_id format
`R30------` + 11 random alphanumerics (e.g. `Ef2V8QOgmt4`, `gGwaDryAo5A`). The
`R30` is the client/version tag; the random suffix is **regenerated per session**
(confirmed: differs across two restarts, and dozens of distinct remote peer_ids
all share the `R30------` prefix). → **ephemeral identity, no account binding.**

### reserved bits
All-zero in the captured handshake, yet the peers still exchange BEP-10 extended
messages (id 20). So Acestream peers assume extended-message support implicitly
rather than signalling it via the standard reserved bit `0x100000`.

## Post-handshake: BEP-10 extended handshake
First message after the handshake is a standard length-prefixed BitTorrent
message: `<4-byte len><0x14 = id 20><0x00 = ext handshake><bencoded dict>`.
Observed dict (vector `encrypter-handshake-ext-msg-trunc.bin`):
```
d 20:ace_metadata_version i1e
  3:asn i3352e
  11:asn_country 2:ES
  13:geoip_country 2:ES
  3:lsp i14691020e
  1:m d 11:ut_metadata i2e e          # extension id map (ut_metadata=2)
  2:mi d 20:distance_from_source i1e
        9:down_rate i1274580e
        19:download_window_… …        # Acestream live-streaming metrics sub-dict
  …
```
Acestream-specific keys carry live-streaming/topology info
(`distance_from_source`, `down_rate`, `download_window…`, `ace_metadata_version`)
alongside the standard `ut_metadata` extension. These drive the live piece logic.

## Answers to the unknowns
- **Unknown #1 (identity):** RESOLVED — ephemeral, locally generated; no
  account/server identity needed (see also `02-test-streams.md` identity-wipe test).
- **Unknown #4 (encryption):** RESOLVED for scope — public content is plaintext;
  AES `Encrypter` only for premium `is_encrypted=1` (out of scope).

## Implementation implication
`ace-peer` can be a **standard BitTorrent peer** with two changes:
1. handshake pstr = `"AceStreamProtocol"` (len 17),
2. implement the BEP-10 extended handshake with the Acestream metadata keys and
   `ut_metadata`, plus the live-streaming extension semantics (the `mi` metrics +
   the live piece picker — see `08-key-finding-bittornado-lineage.md`).
No encryption layer is required for public streams.

## OPEN
- Exact semantics/units of the `mi` live-metrics dict and any additional Acestream
  extension messages (beyond the handshake) used for live piece negotiation.
- The `Encrypter` AES key schedule for `is_encrypted=1` content — deliberately not
  pursued (out of scope).
