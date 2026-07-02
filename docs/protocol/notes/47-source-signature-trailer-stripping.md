# 47 - Source signature trailer stripping fixes per-piece frame drops

Date: 2026-07-02

## Symptom

On the healthier public target `content_id=cid3`
(catalog-resolved infohash `d123456789abcdef0123456789abcdef01234567`), playback was
continuous but visibly stuttery and dropped frames.

## Root cause

Dumping the raw pre-`TsResync` piece stream showed that every Acestream live piece is:

```text
[piece_length - sig_len bytes of continuous MPEG-TS][sig_len-byte trailing RSA source signature]
```

For the standard 768-bit source key, `sig_len=96`. This matches the B0 signing scheme from
note 27: the RSA signature is embedded in-band as the tail of the piece.

The download path was reassembling the whole piece, signature included, and passing it to
`TsResync`. `TsResync` then re-locked at each 1 MiB boundary by discarding bytes, which removed
the signature junk but also dropped one straddling video packet per piece. The measured result
was 42/44 continuity-counter discontinuities on the video PID, one per piece.

## Fix

`PieceReassembler` gained `with_piece_trailer(sig_len)`, which strips the trailing source
signature only when emitting media bytes. The wire/relay path remains unchanged: `PieceStore`
still stores full signed pieces so seeding stays wire-compatible.

`StreamInfo.sig_len` is derived from the transport `pubkey` RSA modulus via
`ace_wire::live_auth::signature_len_from_pubkey_der`. Bare-infohash playback defaults to the
standard 96-byte signature.

## Verification

Live capture after the fix:

- 60 s capture on `cid3`.
- Output grew from 41.5 MB to clean MPEG-TS.
- Continuity-counter discontinuities went `44 -> 0`.
- Macroblock decode errors went `18 -> 1`; the remaining decode error is the expected
  mid-stream join point.
- PTS gaps over 0.1 s went `1 -> 0`.
- `ffprobe` identified 1920x1080 H.264 plus 48 kHz AAC.

`cid3` is now the preferred continuous-playback smoke
target.
