# Parser fuzzing

outpace parses bytes from untrusted peers and sources: bencoded metadata, peer-wire frames
and handshakes, peer-exchange and `ut_metadata` extension payloads, and MPEG-TS media. The
fuzzing setup checks that those parsers never panic and keep their round-trip and bounds
invariants on arbitrary input.

It has two parts:

| Part | Where | Runs |
|---|---|---|
| Invariants and committed seeds | `crates/ace-fuzz` | Every `cargo test --workspace`, so ordinary CI |
| libFuzzer targets | `fuzz/` (separate workspace, nightly) | By hand, when you want a mutation campaign |

`crates/ace-fuzz/src/lib.rs` holds one invariant function per target. The seed replay test
(`crates/ace-fuzz/tests/corpus.rs`) feeds every file under `crates/ace-fuzz/corpus/<target>/`
through it, and a positive-control test proves the seeds reach the parsers' accepted paths
rather than only their rejection paths.

## Targets

| Target | Parsers | Key invariants |
|---|---|---|
| `metadata` | bencode prefix parsing, metainfo decoding | Re-encoding round-trips; piece geometry is consistent and never allocated from advertised sizes |
| `peer_frames` | peer-wire messages, AceStream and BitTorrent handshakes | Decoded frames re-encode byte-for-byte; a frame is never emitted before its last byte |
| `peer_extensions` | `ut_metadata`, peer exchange, peer announce, window ranking | Messages round-trip; peer counts are bounded by input size; ranking order is stable |
| `media_resync` | MPEG-TS resync, PCR timing, HLS segmentation | Output is packet-aligned and identical however the input is chunked; segments concatenate back to the input |

Inputs are capped at 16 KiB (`ace_fuzz::MAX_INPUT`).

## Running a campaign

The libFuzzer targets need a nightly toolchain and `cargo-fuzz`:

```sh
rustup toolchain install nightly
cargo install cargo-fuzz --locked

# From the repository root. The committed seeds are read-only input; new corpus
# entries go to the gitignored fuzz/corpus/<target>/ directory.
cargo +nightly fuzz run metadata fuzz/corpus/metadata crates/ace-fuzz/corpus/metadata \
  -- -max_total_time=600 -max_len=16384
```

Replace `metadata` with `peer_frames`, `peer_extensions` or `media_resync`. A crash writes
its input to `fuzz/artifacts/<target>/`; reproduce it with
`cargo +nightly fuzz run <target> fuzz/artifacts/<target>/<file>`.

## Adding seeds

When a campaign finds a crash, fix the parser, then minimize the input
(`cargo +nightly fuzz tmin <target> <file>`) and commit it under
`crates/ace-fuzz/corpus/<target>/` with a descriptive name, so the regression runs in CI from
then on. Seeds must be synthetic: never commit captured traffic, private captures, or
anything containing a real content id or infohash (see the identifier hygiene rules in
`AGENTS.md`).
