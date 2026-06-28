# Task 8: Rust Harness — infohash = SHA1(transport file) — Validation

**Status:** VALIDATED

## Summary

`infohash_of_transport` (SHA1 of the full transport-file bytes, including the
18-byte `AceStreamTransport` magic prefix) reproduces both committed test vectors
byte-for-byte.

## Harness

Location: `re/harness/idmath/` (git-ignored; re-runnable with `cargo test`)

Functions implemented in `src/lib.rs`:

- `pub fn infohash_of_transport(bytes: &[u8]) -> [u8; 20]`
  SHA1 digest of the entire input slice.
- `pub fn is_transport_file(bytes: &[u8]) -> bool`
  True iff the slice starts with `b"AceStreamTransport"`.

Dependencies: `sha1 = "0.10"`, `hex = "0.4"`.

## `cargo test` output (green)

```
   Compiling idmath v0.1.0 (re/harness/idmath)
    Finished `test` profile [unoptimized + debuginfo] target(s) in 0.19s
     Running unittests src/lib.rs

running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s

     Running tests/vectors.rs

running 3 tests
test infohash_transport_02 ... ok
test rejects_non_transport ... ok
test infohash_transport_01 ... ok

test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s

   Doc-tests idmath

running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s
```

## Vectors validated

| File | Expected infohash | Result |
|------|-------------------|--------|
| `tests/vectors/transport-01.bin` | `34df422b80a4bd94ac1e51be9ede60364ec7a7dd` | PASS |
| `tests/vectors/transport-02.bin` | `ed2c05b3b022e9cc7b7c1ca46d20f10839dc4108` | PASS |

## Notes

- content_id derivation is out of scope for this task (OPEN — see `transport-file.md`).
- The harness lives at `re/harness/idmath/` which is git-ignored; only this note
  is committed. To re-run: `cd re/harness/idmath && cargo test`.
