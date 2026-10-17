# 67 — Injected direct-TS SDT continuity for #172

Date: 2026-10-17. Based on merged `main` at `58755d8`.

## Cause and change

A titled `KeyframeGate` filters upstream SDT and emits its own SDT at the opening
and later video access points. Each synthesized packet previously had continuity
counter zero. Repetition therefore produced PID-17 continuity errors in recent
controlled captures, including 166 errors in the #167 validation.

The output gate now owns a four-bit SDT counter. Every emitted synthesized SDT
uses the next value and advances modulo 16. Gate resets, discontinuity resets and
same-stream rearming retain that output sequence; a newly constructed gate starts
at zero. Only the TS header counter changes. SDT section bytes and CRC, as well as
cached upstream PAT/PMT packets, retain their original bytes.

Review also found a pre-existing filter gap: scan-budget fallback could publish
an upstream SDT as its triggering packet. SDT is now filtered before both gate
phases. Filtered packets still contribute to the scan budget, but fallback waits
for a forwardable packet. This keeps upstream SDT out of the owned sequence and
preserves the pending discontinuity marker for resumed media.

The shared table builder remains a packet template. These changes and validation
concern direct-TS gate emission; they do not certify native-HLS table continuity.
No exposed defaults or configuration change.

## Deterministic evidence

Three initial regressions failed against the original implementation, observing
repeated counter zero instead of advancing values. After the counter fix, review's
scan-budget regression also failed before the filter correction. All four now
pass:

- Twenty access-point emissions have consecutive counters, including 15-to-zero
  wrap, while SDT section and CRC bytes stay identical.
- All three gate-reset methods preserve the next counter.
- Filtered upstream SDT does not consume output counters. PAT/PMT bytes and their
  upstream counter progression remain unchanged, including one cached duplicate.
- An upstream SDT at the scan-budget boundary is filtered after rearming; resumed
  media receives its discontinuity marker and the next synthesized SDT uses one.

All 67 media tests passed. Independent code review's one actionable finding was
tested and fixed; follow-up review found no remaining actionable issues.

## Controlled live validation

A fresh-state simultaneous 180-second comparison used cid9 supplied manually
through the environment, the optimized candidate built from this patch, and the
original 3.2.11 engine with its default image command. Candidate playback used
default settings and the native content-id direct-TS route; the reference used
its compatibility route. Storage and listeners were isolated, with no other
known engine clients. No code or harness read the registry. Private artifacts
retain the binary identity and source diff because the release was built before
the final commit.

The host WARP tunnel was temporarily disabled under existing operator
authorization. Public routing used the ordinary gateway during capture and
returned to `wgcf-profile` afterward. Owned candidate/reference processes were
cleaned up; only the pre-existing BuildKit container remained.

| SDT measurement | Reference | Candidate |
| --- | ---: | ---: |
| Payload packets on PID 17 | 1,051 | 161 |
| Consecutive-counter transition errors | 0 | 0 |
| Correct 15-to-zero wraps | 66 | 10 |
| Analyzer PID-17 continuity errors | 0 | 0 |

Candidate counters began `0, 1, 2, ... 15, 0, 1`; every observed transition
advanced modulo 16. The MPEG-TS analyzer also reported zero continuity errors on
all other PIDs, zero transport sync losses and zero PCR jumps for both captures.
This meets #172's direct-TS live continuity criterion. Earlier source windows had
different packet counts; their results are not a controlled quantitative error-
rate comparison with this run.

Both captures passed FFprobe and full-file audio/video FFmpeg decoding with exit
zero. Decoder progress reported 3,565 reference and 6,351 candidate video frames;
stderr had 22 and two diagnostic lines respectively. These are observations, not
standardized quality scores or proof of smooth real-time playback. Different
captured history lengths and media spans prevent interpreting byte/frame totals
as comparative throughput or decoder superiority.

First body was 1.090554 seconds reference and 4.554864 seconds candidate. Candidate
reservoir release was `TargetDuration` at 30,720 ms selected-PCR duration. These
startup observations do not establish a latency improvement due to this SDT fix
or resolve the variable behavior remaining in #167.

## Final verification

`cargo test --workspace --locked` passed 968 tests with seven ignored. The release
build, `cargo clippy --workspace --all-targets --locked -- -D warnings`,
`cargo fmt --all --check`, identifier hygiene and `git diff --check` passed.
Raw media, logs, probe metadata and private source values remain outside the
repository. This validates the direct-TS SDT fix, not the broader playback/soak
acceptance in #1.
