# Original-engine baseline, 2026-10-05

Validated the harness against the official 3.2.11 engine image and a candidate
built from main at `af5b176` with `cargo build -p ace-engine --bin outpace`.
The runner captured **cid9**, resolved by the operator, simultaneously for 180
seconds with fresh state, host networking and default candidate playback settings.
No WARP interface or other playback client was present. Raw evidence remains
outside the repository; this document contains no live identifiers or stream names.

| Measurement | Original engine | Current-main candidate |
| --- | ---: | ---: |
| Delivered bytes | 229,071,848 | 211,880,512 |
| First byte | 1.113 s | 24.427 s |
| First video RAI | 1.113 s | 24.427 s |
| Three-second player estimate start | 1.313 s | 24.477 s |
| Stale-prefix suspect | none | none |
| Sync losses | 1 | 0 |
| Exact repeated media packets | 0 | 0 |
| Exact repeated PSI/SI packets | 0 | 631 |
| Continuity errors on SDT PID | 0 | 210 |
| Largest arrival gap | 10.144 s | 10.260 s |
| Player estimate stalls / time | 1 / 2.25 s | 0 / 0 s |
| ffmpeg decoder diagnostic lines | 65 | 18 |

The shared PCR window after edge margins was 171.52 seconds. Packet identity was
99.9999056% of reference packets present in the candidate and 99.9999055% of
candidate packets present in the reference. The unique-media mapping contained
two long matching runs separated by one unmatched packet, one missing reference
packet and no backwards/duplicate steps. It skipped 76,878 ambiguous reference
occurrences and 74,965 ambiguous candidate occurrences.

Repeated tables were PAT 210, PMT 210 and SDT 211; they were excluded from media
identity. Candidate continuity errors were SDT 210 and one each on PAT, PMT,
audio and teletext PIDs. Reference continuity was reset at its sync loss, so zero
reported reference CC errors does not establish flawless reference output.

Both streams paused around wall time 103 seconds and had a backward PCR step
around 113 seconds (reference -4.48 s, candidate -4.16 s). Frontier difference was
zero at all sampled times from 30 through 170 seconds. Estimated candidate buffer
was initially about 8.27 seconds deeper, explaining why its player estimate
survived the common pause. These estimates cannot establish decoder superiority.
At 30 seconds, buffer depth was 8.59 seconds reference / 16.86 seconds candidate;
at 170 seconds it was 12.04 / 18.02 seconds.

`ffprobe` found matching H.264 video, AC-3 audio and DVB teletext streams; video was
1920×1080. Full video/audio decode checks exited successfully for both outputs.
Their stderr line counts include mid-GOP startup and timestamp/discontinuity
messages and are not standardized error scores.

The 23.31-second first-byte difference is baseline evidence for the existing
startup work ([#166](https://github.com/jamezrin/outpace/issues/166),
[#167](https://github.com/jamezrin/outpace/issues/167)); this run does not assign all
of that difference to either cause. Repeated SDT continuity belongs to
[#172](https://github.com/jamezrin/outpace/issues/172). No Rust behavior was changed.
This run did not reproduce a stale-prefix or permanent no-upstream recovery
failure and does not prove those existing issues resolved.

Both owned clients were removed at completion; the pre-existing BuildKit container
remained. Offline tests cover the deadline and cleanup boundaries. Results are
one source-dependent run with a debug candidate build, not a performance guarantee;
re-run using the README command and a release build for release comparisons.
