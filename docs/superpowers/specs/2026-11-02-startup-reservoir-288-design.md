# Packet-slice startup observation (#288)

Status: implemented and independently reviewed; 21 focused startup tests, the full engine suite (708 passed, zero failures, one ignored benchmark), and repository lint/format/hygiene gates pass. Merged through PR #316 after successful CI. Independent increment from the peer receive-buffer PR.

Replace StartupReservoir's byte-at-a-time observation loop with borrowed complete 188-byte packet slices. Fill only an existing partial packet, then observe aligned slices directly, and retain a trailing partial packet. Use checked slice-to-array references for ts_timing; no unsafe code or new parser, dependency, configuration or public interface.

Keep every clock observation in the original order. A reset clears first-clean timing/release-duration/release-byte state exactly as before; a backward-PCR reset re-observes the reset packet only when its explicit discontinuity flag is absent. Calculate each reset packet's start using the previous queued bytes plus the current packet's ending offset minus 188. Preserve the last reset in a chunk, including resets whose prefix belongs to an earlier chunk. Empty input, invalid timing packets, selected/other PIDs, clock wrap and trailing fragments must behave identically.

The existing queue, discard-prefix, deadline, fallback bitrate, cap-plus-one-chunk, cancellation and release machinery stays unchanged. A short helper consuming TsTiming may share reset handling between the carried and borrowed packet cases. No full-chunk normalization, sync search, reframing or startup-policy change.

Before production changes, add differential guards against the retained old byte-loop oracle for every two-way split of a bounded synthetic sequence containing explicit and backward resets, clock wrap, another PID and a partial tail. Check returned retention offsets, retained partial bytes and clock/reset state after each call, not only final output. Existing full-wrapper reset/cap/deadline tests remain intact.

Record a bounded warmed release baseline and identical after-measurements for aligned chunks and adversarial split input. Measure scanner elapsed time separately from output queueing, I/O and decoding. No allocator/RSS or live startup claim follows from these microbenchmarks. Source reasoning supports removal of per-byte pushing and complete-packet copies; partial packets still require bounded copying.

Run focused startup tests and affected engine gates, then independent review and normal CI. Do not close #288: shared message/store copying and dependency-policy work remain separate. No deployment or live window is needed for this internal scanning increment.
