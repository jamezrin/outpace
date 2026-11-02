# Explicit VLC diagnostic verbosity (#246)

Status: implemented and independently reviewed; CI and merge pending. This increment does not diagnose or fix the historical playback interruption.

Add `--diagnostic-verbosity {0,1,2}`, default zero. A nonzero level requires `--timestamped-diagnostics`. Apply the explicit level only to playback clients; the version probe remains unchanged. Record the level in private diagnostic configuration so evidence is interpretable. Reject invalid programmatic values before filesystem/process side effects. Existing playback argv remains identical at zero.

Reuse the owned bounded selector capture and its retention/draining, cancellation and cleanup contract. Do not add a media proxy, queue, detached task or unbounded log; do not change player buffering or engine behavior. Additional verbosity can increase drain CPU and hit the existing retention cap sooner. Exhausted capture remains incomplete, never evidence of message absence.

Logs are JSONL with base64 payloads. Interpretation must decode the bounded payload, use per-stream receipt intervals and distinguish observation from cleanup. A local synthetic finite-EOF/held-open probe produced EOF-like messages after cleanup in both cases, so aggregate phrase counts do not prove observation EOF. Verbose access/demux messages may supply positive evidence, but cannot alone prove remote wire behavior, causal ordering or player quality. No new live observation is admitted by this change.

Tests cover default argv compatibility, selected client-only verbosity, private configuration, invalid values and non-diagnostic rejection before launch. Existing capture bounds/cancellation tests remain unchanged. A later live profile must assess output volume and CPU perturbation before selecting verbosity; caps are not expanded here.
