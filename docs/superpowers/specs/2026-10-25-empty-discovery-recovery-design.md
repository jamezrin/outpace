# Completed-empty initial discovery recovery

Status: user approved implementation with “So fix it” after the concrete policy recommendation.

A cold resolved-live open currently fails when its initial discovery finishes without candidates, before live-session rediscovery can run. A deterministic loopback regression reproduces the failure and its healthy-first control delivers media.

## Contract

- At most two initial discovery generations in one resolved-live open, with one second between a fully completed-empty first generation and the second.
- One absolute ten-second initial-discovery budget begins on entering the discovery path, includes warm-cache initialization, waiting and retry gap, and never resets. It does not cover descriptor resolution, peer handshakes, first media or HLS readiness.
- A still-running generation retains ownership and its late candidates. Never restart it just because it has not yielded yet.
- First candidates or warm hints retain the current run and transfer it to the existing live follower. Explicit bootstrap peers keep their existing direct path.
- Two completed-empty generations fail without a third. A hanging first/later generation fails at the original budget. Expiry cannot admit late candidates.
- Dropping the opening future cancels owned discovery and retry waiting; no detached retry work. Preserve existing connection fairness, caps, cooldowns, verification, HLS deadlines and error category.
- No public configuration/API, dependencies, live probes, registry reads or unrelated diagnostic code.

## Approaches

Selected: bounded provider-local initial-discovery retry, the smallest fix to the reproduced gate. A whole-request deadline would require shared manager/HLS ownership changes and is deferred. Unbounded retries or longer tracker/peer deadlines do not satisfy the agreed bound.

## Evidence

Tests must cover same-call second-generation real media, direct-first success, no overlapping generations, all-empty attempts/gap, hanging first/second, original deadline not reset, exact expiry/late peer rejection, cancellation in gap and second generation, warm/bootstrap behavior. Existing late-discovery and initial-pool controls remain relevant. Time bounds in tests must be distinguished from production policy.
