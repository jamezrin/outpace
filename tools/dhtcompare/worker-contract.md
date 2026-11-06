# Worker record and request contract v1 (#44)

This is an offline research contract, not public-network authorization. The
request/parser and record validator do not authorize sockets by themselves.
A worker entry must first validate its fixed arguments and actual isolated
namespace. The parent must own the PID namespace, deadline, pipes and cleanup.

## Protected input

The comparison environment has exactly these four prefixed fields; unrelated
system environment is separate. The parent removes inherited fields in this
prefix and inserts the intended four. Workers reject duplicate or unknown names,
non-Unicode relevant values, missing fields and mismatched roles.

- `OUTPACE_DHT_COMPARE_MODE=offline-v1`
- `OUTPACE_DHT_COMPARE_ROLE=mainline`, `n0`, or the fixed custom entry's `custom`
- `OUTPACE_DHT_COMPARE_TARGET`: exactly twenty hex-decoded bytes
- `OUTPACE_DHT_COMPARE_SEEDS`: one to four unique numeric IPv4 endpoints with
  nonzero ports; at most 256 bytes, and only loopback addresses in offline mode

No identity or endpoint is accepted through argv or emitted in a result, error or
Debug representation. The registry is never read. Valid parsing is only one
prerequisite; no public mode is exposed by this contract.

## One bounded result

A payload is one UTF-8 JSON object of at most 8,192 bytes followed by LF. Duplicate
or unknown keys, nonfinite numbers, booleans masquerading as integers, multiline
payloads and extra records are invalid. Required fields:

| Field | Constraint |
|---|---|
| `schema_version` | integer 1 |
| `candidate` | expected `custom`, `mainline` or `n0` |
| `terminal` | `target`, `exhausted`, `deadline` or `adapter_error` |
| `peer_count` | integer 0 through 1,024 |
| `first_peer_seconds` | null iff zero peers, otherwise finite, nonnegative and no greater than elapsed |
| `elapsed_seconds` | finite, nonnegative, no greater than the parent 30-second envelope |
| `batch_sizes` | first min(observed, 64) raw emission sizes, each unsigned 64-bit |
| `batches_observed` | unsigned 64-bit count |
| `batch_history_truncated` | exactly whether observed exceeds 64 |
| `target_overshoot` | exactly max(peer count minus 8, 0) |
| `nodes_queried`, `valid_responses` | custom numeric diagnostics when actually collected; otherwise null |
| `diagnostics_unavailable_reason` | null with known custom counters, `not_collected` for unavailable custom counters, `library_not_exposed` for upstream counters |

Nontruncated raw emission sizes must account for at least all retained peers;
raw sizes are not limited to the retention cap. A positive peer count requires
an emission. Target completion requires at least eight peers; deadline/exhaustion
cannot claim that target. Adapter errors are reported failures, not successful
discoveries. Elapsed time may exceed 15 seconds when a constructor returns late;
this record is not proof that an async timeout preempted blocking work.

Upstream stdout must contain only the payload. Custom libtest stdout uses one
reserved `DHT_COMPARE_RECORD ` line containing that payload. Its parent also
requires the exact selected test and exactly one passed/zero failed/zero ignored
summary, a zero exit, complete untruncated pipes and known namespace cleanup.
Both normal deferred and serial nocapture libtest name/pass framing are allowed.
An arbitrary JSON-looking line or a stale zero-test success is never accepted.

`worker_records.py` validates the shared wire contract; `vectors/worker-v1.json`
contains synthetic conformance examples. These vectors and parser tests do not
prove a real worker's namespace, network policy, lifecycle or exec integration.
