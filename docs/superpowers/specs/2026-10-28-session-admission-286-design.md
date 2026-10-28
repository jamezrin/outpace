# Configurable provider-session admission (#286)

Status: user approved design and implementation. Unlimited default selected explicitly. Implementation and independent review are complete, including the shutdown correction. Publication and CI are pending; no milestone closure or release is claimed.

## Contract and scope

Add `OUTPACE_MAX_SESSIONS`: absent or `0` means unlimited; a positive integer caps concurrently admitted provider resources. Reject negative, malformed and overflowing values at startup. Preserve existing defaults and `outpace play` behavior. This is a daemon admission limit, not authentication or a hard RSS ceiling. A separate aggregate seed-store byte budget is deferred.

One manager-wide admission controller covers live provider sources and resolved VOD resources. Native HLS and compatibility HLS share their underlying live source's slot; an HLS wrapper does not consume another slot. Metadata-only preparation does not acquire a slot. Existing same-key fan-out remains available when full; admission is checked after cached-resource lookup and same-key serialization, before provider open/resolve work. Pending opens count toward the limit. There is no waiting queue at saturation.

## Lifetime and concurrency

Use a private admission module with an atomically checked count/optional maximum and an owned RAII reservation. Reserve before the first provider await; failures and cancelled opens release automatically. Do not hold an admission mutex across provider I/O. Unlimited mode still tracks current occupancy for operators.

A live source decorator owns its reservation and forwards every TsSource method, including discontinuity, metadata and stats handle. The pump owns this decorator; dropping a manager map entry must not free capacity while the source is still retained. Pump abort is asynchronous, so replacement admission may remain refused until source destruction is observed. End-of-stream drops the source and releases the slot even if a completed session entry remains cached.

A VOD decorator holds shared ownership of the original content plus its reservation. Every opened VodByteSource retains that same owner, so cache expiry or handle removal cannot release a slot while range bodies still use the resource. Multiple ranges of one retained content share one slot. A new uncached resolution may require a new slot while old retained ranges remain active; do not pretend unrelated generations are one resource. Preserve existing VOD caching and live/HLS key serialization rather than adding a global provider lock.

Keep reservation alive through inner resource destruction, including ordinary failure paths. Count bounds admitted provider ownership, not the exact number of OS sockets during asynchronous provider cleanup; this change does not refactor provider shutdown. Stops/reap/shutdown release capacity when the actual owners are destroyed, not merely when their keys disappear.

## Errors and visibility

Add a distinct capacity-exhausted ProviderError variant. Native streaming and VOD/HLS start endpoints return HTTP 503 and `Retry-After: 1` for that error. Compatibility JSON retains `{ "response": null, "error": "session capacity reached" }`; direct compatibility start failure returns 503 before media starts. Preserve all other error mappings and lazy-start behavior: metadata-only compatibility negotiation may succeed before later playback admission fails.

Log refusals with `alog!("[manager] ...")`, using fixed diagnostic text and numeric counts without content selectors. Expose occupied admission count and configured maximum (`null` for unlimited) in the existing native status and supported service-status surfaces; retain existing active_sessions meaning instead of silently redefining it. Document that occupied includes pending starts and retained resources.

Configuration flows from daemon config/environment into StreamManager. Preserve existing public constructors with unlimited behavior where practical, adding an explicit configured constructor/builder rather than forcing unrelated test changes. No dependency, networking, cache-default, seeding or container changes.

## Tests and acceptance

Synthetic providers and loopback HTTP only; no registry or real identifiers.

- Default/zero unlimited, positive cap, invalid config rejection and operator status fields.
- N+1 distinct opens refused before provider work; concurrent opens cannot exceed N; existing live/VOD and direct/HLS fan-out succeeds at the limit.
- Pending-open cancellation, provider error and source completion return capacity; unrelated opens remain concurrent.
- Stop/reap with retained session handles does not free prematurely; capacity is eventually reusable after pump/source destruction. Shutdown cannot leak reservations.
- VOD cache expiry, retained handles and open ranges retain one shared reservation until final resource destruction. Cancelled range creation cannot leak a new resource.
- HLS new-source failure/cancellation and compatibility ownership preserve existing cleanup contracts without double counting.
- Native 503/Retry-After and compatibility JSON/direct errors preserve successful response shapes and existing mappings.
- Existing playback/provenance/discontinuity/VOD/broadcast tests remain passing; workspace tests, Clippy, formatting and identifier hygiene gate publication.

Independent review must scrutinize actual lifetime ownership, all endpoint mappings and same-key race/cancellation paths. No deployment is implied by this design. v0.9.0 remains open until its remaining issues and acceptance criteria are satisfied; no release is due yet.
