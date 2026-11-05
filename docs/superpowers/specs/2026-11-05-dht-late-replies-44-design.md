# Late UDP qualification for #44

This increment extends the existing offline comparison harness only. Issue44 belongs to milestone8 and still requires a live comparison/recommendation; this does not adopt a library or authorize public networking. The current custom callback already rejects elapsed>=budget: do not reimplement that completed change. Earlier plan text describing it as absent is stale.

## Approach and limits

Use actual custom/mainline/n0 lookups in their existing loopback-only owned PID/network namespaces. A synthetic seed observes a real get_peers query, retains the matching transaction and sends a valid ten-peer response only after the lookup's15second measurement budget, with a16second server delay anchored after that query. Keep the server alive independently of the lookup result and join it before assertions. Require actual query and successful late-send witnesses, zero admitted peers/no first-peer timestamp, no target terminal and bounded lookup completion. The original target fixtures remain positive controls. A target accepted beyond budget or a lookup exceeding the bound fails. Early library exhaustion can still satisfy this case without exercising the adapter deadline; existing clock-unit tests separately exercise that guard.

Early frontier exhaustion is a legitimate observed difference: record it separately, and do not claim the custom lookup remained pending until15seconds, that a late datagram was consumed by a terminated lookup, or that an underlying library internally rejected it. This proves the comparison record excludes beyond-budget reply opportunities and its external process owner cleans up. It does not prove library actor joining, production stream cancellation or public destination confinement.

Do not force libraries to remain pending or change production budget/algorithm to make a fixture look equivalent. Mock-only elapsed checks already exist; they complement rather than replace this real-socket boundary. No new production API, manifest/dependency/lock changes or live worker entry. All peers/IDs synthetic. Namespace guard must run before sockets, external owner remains the hard bound, and all fixture tasks must join before assertion failures escape.

## Bound and ownership

Separate fixed late-replies qualification profile: exactly3cases,22second execution timeout each,80second whole-block deadline including existing2.1second per-worker cleanup allowance. Fail closed on missed query/send witness, nonzero or zero-test execution, timeout/truncation/unknown namespace cleanup; no automatic retries or extension. Existing9case and cancellation profiles stay unchanged. Private logs/pins remain bounded by existing runner. Runtime active #195 closure files cannot be modified.
