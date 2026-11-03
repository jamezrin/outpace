# Autonomous support lifecycle baseline (#270)

This increment establishes a synthetic source → support → leecher prerequisite for
Post-1.0 issue #270. It does not introduce or claim an autonomous service.

Use the real live follower and independent inbound admission/serving paths over
loopback TCP. The source publishes two signed synthetic MPEG-TS pieces. The support
owns a capacity-one output receiver without a player; its follower must acquire and
publish a verified whole piece that an independent leecher retrieves byte-for-byte.
Then closing the service-owned receiver must join the follower and release its
producer registry lease. Retained inbound connections are separately owned and
joined: removing a producer lease does not revoke snapshots already admitted.

The baseline keeps the output receiver undrained while proving reseeding. This
checks publication independence from consumer progress, rather than asserting that
a discard consumer is the final service architecture. The cache holds four wire
pieces, inbound admission is one connection, all peers use ephemeral loopback
ports, and observation/cleanup have explicit deadlines. No discovery, tracker,
external source, decoder or closed-engine interaction is involved.

Alternatives: manual store injection on the support would miss acquisition and
producer ownership; a new store-only follower would preempt the evidence and
introduce cursor/scheduling semantics. Reuse existing production paths first.

Subsequent service design must separately decide source configuration, bounded
restart/backoff, shared provider admission, store/backend lifetime, inbound task
ownership, announcements, status and operator stop/join. This baseline cannot close
#270, establish long-running resource behavior or official-engine interoperability.
