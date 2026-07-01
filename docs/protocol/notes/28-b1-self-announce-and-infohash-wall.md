# 28 — B1 self-announce fix, and the infohash-formula wall (Task 7 reverse direction)

Follow-up to note 27 (B0 cracked). While pursuing Task 7's reverse-direction proof (get the
**official engine itself** to download from outpace), found and fixed a real bug, then
hit a real, precisely-scoped wall that blocks the full proof this session.

## Real bug found and fixed: B1 broadcasts never announced themselves

`PUT /broadcast/{name}` (B1 origination) minted a transport, signed pieces (note 27), and
registered the piece store in the shared `SeedRegistry` — but **never called
`announce_seeder`/`dht_announce_peer` for the minted infohash**. Only the *leech* path
(`ace_provider.rs`'s `follow_live`, when outpace is consuming someone else's live stream)
self-announced. A broadcast originated via B1 was therefore **completely undiscoverable** —
servable only to a peer that already knew outpace's address out of band. This is exactly
why every discovery-dependent test this session (official-engine support-node mode,
client-console `getstream`) found nothing: there was nothing to find.

Fixed: `ace_provider::announce_seeder_periodically` was split so its core loop
(`announce_infohash_periodically(trackers, infohash, port)`) no longer needs a full
`StreamInfo` — just an infohash + trackers + port. `http.rs`'s `broadcast_ingest` now spawns
it once per **freshly minted** name (via `BroadcastRegistry::start_or_resume` returning
`(Broadcast, bool)`, the bool distinguishing a fresh mint from a resumed `PUT` so repeated
`PUT`s to the same name don't spawn duplicate competing announce loops), gated on
`inbound_peer_port: Option<u16>` in `BroadcastState` (`None` when inbound serving is off —
advertising a port nobody's listening on would misdirect real peers).

**Live-verified**: minting a broadcast now immediately logs a real DHT `announce_peer` to
the public mainline DHT (`[dht] seeded N bootstrap node(s)` /
`[ace] seeder self-announce for <infohash>: ... DHT announce_peer sent to N node(s)`) —
previously nothing happened at all until this fix.

## The wall: official engine computes a different infohash for our transport

Used the engine's own documented `GET /ace/getstream?url=<transport-url>` (client-console
API, `references/ace-network-docs/docs/developers/start-playback.md`) to have the **real
official engine** fetch a outpace-minted transport directly over HTTP (via outpace's
new `GET /broadcast/{name}` endpoint, added this session specifically to make this test
possible). This is a clean, well-documented, non-hacky way to hand the engine an arbitrary
transport — no support-node CLI flakiness involved.

The engine successfully fetches and parses the transport (`is_live: 1` in the response), but
**computes a different infohash than outpace did for the exact same bytes** — reproduced
across 3 independent fresh broadcasts (outpace's infohash vs. the engine's, for identical
transport content, always differ). Confirmed this is a **pure function of file content**, not
of the fetch URL (serving byte-identical transport bytes from a second, unrelated URL yields
the *same* engine-computed infohash) and not session/time-dependent (repeated fetches of the
same URL are stable). This reconfirms and sharpens note 25's flagged, pre-existing gap — this
was not introduced this session, but it directly blocks the reverse-direction proof, since
DHT/tracker discovery is infohash-keyed: outpace now correctly announces under **its own**
computed infohash, but the engine will only ever look for **its own** (different) one.

### What was tried to crack the formula (all ruled out)

- Static: `Transport.so`'s existing Ghidra decompilation (from a prior session) has no
  readable bencode-key string anchors near any hash-related logic — same class of dead end
  as note 25's original attempt, impractical to search blindly across ~3MB of `FUN_xxxxx`
  decompiled C.
- Live, PyCryptodome hooks (the same technique that cracked B0 cleanly): hooked
  `SHA1_update`/`_digest`/`SHA256`/`MD5` on the **client-console** process while triggering
  `getstream?url=`. Only caught unrelated ad-system (VAST XML) hashing — the infohash
  computation does not appear to go through PyCryptodome in this code path (plausible: B0's
  signing lives in `live.so`; this is `Transport.so`'s `TorrentDef.pyx`, a different Cython
  module that may use a different backend).
- Live, `libcrypto`'s `EVP_DigestUpdate`/`EVP_DigestFinal_ex`: fired, but with `Update` calls
  never attributable to the matching `Final` call (buffered input length 0) even after
  narrowing to `(thread_id, ctx_pointer)` pairs — strong evidence the real computation
  doesn't flow through this exact Update+Final pair (likely a one-shot `EVP_Digest()`, a
  legacy non-EVP primitive, or something outside libcrypto entirely, e.g. a bundled hash
  implementation).
- Exhaustive brute force (Python, against **3 independent real ground-truth pairs**
  simultaneously — transport bytes to engine-computed infohash — so a false positive is
  vanishingly unlikely): whole file, whole plaintext, header+plaintext, every individual
  bencode field's value bytes and full key:value encoding (SHA1 and MD5), every contiguous
  field-range concatenation, and re-serialized subset dicts with non-standard keys
  (`outpace_name`, `allow_public_trackers`) dropped. No match found.

### Follow-up same session: exhausted the remaining static+dynamic RE angles

- Fixed the Ghidra export tooling gap itself: the earlier Python `ExportDecomp.py`
  postScript failed (`Ghidra was not started with PyGhidra`). `re/ghidra/ExportDecomp.java`
  (a Java postScript, no PyGhidra needed) already existed in-repo; running
  `analyzeHeadless <proj> acestream -process "live.so" -noanalysis -scriptPath re/ghidra
  -postScript ExportDecomp.java` against the already-imported `live.so` (imported successfully
  in the prior attempt, just never exported) worked and produced a full 34 MB decompile —
  `re/ghidra/ExportDecomp.java` is now the one to use for any future Ghidra export in this
  project, not the `.py` version.
- `live.so`'s `.pyx` string-literal inventory includes `core/src/live/TransportDescriptor.pyx`
  (distinct from `Transport.so`'s `TorrentDef.pyx`) — a plausible separate code path for
  *live* transport descriptors specifically. Followed every reference to it (39 call sites);
  all were the same generic Cython bounds-check/exception-raising boilerplate
  (`FUN_xxxxx(0, line_no, "core/src/live/TransportDescriptor.pyx")`) seen everywhere in this
  codebase, not hash-specific.
- Grepped **both** `Transport.so.c` and `live.so`'s decompile directly for literal calls to
  the PyCryptodome symbol names that cracked B0 (`SHA1_update`, `SHA1_digest`, etc.) — zero
  matches in either. Cross-checked with `nm -D` on every engine `.so` (`Transport.so`,
  `node.so`, `live.so`, `Core.so`, `CoreApp.so`, `streamer.so`, `pysegmenter.so`): **none**
  dynamically import any hash/digest/CRC function. This resolves *why* grepping the
  decompiled C never finds a hash call site: `_SHA1.abi3.so` etc. are separately-`dlopen`ed
  Python extension modules, and Cython code reaches them through the **Python object layer**
  (attribute lookup + method call), not a direct ELF symbol reference — so there is no
  "SHA1(...)" call visible anywhere in the native decompilation to grep for, in principle,
  regardless of how thoroughly it's searched.
- Retried the live Frida hook completely unfiltered (every `update`/`digest` call on
  `_SHA1`/`_SHA256`/`_MD5.abi3.so`, no length threshold, attached *before* triggering
  `getstream?url=`) — still only the same VAST-XML ad-system hashing, nothing else, across
  a fresh broadcast. This is now the **fourth** independent Frida configuration to come back
  negative for this specific call site (PyCryptodome untargeted, PyCryptodome combined with
  EVP, thread/backtrace-filtered EVP+PyCryptodome, and this fully unfiltered pass) — treat
  "does the infohash computation call any of PyCryptodome's exported hash functions" as
  answered: **no**, not as an open question needing more hook attempts.
- Separately: actually pulling playback (`GET /ace/r/<infohash>/<key>`, not just resolving
  via `getstream`) on a outpace-minted, self-announcing broadcast returned `failed to
  start` within seconds, and the playback session was gone (`unknown playback session id`)
  by the time it was checked ~5 minutes later. This might be the *same* infohash-mismatch
  problem surfacing as an immediate client-side give-up rather than a slow discovery
  timeout, or it might be an independent transport-validation rejection — not yet
  distinguished. Either way, no inbound connection ever reached outpace's peer listener
  (confirmed via the source-IP-logging added this session to `PeerListener`'s accept loop),
  so reachability (proven working, note 24/this session) was never actually exercised by
  the engine in this attempt.

This is a precisely-scoped, thoroughly-exhausted-for-this-session open question: *some*
transformation of the transport's decrypted bencode content yields the engine's infohash,
it is none of the ~20 formulas brute-forced, it does not call any native hash primitive this
session could find (PyCryptodome or libcrypto, live or statically), and even discovery
succeeding might not be sufficient by itself (the `failed to start` observation). Next
session's highest-leverage next step is almost certainly **PyGhidra** (Python scripting)
specifically so the actual Python-level call graph (module imports, attribute chains) can be
read directly instead of guessed at via native hooks — or, short of that, hooking at the
CPython bytecode/frame-eval level (`sys.settrace`-style, or Frida's `Interceptor` on
`_PyEval_EvalFrameDefault` filtered by code-object name) to see the actual Python call
sequence around `TorrentDef`/`TransportDescriptor` construction.

## Net effect on the parity goal

- **B0**: cracked, implemented, tested (note 27) — unaffected by this wall.
- **B1**: origination + real signing + **now real tracker/DHT self-announce** — a
  outpace-originated broadcast is, for the first time, genuinely discoverable on the
  public network under its own (correctly-computed-by-us) infohash.
- **Outpace-to-outpace interop** (leech proven repeatedly, B1 loopback proven in note
  26): entirely unaffected, since both ends agree on the infohash (we compute it consistently
  with ourselves).
- **Official-engine-as-consumer-of-our-broadcast** (Task 7 reverse direction): still not
  demonstrated. Not for lack of discoverability anymore (that's fixed) — purely because the
  engine and outpace disagree on what infohash a given transport maps to. This is the one
  remaining, precisely-characterized blocker.
