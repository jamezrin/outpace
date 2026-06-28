# Phase 0 — Acestream Protocol Recovery Spike — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Recover enough of the undocumented Acestream P2P protocol to (a) write a clean protocol spec + binary test vectors and (b) make a documented go/no-go decision on the four viability unknowns, culminating in fetching and verifying one real piece from the live public network with our own code.

**Architecture:** This is a time-boxed reverse-engineering spike, not product code. We run the official closed engine in a controlled Docker sandbox, capture its network traffic, instrument its crypto boundary to obtain plaintext + keys, cross-reference with Ghidra lifts of the Cython `.so` modules and decompiled older pure-`.pyc` releases, and consolidate findings into a versioned spec (`docs/protocol/`) plus committed test vectors (`tests/vectors/`). The spike ends with a throwaway interop binary that proves we can join the swarm.

**Tech Stack:** Docker + docker-compose, tcpdump/tshark, Frida (libsodium hooking), Ghidra (Cython `.so` lift), `decompyle3` (legacy `.pyc`), Python 3 for harness scripts, Rust for the id-math + interop spike (matches the chosen implementation language).

**The four unknowns this spike must answer (the go/no-go):**
1. Does public-swarm entry require an acestream-issued identity/signed key?
2. What are the bootstrap/tracker/supernode endpoints and their protocol?
3. How does a bare `content_id` resolve to a transport file / `infohash`?
4. When is per-stream encryption used, and where does the key come from?

**Working-directory & git policy:**
- All raw/derived RE artifacts live under `re/` and are **git-ignored** (large, derivative, potentially sensitive): `re/captures/`, `re/ghidra/`, `re/decompiled/`, `re/engine/`, `re/harness/`.
- Only **clean artifacts** are committed: investigation notes under `docs/protocol/notes/`, the consolidated spec under `docs/protocol/`, and minimal binary vectors under `tests/vectors/`.

---

## File / Directory Map

| Path | Committed? | Responsibility |
|---|---|---|
| `re/` (+ `.gitignore` entry) | no | All raw RE working artifacts |
| `re/engine/` | no | Extracted official engine tarball |
| `re/sandbox/docker-compose.yml` | no | Official engine + tcpdump sidecar |
| `re/harness/dump.js` | no | Frida libsodium plaintext/key dumper |
| `re/harness/idmath/` | no | Rust id-math validation harness |
| `re/harness/fetch-piece/` | no | Throwaway interop spike (Rust) |
| `docs/protocol/notes/*.md` | yes | Investigation notes per task |
| `docs/protocol/wire-protocol.md` | yes | Consolidated wire-protocol spec |
| `docs/protocol/transport-file.md` | yes | Transport-file + identifier-math spec |
| `docs/protocol/bootstrap.md` | yes | Tracker/supernode bootstrap spec |
| `tests/vectors/` | yes | Binary test vectors + expected values |
| `docs/superpowers/specs/2026-06-28-phase0-findings.md` | yes | Final go/no-go memo |

---

## Task 0: Working directory, tool check, and artifact inventory

**Files:**
- Create: `re/` (git-ignored), `docs/protocol/notes/00-inventory.md`
- Modify: `.gitignore`

- [ ] **Step 1: Add the `re/` working dir to .gitignore**

Append to `.gitignore`:

```gitignore
# Phase-0 reverse-engineering working artifacts (raw/derived, not committed)
/re/
```

- [ ] **Step 2: Create the working tree**

Run:
```bash
mkdir -p re/{engine,sandbox,captures,ghidra,decompiled,harness,vectors} \
         docs/protocol/notes tests/vectors
```

- [ ] **Step 3: Verify required tools are present**

Run:
```bash
for t in docker tshark frida python3 cargo ghidra strings sha256sum jq; do
  printf '%-10s ' "$t"; command -v "$t" || echo "MISSING";
done
```
Expected: a path for each. If `ghidra`/`frida`/`tshark` are MISSING, install them
(`pipx install frida-tools`, distro packages for `tshark`/`wireshark`, Ghidra from
NSA release page). Do not proceed past Task 4 without `frida` and `tshark`.

- [ ] **Step 4: Extract the official engine and inventory the modules**

Run:
```bash
tar xzf "references/acestream_3.2.11_ubuntu_22.04_x86_64_py3.10.tar.gz" -C re/engine
cd re/engine
{
  echo "# Phase-0 Inventory"; echo
  echo "## Core Cython modules"; echo
  echo "| module | size | sha256 | cython |"
  echo "|---|---|---|---|"
  for f in lib/acestreamengine/*.so; do
    cy=$(strings "$f" | grep -oE '_cython_[0-9_]+' | head -1)
    printf "| %s | %s | %s | %s |\n" "$(basename "$f")" \
      "$(stat -c%s "$f")" "$(sha256sum "$f" | cut -c1-16)" "${cy:-n/a}"
  done
} > ../../docs/protocol/notes/00-inventory.md
cd ../..
```

- [ ] **Step 5: Verify the inventory captured every core module**

Run:
```bash
grep -c '| .*\.so |' docs/protocol/notes/00-inventory.md
```
Expected: `7` (Core, CoreApp, Transport, node, live, streamer, pysegmenter).
Exit criterion: every core module is listed with a Cython version of `_cython_0_29_22`.

- [ ] **Step 6: Commit the inventory note**

```bash
git add .gitignore docs/protocol/notes/00-inventory.md
git commit -m "phase0: tool check and engine module inventory"
```

---

## Task 1: Run the official engine in a capture-ready sandbox

**Files:**
- Create: `re/sandbox/docker-compose.yml`, `re/sandbox/Dockerfile`

- [ ] **Step 1: Write a Dockerfile that runs our local engine tarball**

Create `re/sandbox/Dockerfile`:
```dockerfile
FROM python:3.10-slim-bookworm
ENV PIP_BREAK_SYSTEM_PACKAGES=1 PYTHONUNBUFFERED=1
RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates tcpdump procps && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY engine/ /app/
RUN pip install --no-cache-dir -r /app/requirements.txt frida-tools
EXPOSE 6878 8621
# The tarball ships a `start-engine` launcher at its root (confirmed: this is how
# acestream-http-proxy invokes it). --bind-all exposes the HTTP API; verbose logs.
CMD ["/app/start-engine", "--client-console", "--bind-all", \
     "--log-stderr", "--log-stderr-level", "debug", "--log-modules", "root:D"]
```
Note: flags come from `engine-command-line-options.md`. `start-engine` is a shell
launcher that sets `LD_LIBRARY_PATH` to the bundled `lib/` and execs the engine;
ensure it is executable after `COPY` (`RUN chmod +x /app/start-engine`).

- [ ] **Step 2: Write the compose file with a tcpdump sidecar sharing the netns**

Create `re/sandbox/docker-compose.yml`:
```yaml
services:
  acestream:
    build: .
    cap_add: ["NET_RAW", "NET_ADMIN", "SYS_PTRACE"]  # SYS_PTRACE for frida
    ports: ["6878:6878"]
    security_opt: ["seccomp:unconfined"]
  capture:
    image: nicolaka/netshoot
    network_mode: "service:acestream"   # same network namespace
    cap_add: ["NET_RAW", "NET_ADMIN"]
    volumes: ["./../captures:/caps"]
    command: ["tcpdump", "-i", "any", "-w", "/caps/session.pcap", "-U"]
    depends_on: ["acestream"]
```

- [ ] **Step 3: Confirm the real engine entrypoint, then build**

Run:
```bash
ls -R re/engine | grep -iE 'start|engine|acestreamengine' | head
cd re/sandbox && docker compose build acestream
```
Expected: image builds. If `pip install -r requirements.txt` fails on `apsw`/`pynacl`,
add `build-essential libffi-dev` to the Dockerfile `apt-get` line and rebuild.

- [ ] **Step 4: Start the engine and verify it answers the documented API**

Run:
```bash
docker compose up -d acestream
sleep 8
curl -s "http://127.0.0.1:6878/webui/api/service?method=get_version" | jq .
```
Expected: JSON with `result.version` ~ `3.2.11` and `platform: linux`.
Exit criterion: the official engine is running and reachable. **This is our ground-truth oracle for every later task.**

- [ ] **Step 5: Commit nothing (re/ is git-ignored); record the working invocation**

Append the confirmed `docker compose up` invocation and `get_version` output to
`docs/protocol/notes/01-sandbox.md`, then:
```bash
git add docs/protocol/notes/01-sandbox.md
git commit -m "phase0: document official-engine sandbox setup"
```

---

## Task 2: Establish known-good public test streams (live + VOD)

**Files:**
- Create: `docs/protocol/notes/02-test-streams.md`

- [ ] **Step 1: Confirm the documented synthetic demo channel resolves**

Run (using the infohash from `start-playback.md`):
```bash
IH=685edf209ccfdf88977c0d317e1407baca486067
curl -s "http://127.0.0.1:6878/server/api?api_version=3&method=get_media_files&infohash=$IH" | jq .
```
Expected: `result.type` is `live` or `vod`, with a `name` and `files[]`. Record `transport_type`.

- [ ] **Step 2: Start a playback session and confirm peers connect**

Run:
```bash
SID=$(curl -s "http://127.0.0.1:6878/ace/manifest.m3u8?format=json&infohash=$IH" | jq -r '.response.stat_url')
for i in 1 2 3 4 5; do curl -s "$SID" | jq '{status,peers,speed_down,downloaded}'; sleep 3; done
```
Expected: within ~15s, `peers > 0` and `downloaded` increasing. If `peers` stays 0,
the infohash is dead — find a live one from a public Acestream playlist and repeat.
Exit criterion: at least **one live** and **one VOD** infohash that reach `peers>0` and download.

- [ ] **Step 3: Record the working streams**

Write `docs/protocol/notes/02-test-streams.md` with, for each stream: infohash,
`transport_type`, `is_live`, observed peer count, and the `get_content_id` result:
```bash
curl -s "http://127.0.0.1:6878/server/api?api_version=3&method=get_content_id&infohash=$IH" | jq -r .result.content_id
```
(This `content_id`/`infohash` pair becomes a test vector in Task 8.)

- [ ] **Step 4: Commit**

```bash
git add docs/protocol/notes/02-test-streams.md
git commit -m "phase0: record known-good public test streams"
```

---

## Task 3: Full packet capture of a playback session

**Files:**
- Create: `re/captures/session.pcap` (git-ignored), `docs/protocol/notes/03-capture.md`

- [ ] **Step 1: Start the capture sidecar, then play a stream end-to-end**

Run:
```bash
cd re/sandbox && docker compose up -d capture
IH=<live-infohash-from-task-2>
curl -s "http://127.0.0.1:6878/ace/getstream?infohash=$IH" -o /dev/null --max-time 45 &
sleep 50 && docker compose stop capture
```
Expected: `re/captures/session.pcap` grows to at least a few MB.

- [ ] **Step 2: Summarize endpoints by role (DNS, tracker, peer, API)**

Run:
```bash
cd ../..
echo "== DNS lookups =="; tshark -r re/captures/session.pcap -Y dns.flags.response==1 \
  -T fields -e dns.qry.name -e dns.a 2>/dev/null | sort -u | head -40
echo "== Talkers by bytes =="; tshark -r re/captures/session.pcap -q -z conv,ip 2>/dev/null | head -40
echo "== UDP vs TCP dst ports =="; tshark -r re/captures/session.pcap \
  -T fields -e ip.proto -e udp.dstport -e tcp.dstport 2>/dev/null | sort | uniq -c | sort -rn | head
```
Expected: you can see DNS names (candidate bootstrap hosts), a set of peer IPs on
high/UDP ports (the swarm), and possibly tracker hosts on HTTP/UDP.

- [ ] **Step 3: Classify and record**

Write `docs/protocol/notes/03-capture.md` separating: (a) Acestream-owned infra
hosts (DNS names ending in acestream domains), (b) tracker endpoints + transport
(HTTP/UDP), (c) peer connections (IP\:port, transport, byte volume). Note the local
P2P port actually used (expected `8621`).
Exit criterion: the note clearly distinguishes **infrastructure** traffic from
**peer** traffic — the input to Tasks 4 and 9.

- [ ] **Step 4: Commit the note**

```bash
git add docs/protocol/notes/03-capture.md
git commit -m "phase0: classify playback-session traffic"
```

---

## Task 4: Enumerate bootstrap / tracker / supernode endpoints

**Files:**
- Create: `docs/protocol/bootstrap.md`

- [ ] **Step 1: Cross-reference captured hosts against strings in the binaries**

Run:
```bash
for h in $(grep -oE '[a-z0-9.-]+\.[a-z]{2,}' docs/protocol/notes/03-capture.md | sort -u); do
  if strings re/engine/lib/acestreamengine/*.so | grep -qF "$h"; then
    echo "HARDCODED  $h"; else echo "runtime    $h"; fi
done
```
Expected: a set of `HARDCODED` bootstrap hostnames (these are the network entry points).

- [ ] **Step 2: Capture a raw tracker announce/response**

If tracker traffic is HTTP, extract it:
```bash
tshark -r re/captures/session.pcap -Y 'http.request || http.response' \
  -T fields -e http.host -e http.request.uri -e http.response.code 2>/dev/null | head -40
```
If UDP, isolate the tracker peer-list packets:
```bash
tshark -r re/captures/session.pcap -Y "udp && ip.addr==<tracker-ip>" \
  -T fields -e frame.number -e data.data 2>/dev/null | head
```
Expected: an observable announce request and a response carrying a peer list.

- [ ] **Step 3: Document the bootstrap spec**

Write `docs/protocol/bootstrap.md`: the hardcoded host(s), transport (HTTP/UDP),
the announce request format (params/fields observed), and the response peer-list
encoding. Mark anything still unknown explicitly as `OPEN:`.
Exit criterion (answers Unknown #2): we can describe how to obtain a peer list for
a given infohash from the live network.

- [ ] **Step 4: Commit**

```bash
git add docs/protocol/bootstrap.md
git commit -m "phase0: document tracker/supernode bootstrap"
```

---

## Task 5: Instrument the crypto boundary (plaintext + keys) — the crux

**Files:**
- Create: `re/harness/dump.js` (git-ignored), `docs/protocol/notes/05-crypto.md`

- [ ] **Step 1: Write a Frida script hooking libsodium's box/secretbox/sign**

Create `re/harness/dump.js`:
```javascript
// Dump plaintext, nonce, and keys at the NaCl boundary used by PyNaCl.
// PyNaCl calls into a bundled libsodium (_sodium*.so) via these exports.
function hexdump(ptr, len) {
  if (ptr.isNull() || len <= 0 || len > 1<<20) return null;
  return Memory.readByteArray(ptr, len);
}
function log(rec) { send(rec); }   // delivered to the host CLI as JSON

const targets = [
  // name, argIndex map {ct, ctLenPtr, msg, msgLen, nonce, pk_or_k, sk}
  { sym: "crypto_box_easy",            dir: "encrypt",
    msg: 1, msgLen: 2, nonce: 3, pk: 4, sk: 5 },
  { sym: "crypto_box_open_easy",       dir: "decrypt",
    ct: 1, ctLen: 2, nonce: 3, pk: 4, sk: 5 },
  { sym: "crypto_secretbox_easy",      dir: "encrypt",
    msg: 1, msgLen: 2, nonce: 3, key: 4 },
  { sym: "crypto_secretbox_open_easy", dir: "decrypt",
    ct: 1, ctLen: 2, nonce: 3, key: 4 },
];

targets.forEach(t => {
  const a = Module.findExportByName(null, t.sym);
  if (!a) { console.log("no export " + t.sym); return; }
  Interceptor.attach(a, {
    onEnter(args) {
      const rec = { sym: t.sym, dir: t.dir };
      if (t.msg !== undefined) {
        const len = args[t.msgLen].toInt32();
        rec.msg = hexdump(args[t.msg], len); rec.msgLen = len;
      }
      if (t.ct !== undefined) {
        const len = args[t.ctLen].toInt32();
        rec.ct = hexdump(args[t.ct], len); rec.ctLen = len;
      }
      if (t.nonce !== undefined) rec.nonce = hexdump(args[t.nonce], 24);
      if (t.pk    !== undefined) rec.pk    = hexdump(args[t.pk], 32);
      if (t.sk    !== undefined) rec.sk    = hexdump(args[t.sk], 32);
      if (t.key   !== undefined) rec.key   = hexdump(args[t.key], 32);
      log(rec);
    }
  });
});
```

- [ ] **Step 2: Attach Frida to the running engine inside the container**

Run:
```bash
PID=$(docker compose -f re/sandbox/docker-compose.yml exec acestream pgrep -f acestreamengine | head -1)
docker compose -f re/sandbox/docker-compose.yml exec acestream \
  frida -p "$PID" -l /app/../harness/dump.js -o /caps/plaintext.jsonl &
```
If the harness path isn't mounted in the container, copy it in first:
`docker compose -f re/sandbox/docker-compose.yml cp re/harness/dump.js acestream:/tmp/dump.js`
and reference `/tmp/dump.js`. Adjust the PID if multiple processes match.
Expected: Frida attaches without `Failed to attach` (needs `SYS_PTRACE`, already in compose).

- [ ] **Step 3: Replay a playback to capture handshake plaintext**

Run a fresh playback (`curl .../ace/getstream?infohash=$IH`) for ~30s, then inspect:
```bash
wc -l re/captures/plaintext.jsonl
head -3 re/captures/plaintext.jsonl | jq '{sym,dir,msgLen,ctLen}'
```
Expected: non-empty; you see `crypto_box_easy`/`open_easy` calls during connection
setup (the handshake) and `secretbox` calls during steady-state piece transfer
(or confirm streams are unencrypted if these are absent for data).

- [ ] **Step 4: Determine the identity-key question (Unknown #1)**

Run:
```bash
jq -r 'select(.sk!=null) | .sk' re/captures/plaintext.jsonl | sort -u | head
```
Inspect whether the local secret/public keys are **stable across restarts**
(persisted identity in `~/.ACEStream`) or **ephemeral per session**, and whether
the handshake embeds a server-signed token. Restart the container and re-capture to
compare. Record the conclusion.
Exit criterion (answers Unknown #1 & #4): we know whether swarm entry needs a
server-issued identity, and whether/where stream encryption keys originate.

- [ ] **Step 5: Document and commit the crypto findings**

Write `docs/protocol/notes/05-crypto.md` (handshake message sequence, key roles,
encrypted-vs-plain data path, identity persistence). Do **not** commit raw key
material. Then:
```bash
git add docs/protocol/notes/05-crypto.md
git commit -m "phase0: document NaCl handshake and stream-encryption findings"
```

---

## Task 6: Decompile a legacy pure-`.pyc` release for the algorithmic skeleton

**Files:**
- Create: `re/decompiled/` (git-ignored), `docs/protocol/notes/06-algorithms.md`

- [ ] **Step 1: Obtain an older pyc-based engine of the same lineage**

Acquire an early ACE 3.0/3.1 or TorrentStream build whose `acestreamengine` ships as
`.pyc` (pre-Cython). Place its `.pyc` tree under `re/decompiled/pyc/`.
Note: if none is available, skip to Ghidra (Task 7); this task is an accelerator,
not a hard dependency.

- [ ] **Step 2: Decompile the transport/hashing/protocol modules**

Run:
```bash
pip install decompyle3 || pipx install decompyle3
find re/decompiled/pyc -name '*.pyc' \
  \( -iname '*transport*' -o -iname '*hash*' -o -iname '*sha*' \
     -o -iname '*node*' -o -iname '*tracker*' -o -iname '*proto*' \) \
  -exec sh -c 'decompyle3 "$1" > "re/decompiled/$(basename "$1" .pyc).py" 2>/dev/null' _ {} \;
ls re/decompiled/*.py
```
Expected: readable `.py` sources for the targeted modules.

- [ ] **Step 3: Extract the stable algorithms**

From the decompiled sources, document: transport-file structure, how `infohash`
and `content_id` are computed (which fields are hashed, which digest), bencode usage,
and message-ID constants. Write these into `docs/protocol/notes/06-algorithms.md`.
Exit criterion: a candidate algorithm for `content_id`/`infohash` to validate in Task 8.

- [ ] **Step 4: Commit**

```bash
git add docs/protocol/notes/06-algorithms.md
git commit -m "phase0: extract algorithm skeleton from legacy pyc"
```

---

## Task 7: Ghidra lift of `node.so` and `Transport.so`

**Files:**
- Create: `re/ghidra/` (git-ignored), `docs/protocol/notes/07-node.md`, `docs/protocol/notes/07-transport.md`

- [ ] **Step 1: Headless-import and auto-analyze the two priority modules**

Run:
```bash
ghidra_headless=$(dirname "$(command -v ghidra)")/support/analyzeHeadless
"$ghidra_headless" re/ghidra acestream -import \
  re/engine/lib/acestreamengine/node.so re/engine/lib/acestreamengine/Transport.so \
  -analysisTimeoutPerFile 1800
```
Expected: analysis completes; project `re/ghidra/acestream.gpr` created.
(If headless is unavailable, open the GUI and import the two `.so` manually.)

- [ ] **Step 2: Map the handshake + message dispatch in `node.so`**

Using the preserved Cython symbol names as anchors (`get_trackers`,
`get_meta_trackers`, `add_trackers`, `get_peers`, `set_infohash`,
`get_piece_length`), trace the functions that build the handshake and dispatch
peer messages. Record each message ID and its field layout.
Write `docs/protocol/notes/07-node.md`.

- [ ] **Step 3: Map the transport-file format in `Transport.so`**

Anchor on `TransportDescriptor`, `MultiTransportDescriptor`,
`__pyx_unpickle_TransportDescriptor`. Document the on-disk/on-wire field order,
types, and how multi-stream descriptors nest. Write `docs/protocol/notes/07-transport.md`.
Exit criterion: enough structure to parse a real transport file byte-for-byte in Task 8.

- [ ] **Step 4: Commit both notes**

```bash
git add docs/protocol/notes/07-node.md docs/protocol/notes/07-transport.md
git commit -m "phase0: ghidra lift of node and transport modules"
```

---

## Task 8: Reconstruct & validate transport-file + identifier math (Rust harness)

**Files:**
- Create: `re/harness/idmath/` (git-ignored Rust crate), `tests/vectors/transport-01.bin`, `docs/protocol/transport-file.md`

- [ ] **Step 1: Capture a real transport file as a vector**

Get the raw transport file the engine uses for a known infohash:
```bash
IH=<vod-infohash-from-task-2>
curl -s "http://127.0.0.1:6878/server/api?api_version=3&method=get_media_files&infohash=$IH&dump_transport_file=1" \
  -o tests/vectors/transport-01.json
# If the API returns a URL/path, fetch the raw bytes into tests/vectors/transport-01.bin
```
Also record the **ground-truth** mapping from Task 2: `infohash` ⇄ `content_id`.

- [ ] **Step 2: Scaffold the Rust id-math harness**

Run:
```bash
cargo new --lib re/harness/idmath
```
Add to `re/harness/idmath/Cargo.toml`:
```toml
[dependencies]
sha1 = "0.10"
sha2 = "0.10"
hex = "0.4"
```

- [ ] **Step 3: Write a failing test asserting our math matches ground truth**

Create `re/harness/idmath/tests/vectors.rs` (fill the two constants from Step 1):
```rust
use idmath::{parse_transport, content_id_of, infohash_of};

const TRANSPORT: &[u8] = include_bytes!("../../../../tests/vectors/transport-01.bin");
const EXPECTED_INFOHASH: &str   = "PUT_REAL_INFOHASH_FROM_TASK2";
const EXPECTED_CONTENT_ID: &str = "PUT_REAL_CONTENT_ID_FROM_TASK2";

#[test]
fn infohash_matches_engine() {
    let t = parse_transport(TRANSPORT).expect("parse");
    assert_eq!(hex::encode(infohash_of(&t)), EXPECTED_INFOHASH);
}

#[test]
fn content_id_matches_engine() {
    let t = parse_transport(TRANSPORT).expect("parse");
    assert_eq!(hex::encode(content_id_of(&t)), EXPECTED_CONTENT_ID);
}
```

- [ ] **Step 4: Run the test to confirm it fails (no implementation yet)**

Run: `cd re/harness/idmath && cargo test`
Expected: FAIL — `parse_transport`/`infohash_of`/`content_id_of` not found.

- [ ] **Step 5: Implement parsing + hashing from the Task 6/7 findings**

Create `re/harness/idmath/src/lib.rs` implementing `parse_transport`, `infohash_of`
(SHA-1 over the descriptor per `Transport.so`), and `content_id_of` (the digest
identified in Task 6). Implement exactly the field layout documented in
`docs/protocol/transport-file.md`.

- [ ] **Step 6: Run until both vectors pass**

Run: `cd re/harness/idmath && cargo test`
Expected: PASS on both tests. If they fail, the layout/digest in the notes is wrong —
return to Task 7 Step 3 and correct the spec, then re-run. **Iterate here until the
engine's own identifiers are reproduced byte-for-byte.**
Exit criterion (answers Unknown #3 partially): we can derive `infohash`/`content_id`
from a transport file, matching the official engine.

- [ ] **Step 7: Promote the validated layout to the committed spec + commit**

Finalize `docs/protocol/transport-file.md` (field table + the two hash algorithms,
with the passing vector referenced). Commit the spec and the vector:
```bash
git add docs/protocol/transport-file.md tests/vectors/transport-01.*
git commit -m "phase0: validated transport-file format and identifier math"
```

---

## Task 9: Reconstruct the wire protocol + capture message vectors

**Files:**
- Create: `tests/vectors/messages/*.bin`, `docs/protocol/wire-protocol.md`

- [ ] **Step 1: Correlate plaintext (Task 5) with binary structure (Task 7)**

For each distinct message observed in `re/captures/plaintext.jsonl`, line it up with
the dispatch layout from `docs/protocol/notes/07-node.md`. Identify: handshake
request/response, tracker announce, `have`/`bitfield`, piece `request`, piece `data`,
and any keep-alive.

- [ ] **Step 2: Save one annotated vector per message type**

For each message type, write its raw plaintext bytes to
`tests/vectors/messages/<name>.bin` and an adjacent `<name>.md` describing every
field (offset, length, type, meaning).
```bash
mkdir -p tests/vectors/messages
# e.g. extract a handshake plaintext from the jsonl into a .bin:
jq -r 'select(.sym=="crypto_box_easy" and .dir=="encrypt") | .msg' \
  re/captures/plaintext.jsonl | head -1   # decode hex/bytearray to bytes -> handshake.bin
```

- [ ] **Step 3: Write the consolidated wire-protocol spec**

Write `docs/protocol/wire-protocol.md`: connection lifecycle (TCP/UDP per Task 3),
the NaCl handshake sequence (per Task 5), framing (length prefix + message ID),
the full message table with field layouts, and the live piece/`livepos` semantics.
Mark residual gaps as `OPEN:`.
Exit criterion: a spec complete enough to implement `ace-wire` + `ace-peer` against.

- [ ] **Step 4: Commit**

```bash
git add docs/protocol/wire-protocol.md tests/vectors/messages/
git commit -m "phase0: wire-protocol spec and message vectors"
```

---

## Task 10: Interop spike — fetch and verify one real piece with our own code

**Files:**
- Create: `re/harness/fetch-piece/` (git-ignored Rust binary)

- [ ] **Step 1: Scaffold the throwaway interop binary**

Run:
```bash
cargo new re/harness/fetch-piece
```
Add deps to `re/harness/fetch-piece/Cargo.toml`:
```toml
[dependencies]
tokio = { version = "1", features = ["full"] }
crypto_box = "0.9"
sha1 = "0.10"
hex = "0.4"
```

- [ ] **Step 2: Implement the minimal path from the specs**

In `re/harness/fetch-piece/src/main.rs`, implement exactly the validated specs:
(1) announce to the bootstrap/tracker (`docs/protocol/bootstrap.md`) for a known
infohash → peer list; (2) NaCl handshake with one peer (`docs/protocol/wire-protocol.md`);
(3) send a piece `request` for piece 0; (4) receive the piece and verify its SHA-1
against the transport file's piece hash (`docs/protocol/transport-file.md`).
Print `OK <piece_index> <sha1>` on success.

- [ ] **Step 3: Run it against the live network**

Run:
```bash
cd re/harness/fetch-piece && cargo run -- --infohash <vod-infohash-from-task-2>
```
Expected: `OK 0 <sha1>` with a hash matching the transport file. If the handshake is
rejected, capture our outgoing bytes with tshark and diff against the official
handshake vector (`tests/vectors/messages/handshake.bin`) — the mismatch localizes
the spec error. **Iterate Tasks 5/9 ↔ 10 until a real peer serves us a valid piece.**
Exit criterion (the viability proof): our own code obtains and verifies a real piece
from the public swarm.

- [ ] **Step 4: Record the result**

Append the successful run output and any required handshake quirks to
`docs/protocol/notes/10-interop.md`, then:
```bash
git add docs/protocol/notes/10-interop.md
git commit -m "phase0: interop spike fetched a verified piece from the live swarm"
```

---

## Task 11: Go/No-Go findings memo

**Files:**
- Create: `docs/superpowers/specs/2026-06-28-phase0-findings.md`

- [ ] **Step 1: Answer the four unknowns explicitly**

Write `docs/superpowers/specs/2026-06-28-phase0-findings.md` with a section per
unknown, each marked RESOLVED / PARTIAL / BLOCKED, citing the note/vector that
supports it:
1. Identity/signed-key requirement for swarm entry (Task 5).
2. Bootstrap/tracker endpoints & protocol (Task 4).
3. `content_id`→transport-file resolution (Tasks 6/8; note the `content_id`-only
   lookup path if it requires a hosted service).
4. Stream-encryption usage & key origin (Task 5).

- [ ] **Step 2: State the go/no-go decision**

Add a decision section: **GO** if Task 10 fetched a verified piece and no unknown is
BLOCKED; otherwise **NO-GO / CONDITIONAL** with the specific blocker and what would
unblock it. Include a rough effort estimate for Phases 1–6 based on what the spike revealed.

- [ ] **Step 3: Confirm spec completeness for downstream phases**

Verify these committed artifacts exist and are non-empty:
```bash
ls -l docs/protocol/wire-protocol.md docs/protocol/transport-file.md \
      docs/protocol/bootstrap.md tests/vectors/transport-01.bin \
      tests/vectors/messages/
```
Expected: all present. These are the inputs Phase 1 (`ace-wire`) builds against.

- [ ] **Step 4: Commit the memo**

```bash
git add docs/superpowers/specs/2026-06-28-phase0-findings.md
git commit -m "phase0: go/no-go findings memo and consolidated protocol spec"
```

---

## Self-Review Notes (coverage against the spec)

- Unknown #1 (identity/keys) → Task 5; Unknown #2 (bootstrap) → Tasks 3–4;
  Unknown #3 (`content_id`→transport) → Tasks 6, 8 (+11 for the hosted-lookup path);
  Unknown #4 (encryption) → Task 5. All four are also re-stated in the Task 11 memo.
- RE methodology from the design doc (Ghidra lift, legacy `.pyc` decompile, dynamic
  capture + instrumentation, clean spec + vectors) → Tasks 7, 6, 3+5, 8+9 respectively.
- Deliverables from design §6/§7 Phase 0 (written spec, test vectors, go/no-go memo)
  → `docs/protocol/*`, `tests/vectors/*`, Task 11 memo.
- The interop spike (Task 10) is the concrete viability proof the design calls the
  "gate."
- Adaptation note: TDD's failing-test/passing-test rhythm is preserved literally in
  Tasks 8 and 10 (real `cargo test`/run with expected FAIL→PASS); investigation tasks
  use an explicit "Exit criterion" as the verification analogue.
```
