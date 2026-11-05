# Offline autonomous support topology

Issue #270's Linux-only opt-in tests are in
`crates/ace-engine/src/runtime/support_topology_tests.rs`. Normal workspace runs compile
and ignore them. They must run through a bounded launcher in a fresh user/network
namespace containing only loopback. Public-mode discovery may still attempt tracker/DHT
work even with explicit upstream peers; loopback bootstrap alone is not isolation.

Build the engine library test executable with the pinned toolchain:

```sh
cargo test -p ace-engine --lib --offline --locked --no-run
```

Use the exact executable path reported by this build. Do not select an old binary by a
wildcard. Choose one full case name from the list below. The following launches one case:

```sh
support_binary=/absolute/path/to/the/reported/ace_engine-test-executable
support_case=runtime::support_topology_tests::signed_support_restart_replaces_disk_generation
support_dir=$(mktemp -d)
chmod 700 "$support_dir"
OUTPACE_TEST_ISOLATED_SUPPORT=1 OUTPACE_TEST_SUPPORT_DIR="$support_dir" \
  unshare --user --map-root-user --net sh -c \
  'ip link set lo up && exec timeout --kill-after=5s 35s "$1" "$2" --exact --ignored --nocapture' \
  sh "$support_binary" "$support_case"
```

Require exit zero and exactly **one passed test**, with zero failed and zero ignored.
Zero selected tests are a failed observation, even if the executable exits zero.
If user/network namespaces are unavailable, report the observation as unavailable;
never rerun the public-mode case on host networking. The test checks loopback-only
interfaces and absence of IPv4 routes, but these checks alone do not prove a fresh namespace.

Keep the temporary directory until the entire launched process has exited, including cache
writers. Then remove that exact directory. Source shutdown alone does not establish completion
of provider-owned warm-cache writers or unrelated runtime listeners. The launcher deadline
bounds a cleanup regression; forced termination is a failed cleanup observation, not a pass.

## Cases

All names below have prefix `runtime::support_topology_tests::`:

- `public_daemon_reseeds_signed_pieces_without_a_viewer`: actual daemon supervision,
  signed upstream, verified piece store and independent inbound peer; no playback clients.
- `explicit_source_reseeds_signed_pieces_without_supervision`: same finite source workload
  started explicitly, for comparison with the supervisor path.
- `signed_reseeding_does_not_require_decoder_startup`: signed wire delivery while the native
  playback gate cannot open on that exact payload; a ready-media control must open. This
  does not run VLC/FFmpeg or claim decoder quality.
- `signed_support_restart_replaces_memory_generation`: native stop/start with confirmed
  old source cleanup and released capacity, then distinct signed replacement media/header.
- `signed_support_restart_replaces_disk_generation`: the same lifecycle with actual disk
  files, bounded retained piece bytes and old-generation file cleanup before restart.

The fixtures use existing SDK metadata injection for synthetic descriptors. Public daemon
configuration forbids private local-descriptor maps, and public transport fetches reject
loopback destinations. These policies are preserved; the topology is not proof of public
catalog/transport descriptor acquisition or official-engine interoperability.

The actual daemon inbound cap is one. After the data client closes, another admitted
handshake proves the previous handler released its global permit and retained reader.
This is not a join of the whole listener or all independent consumers. Existing corrupt-signature
coverage in `live_session::tests::authentication::rejected_signed_piece_retries_through_full_peer_pool`
checks that rejected bytes produce neither availability nor inbound/reciprocal replies for
both memory and disk stores before the valid replacement arrives.

## Resource interpretation

Each case emits a private `[support-fixture]` JSON observation with process RSS, file descriptors,
OS threads, Tokio task count and signal-to-runtime-return time. These are finite snapshots,
not peaks or worst-case latency. The latter interval excludes the subsequent upstream-handler
join. Process-wide CPU/peak RSS can be captured by a parent using per-child `wait4` accounting;
include launcher/key-generation overhead in the stated scope.

A historical comparison must pin the old revision and test-only adapter, use separate build
outputs, identical geometry and finite observation settings, and require one real passing case
on each binary. The older explicit-source path is a comparison workload, not an older autonomous
service. One pair with fresh keys/ports cannot establish throughput, statistical regression
thresholds, parser-specific CPU, sustained stability or readiness. No service change to parser
or payload queues is inferred from these observations.

Store-piece counts/file sizes are checked separately from process RSS. Residual threads or
file descriptors after source cleanup are reported; they must not be relabeled full restoration.
The existing inbound and cache limits, session admission, signature verification and cancellation
regressions remain separate evidence, not claims derived from a short process snapshot.
