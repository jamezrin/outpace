# Android companion app: design

Status: proposed. Issue: #453.

## Intent

Today, running outpace on Android means Termux, a manual download, `termux-wake-lock` and a
terminal. That works for people who already live in Termux and is a wall for everyone else,
especially on Android TV, where there is no comfortable terminal at all.

The app is a thin, installable wrapper that makes outpace a normal Android citizen: install an
APK, press start, and the engine runs in the background on `127.0.0.1:6878`. Its second job is
to turn a content id or `acestream://` link into playback in whatever player the user already
has (VLC, MX Player, Just Player, Kodi), without the user ever seeing or copying a URL.

It must work equally well on a phone and on an Android TV box.

## Goals

1. Start, stop and supervise the outpace engine on the device, without root and without Termux.
2. Keep the engine alive in the background reliably (foreground service).
3. Show whether the engine is healthy and which streams are active.
4. Accept a content id or `acestream://` link from link taps, the system share sheet, a paste
   box, or a recent-streams list, and hand the stream to an external player.
5. Expose a small set of engine settings through a UI, mapped to existing `OUTPACE_*`
   environment variables.
6. Let the user pick which engine release to run, downloaded from GitHub releases, and always
   see which version is running and which is the latest.
7. One APK for phones and Android TV, covering 64-bit and 32-bit ARM devices.

## Non-goals

- No built-in player, ever. Playback is always delegated to another app.
- No stream search or channel lists.
- No broadcasting UI (RTMP, SRT, `outpace broadcast`).
- No self-update of the app itself in v1 (a newer APK is installed by hand). Engine versions are
  managed in-app, see feature 7.
- No automatic engine upgrades: the app tells the user a newer release exists; the user decides.
- No engine bundled in the APK.
- No root features.
- No Play Store listing in v1 (sideloaded from GitHub releases). Do not design anything that
  rules a store listing out later, but do not do the store-specific work now.
- No phone-to-TV "send this link to my TV" feature in v1.
- No changes to the engine's behavior to suit the app. If the app needs something the engine
  lacks, file it as a separate engine issue.

## Anti-goals (would make this a failure even if it works)

- The app drains a phone's battery: background polling, wake locks held with no streams, or the
  engine left running for hours unused on a phone.
- The app adds measurable cost to the engine (CPU, memory, startup). The engine binary is the
  same one shipped in releases; the app adds nothing to its hot paths.
- A TV user cannot do everything with a D-pad remote.
- The engine's HTTP API is exposed to the LAN without the user explicitly turning that on. (The
  P2P peer listener, `OUTPACE_PEER_LISTEN`, keeps its engine default of `0.0.0.0:8621` like every
  other platform; that is the peer-to-peer port, not the API.)
- The app or its docs contain a real content id, infohash or stream name (see the repository's
  identifier hygiene rules). Examples use `0123456789abcdef0123456789abcdef01234567`.

## Constraints

- **Engine as a child process, not a library.** The app downloads the unmodified `outpace`
  release binary for the device from GitHub releases and runs `outpace serve` with
  `ProcessBuilder` from a foreground service. No JNI, no Rust changes, no engine in the APK. A
  crash in the engine kills only the engine process; the app reports it.
- **Where the binary lives and how it runs (decided, verified).** Downloaded engines live in
  `filesDir/engines/<version>/outpace`. Since Android 10, apps targeting API 29+ cannot `exec()`
  files in their own storage. The app therefore runs the engine through the system loader:
  `/system/bin/linker64 <path> serve` for a 64-bit engine, `/system/bin/linker <path> serve` for a
  32-bit one (picked from the engine's ELF class, not the app's bitness). The loader only accepts
  a program argument from Android 10, the same release that introduced the block, so on Android 5
  to 9 the app executes the file directly. The launch command depends only on the device's API
  level, never on where the engine came from. Verified with targetSdk 36 and locally built
  engines on emulators:

  | Image | Direct exec | Through the loader | `/healthz` | `destroy()` |
  | --- | --- | --- | --- | --- |
  | API 35, x86_64 (64-bit only) | `error=13, Permission denied` | `linker64` runs the engine | `ok` in 500 ms | exit 0 (SIGTERM) |
  | API 30, x86 (32-bit) | `error=13, Permission denied` | `linker` runs the engine | `ok` in 500 ms | exit 0 (SIGTERM) |
  | API 28, x86_64 and x86 | runs (64 and 32-bit) | prints its help text and exits **0** | `ok` in 500 ms | exit 0 (SIGTERM) |

  So the cutoff is exactly API 29, and it must be chosen by API level: on API 28 the loader
  "succeeds" without running anything, so a failed launch cannot be detected from its exit code.
  A 64-bit-only device has no `/system/bin/linker`, so the engine choice must follow
  `SUPPORTED_ABIS` (never offer a 32-bit engine there). API 21 to 27 were not tested; older
  libcore may implement `destroy()` as SIGKILL, so below API 24 the app sends SIGTERM itself via
  `android.os.Process.sendSignal` and the child's pid. The thread reading the engine's output gets
  an `IOException` when the engine exits and must treat that as end of stream. This relies on release binaries being
  dynamically linked PIEs with the bionic interpreter, which they are today (NDK builds); the
  Android CI jobs must keep it that way.
- **ABIs.** `arm64-v8a` (existing `aarch64-linux-android` release asset) and `armeabi-v7a` (new
  `armv7-linux-androideabi` asset). Many TV devices run a 32-bit userland on 64-bit chips, so
  32-bit is required, not optional. The app maps `Build.SUPPORTED_ABIS[0]` to a release target:
  `arm64-v8a` to `aarch64-linux-android`, `armeabi-v7a` to `armv7-linux-androideabi`, `x86_64` to
  `x86_64-linux-android`. `x86_64` releases are optional (emulator use); the mapping exists so a
  release that adds them just works. The 32-bit engine build must be added to CI; the engine
  already keeps the system allocator on Android and on 32-bit targets, so no allocator work is
  expected, but this is unverified until CI builds and smoke-tests it.
- **Play Store.** Downloading executable code is against Play policy, so a store build would need
  a different engine source. Out of scope for v1 (sideload only); nothing here prevents adding one.
- **Minimum Android version.** Match the engine: API 21 (Android 5). Target the current SDK
  required for sideloaded installs to not warn.
- **Stopping.** The engine handles SIGTERM with a graceful shutdown. The app sends SIGTERM, waits
  a few seconds, then force-kills. Caveat: on API 21 to 25, `Process.destroy()` sends SIGKILL and
  `destroyForcibly()`/timed `waitFor` do not exist, so the builder sends SIGTERM explicitly
  (`android.os.Process.sendSignal` with the child's pid) and implements the timed wait.
- **Foreground service type.** Android 14+ requires a declared type. Because the engine may run
  for hours and may start from `BOOT_COMPLETED`, use `specialUse` (not `dataSync`, which Android 15
  caps at about 6 hours a day and blocks from boot). This is fine for sideloading; a future store
  listing would need the `specialUse` justification.
- **Engine configuration** is passed only as environment variables at process start. A settings
  change takes effect on the next engine start; the UI says so and offers a restart.
- **Data directories.** `OUTPACE_DATA_DIR` points into the app's private files dir;
  `OUTPACE_CACHE_DIR` into its cache dir. Uninstalling the app removes both.
- **Plain HTTP.** The engine serves `http://`. The app's own status requests to `127.0.0.1` need
  cleartext allowed for localhost only (network security config). Players fetch the stream
  themselves, so their own settings apply.
- **Efficiency.** UI status polling (every couple of seconds) happens only while an app screen is
  visible. While the engine runs with no screen visible, the service does one slow poll of
  `GET /streams` (suggested every 30 s) to drive the notification, the wake lock and idle
  auto-stop; the engine has no push signal, and a localhost request every 30 s is negligible.
  Nothing polls while the engine is stopped. The engine's stderr goes into a bounded in-memory
  ring buffer (a few hundred lines); the app never parses it.
- **Kotlin**, minimal dependencies (AndroidX core, the leanback launcher bits for TV). No
  cross-platform UI frameworks. With no engine bundled, the APK should stay well under 5 MB.
- **Location in the repo:** an `android/` directory with its own Gradle build. The Rust workspace
  is unaffected.
- Repository rules apply as usual: wiki updated in the same PR as user-visible changes, branch
  prefixes, no em dashes in docs, identifier hygiene.

## Decisions taken in conversation

These were agreed with the maintainer:

- Barebones engine manager, no built-in player.
- Feature set below, including `acestream://` link handling.
- Share/hand-off of a content id to an external player (VLC and others).
- Phones and Android TV are equally important.
- Phone auto-stop on idle, start-on-boot for TV, both as defaults the user can change.
- Download-only engine: no engine in the APK; the user picks a release; one launch mechanism
  (per Android version), chosen over a bundled engine or a bundled-plus-download hybrid to keep
  one way of running and debugging the engine.

## Features

### 1. Engine service

- A foreground service owns the engine process. Its persistent notification shows "running",
  the number of active streams, and a **Stop** action.
- States: stopped, starting, running, stopping, crashed. "Running" means `GET /healthz` returned
  `ok`. Starting times out (suggested 15 s) into "crashed" with the last log lines visible.
- If the engine exits unexpectedly, the app shows "crashed" and offers restart. Automatic restart
  is limited (suggested: once, then stay crashed) so a broken binary does not loop.
- A partial wake lock is held only while at least one stream is active, not merely while the
  engine runs.
- "Active stream" means `GET /streams` lists at least one session, regardless of its `clients`
  count. TS sessions linger for a grace period after the last viewer, and HLS sessions are kept
  alive by playlist fetches rather than connections, so `clients` is not a reliable viewer
  signal. VOD sessions do not appear in `/streams` at all; see the VOD note in feature 4.

### 2. Status screen (the main screen)

- Big start/stop control and current state.
- Active streams from `GET /streams`, which returns `network`, `id` (prefixed, `cid:<hex>` or
  `ih:<hex>`), `clients` and `metadata` (`title`, `bitrate`, `categories`). Show title, bitrate
  and clients. Peers come only from `GET /streams/ace/<id>/status`, one request per stream; fetch
  it only for a stream the user opens, not on every poll.
- The base URL players should use (`http://127.0.0.1:6878`, or the LAN address when LAN access
  is on).
- The paste box and the recent-streams list (feature 4) live here too, so the main screen is
  also the "play something" screen.

### 3. Settings

Each maps to an existing variable; see `docs/wiki/Environment-Variables.md` for exact semantics.

| Setting | Variable | App default |
| --- | --- | --- |
| Allow LAN access | `OUTPACE_BIND` (`0.0.0.0:<port>` vs `127.0.0.1:<port>`) | off |
| HTTP port | port part of `OUTPACE_BIND` | 6878 |
| Seeding | `OUTPACE_ENABLE_SEEDING` | engine default (on) |
| Accept inbound peers | `OUTPACE_ENABLE_INBOUND` | engine default (on) |
| Router port mapping | `OUTPACE_ENABLE_PORT_MAPPING` | engine default (off) |
| AceStream-compatible API | `OUTPACE_ACE_COMPAT` | off |
| Start on boot | app only | on for TV, off for phone |
| Stop when idle | app only | on for phone (suggested 10 min with no active streams), off for TV |

Turning on LAN access shows a one-line warning to use it only on a trusted network.
An "advanced" free-form list of extra `KEY=value` variables is the builder's call; if added, it
only accepts `OUTPACE_*` keys.

### 4. Play a content id in an external player

Entry points, all leading to the same flow:

- **Link taps:** intent filter for the `acestream` scheme (`acestream://<id>` and
  `acestream:?content_id=<id>`).
- **Share sheet:** `ACTION_SEND` with `text/plain`. The app extracts the first content id from the
  text: an `acestream://<id>` or `acestream:?content_id=<id>` link, or a bare 40-hex string (the
  engine also treats bare hex as a content id).
- **Paste box** on the main screen.
- **Recent streams:** the last N (suggested 10) ids the user played, stored locally, one click to
  replay. Essential on TV, where typing is painful. The user can clear it.

Only content ids are accepted. A bare 40-hex string cannot be told apart from an infohash, so it
is treated as a content id, matching the engine. Explicit infohash forms
(`acestream:?infohash=`, `magnet:` links) are rejected with a short explanation, because the
content-id path is the reliable one.

Flow:

1. Validate the id (40 hex characters).
2. If the engine is not running, start it and wait for `/healthz` (bounded by the start timeout).
3. Build `http://127.0.0.1:<port>/streams/ace/<id>` (or the LAN address, see below).
4. Fire `ACTION_VIEW` with that URI and MIME type `video/mp2t`, through the system chooser so the
   user can pick a player and tick "always".

Secondary actions on the same screen: **Copy URL**, **Share URL** (plain text, for sending to
another device), and **Open as HLS** (`/streams/ace/<id>.m3u8`, MIME
`application/vnd.apple.mpegurl`), for players that prefer HLS.

URL host rule: hand-offs to a player on this device always use `127.0.0.1`. "Copy URL" and
"Share URL" use the device's LAN address when LAN access is on, and `127.0.0.1` otherwise, with a
hint that other devices need LAN access enabled.

Live vs VOD: the native live route is `/streams/ace/<id>`; VOD content uses
`/vod/ace/cid:<id>`. The app cannot tell which a content id is before resolving it. v1 targets
live content. Whether to add VOD in v1 depends on what the live route returns for a VOD id;
see "Left to the builder".

### 5. Logs

- A screen showing the engine's recent output (the bounded ring buffer), with **Copy** and
  **Share** for bug reports. The engine's own `alog!` lines are the content; the app does not
  parse them.

### 6. Battery and boot

- A one-time prompt (and a settings entry) to exempt the app from battery optimisation, shown
  prominently on TV and on phones only when start-on-boot is enabled.
- `BOOT_COMPLETED` receiver starts the service when start-on-boot is on.

### 7. Engine versions

- **Source:** the GitHub releases of `outpace-stream/outpace-engine` (a constant in the app). The
  unauthenticated GitHub API allows 60 requests an hour per IP address, which users behind VPNs,
  carrier-grade NAT or shared networks can exhaust (observed during the design check). So:
  - "Latest" comes from `HEAD https://github.com/outpace-stream/outpace-engine/releases/latest`, which
    redirects to `/releases/tag/v<version>` and is not API rate-limited.
  - Downloads use the predictable URLs
    `https://github.com/outpace-stream/outpace-engine/releases/download/v<version>/outpace-<version>-<target>.tar.gz`
    and the same plus `.sha256`. Whether a release has an asset for the device is checked by
    fetching that small `.sha256` file.
  - Only the full list for the versions screen uses
    `GET https://api.github.com/repos/outpace-stream/outpace-engine/releases`, cached for 24 hours. If
    the API refuses (rate limit or network), the screen still shows the latest release and the
    installed ones, with a short note that the full list is temporarily unavailable.
- **Versions screen:** lists releases newest first. Each row shows the version, its date, and a
  badge: *running*, *installed*, *latest*, or *not available for this device* (no asset for the
  device's target, for example releases before v1.0.0, or 32-bit before the armv7 build ships).
  Drafts are never shown; pre-releases are hidden unless a "show pre-releases" toggle is on.
- **Install:** download the archive and its `.sha256` over HTTPS into the cache dir, verify the
  SHA-256, extract only the `outpace` entry from the tar.gz into
  `filesDir/engines/<version>/outpace`, then mark it installed. A failed or cancelled download
  leaves nothing behind. Download progress is shown; the download runs only while the user waits
  on that screen (no background download service in v1).
- **Activate:** choosing an installed version makes it the active engine; if the engine is
  running, the app restarts it on the new version.
- **Storage bound:** at most 3 installed versions; installing a fourth asks which to remove.
  The active version cannot be removed while it runs.
- **First run:** with no engine installed, the main screen's start control becomes "Download
  engine", preselecting the latest release that has an asset for the device.
- **Running vs latest:** the main screen shows "Engine vX.Y.Z" and, when a newer release with an
  asset for this device exists, "vA.B.C available" linking to the versions screen. The latest
  release is fetched when the main screen opens, at most once every 24 hours, plus on a manual
  refresh. Network failures are shown quietly and never block starting an installed engine.
- **Integrity scope:** the `.sha256` comes from the same release, so it detects corruption and
  truncation; transport security comes from HTTPS to GitHub. Signature verification is out of
  scope for v1.

### 8. Phone and TV presentation

- One APK. TV: `LEANBACK_LAUNCHER` intent filter, a launcher banner, `touchscreen` declared not
  required. Phone: normal launcher icon.
- Every screen is fully navigable with a D-pad: large focusable controls, visible focus, no
  gesture-only actions. Phone layouts are the same screens with touch, not a separate UI.

## Build, CI and release

- CI gains an `armv7-linux-androideabi` engine build next to the existing best-effort
  `aarch64-linux-android` one, with the same QEMU smoke test, and the release workflow uploads it
  the same way (its own archive and `.sha256`). These archives are what the app downloads, so
  their names and layout (`outpace-<version>-<target>/outpace` inside the tar.gz) are now an
  interface the app depends on.
- A workflow builds and unit-tests the APK on PRs touching `android/`, and attaches the APK to
  the GitHub release as a best-effort asset with its own `.sha256`. The APK contains no engine,
  so it does not depend on the engine build jobs.
- Release signing: a dedicated release keystore stored as a CI secret. Creating the key and
  adding the secrets is a maintainer task; until then CI builds an unsigned or debug-signed APK
  for testing only.
- App version name follows the engine release version (`X.Y.Z`); version code is derived from it.

## Documentation

- New wiki page `Android-App.md` (install, first start and engine download, choosing engine
  versions, playing a link, settings, battery advice, TV notes), linked from `_Sidebar.md` and
  `Home.md`.
- `Installation.md`: add the APK as the recommended Android route; keep Termux as the
  alternative.
- `Releases-and-Versions.md`: list the APK asset and its best-effort status.

## Testing

- Unit tests for the pure parts: content-id extraction from shared text and links (only the
  synthetic placeholder id in tests), URL building, settings-to-environment mapping, release-list
  parsing and asset matching, version comparison, SHA-256 file parsing, tar.gz extraction, and
  launch-command selection per API level and ELF class.
- Instrumented or manual test checklist: start/stop, crash detection (kill the process), link
  tap with the engine stopped, share-sheet hand-off to VLC, TV D-pad navigation, boot start, idle
  auto-stop. Live playback checks use a current content id resolved at runtime, never recorded.
- At least one manual pass on a real 32-bit TV device and a real 64-bit phone before the first
  release that ships the APK.

## Left to the builder

- Screen layouts, visuals, icon and banner design.
- Whether to use Jetpack Compose (with TV support) or Views; prefer whatever keeps D-pad focus
  handling simplest and the APK small.
- Exact timeouts, recent-list length and idle period (suggested values above).
- Whether the advanced extra-variables setting exists in v1.
- Live vs VOD: `/streams/ace/<id>` currently returns a bodyless 404 for most start failures, so
  it may not give a clean "this is VOD" signal. Verify what it returns for a VOD id. If a reliable
  signal exists (or a small engine issue can add one), fall back to `/vod/ace/cid:<id>`, and make
  idle auto-stop aware of VOD hand-offs, since VOD sessions are not listed in `/streams`. If not,
  ship live-only in v1 and say so in the wiki.
- Whether to add `x86_64` engine release assets for emulator use.
- How the APK workflow is split from or folded into `release.yml`.
