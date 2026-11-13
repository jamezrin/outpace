# Android companion app Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** An Android app (phone and TV) that downloads an outpace engine release, runs and supervises it, and hands content ids to an external player.

**Architecture:** Plain Kotlin app with no third-party dependencies. Pure logic lives in `core/` and is JVM unit-tested. The Android parts (service, activities, receiver) stay thin over it. The engine is a child process started through the system loader on API 29+.

**Tech Stack:** Kotlin, AGP 9.4.1 (built-in Kotlin), Gradle 9.8 wrapper, platform `android.app.Activity` with XML layouts and the DeviceDefault theme, `org.json` (platform; `org.json:json` for JVM tests), JUnit 4.

**Spec:** `docs/superpowers/specs/2026-11-13-android-app-design.md`

## Global Constraints

- `minSdk 21`, `targetSdk 36`, `compileSdk 36`. Application id `io.github.jamezrin.outpace`.
- No dependencies beyond the Kotlin stdlib at runtime. Test-only: `junit:junit`, `org.json:json`.
- Releases repo constant: `jamezrin/outpace-stream`. Assets: `outpace-<version>-<target>.tar.gz` and `.tar.gz.sha256` (`sha256sum` format). The tag is `v<version>`.
- ABI to target: `arm64-v8a` → `aarch64-linux-android`, `armeabi-v7a` → `armv7-linux-androideabi`, `x86_64` → `x86_64-linux-android`, `x86` → `i686-linux-android`. Walk `Build.SUPPORTED_ABIS` in order.
- Launch: API ≥ 29 → `[/system/bin/linker64 | /system/bin/linker, <bin>, "serve"]` by the engine's ELF class; API < 29 → `[<bin>, "serve"]` (confirmed in Task 0).
- Engines live in `filesDir/engines/<version>/outpace`; at most 3 installed.
- Defaults: port 6878, LAN off, start on boot (TV on / phone off), idle stop (phone on, 10 min / TV off), recent list 10, start timeout 15 s, background poll 30 s, UI poll 2 s, log ring 500 lines, update check at most every 24 h.
- Copy: no em dashes. Tests and docs use only the placeholder id `0123456789abcdef0123456789abcdef01234567`.

## Review Focus

- Shared text with an id surrounded by punctuation or inside a URL query (`acestream:?content_id=` in a web link): extract it (Task 1 test `extractsFromQueryInsideText`).
- Upper-case hex ids: accepted and normalised to lower case (Task 1 `normalisesCase`).
- A release with an arm64 asset missing but armv7 present, on a device listing both ABIs: armv7 is chosen (Task 2 `fallsBackToNextAbi`).
- A tar.gz with GNU long-name or pax headers before `outpace`: still extracted (Task 3 `skipsPaxAndLongNameHeaders`).
- A `.sha256` file whose filename doesn't match the archive, or a hash of the wrong length: rejected (Task 3 `rejectsMismatchedName`).

---

### Task 0: Loader facts (throwaway check, already started)

- [x] API 35, x86_64: direct exec is denied; `linker64` starts the engine; `/healthz` answers; SIGTERM gives exit 0.
- [x] API 28: direct exec works and `linker64 <bin>` fails, which sets the cutoff at 29.
- [x] A 32-bit engine (i686) runs through `/system/bin/linker` on API 30 x86 (the API 35 image is 64-bit only and has no `linker`).
- [x] `Process.destroy()` delivers SIGTERM (the engine logs `shutdown signal received`) on API 28 and 35.
- [x] HTTPS to github.com (latest redirect) and to the asset host works on API 28, 30 and 35. The API is rate-limited per IP, see the spec.

Record the results in the spec's Constraints section.

### Task 1: Content ids and URLs (`core/ContentId.kt`, `core/StreamUrls.kt`)

**Produces:**
- `object ContentId { fun extract(text: String): Result }`, where `sealed class Result { data class Id(val hex: String); object Infohash; object None }`.
- `object StreamUrls { fun live(host: String, port: Int, id: String): String; fun hls(...): String; fun base(host, port): String }`.

Tests (`ContentIdTest`): `acceptsSchemeLink` (`acestream://<id>`), `acceptsQueryLink` (`acestream:?content_id=<id>`), `acceptsBareHex`, `extractsFromQueryInsideText`, `normalisesCase`, `rejectsInfohashQuery` (`acestream:?infohash=` → `Infohash`), `rejectsMagnet` (`magnet:?xt=urn:btih:` → `Infohash`), `noneForGarbage`, `ignoresLongerHexRuns` (a 64-hex run is not an id). `StreamUrlsTest`: the live URL equals `http://127.0.0.1:6878/streams/ace/<id>`, and the HLS URL ends in `.m3u8`.

### Task 2: Releases, versions and asset choice (`core/Releases.kt`, `core/Abi.kt`)

**Produces:**
- `data class Asset(val name: String, val url: String, val size: Long)`.
- `data class Release(val tag: String, val version: String, val publishedAt: String, val prerelease: Boolean, val assets: List<Asset>)`.
- `fun parseReleases(json: String): List<Release>` (drafts dropped), and `fun parseRelease(json: String): Release`.
- `fun compareVersions(a: String, b: String): Int` (numeric `X.Y.Z`, with a pre-release suffix sorting lower).
- `object Abi { fun targetFor(abi: String): String?; fun pick(release: Release, abis: List<String>): EngineAsset? }`, where `data class EngineAsset(val target: String, val archive: Asset, val checksum: Asset)`.

Tests: `parsesGithubPayload`, using a trimmed fixture built from the real API shape with no ids in it. Also `dropsDrafts`, `versionStripsV`, `comparesNumerically` (`1.10.0 > 1.9.0`), `prereleaseSortsLower`, `picksFirstAbiWithAsset`, `fallsBackToNextAbi`, `nullWhenNoAsset`, `nullWithoutChecksum`.

### Task 3: Archive install (`core/Sha256File.kt`, `core/TarGz.kt`, `core/EngineStore.kt`)

**Produces:**
- `fun parseSha256(text: String, expectedName: String): String` throws `IllegalArgumentException`.
- `fun sha256Hex(file: File): String`.
- `object TarGz { fun extractEntry(archive: File, entrySuffix: String, dest: File): Boolean }`. It handles ustar and GNU headers, skips `x`/`g`/`L` entries and treats the `L` payload as the next name, and stops at two zero blocks.
- `class EngineStore(root: File) { fun installed(): List<String>; fun binary(version: String): File; fun install(version: String, archive: File, sha256File: File, archiveName: String): File; fun remove(version: String); companion object { const val MAX_INSTALLED = 3 } }`. `install` verifies, extracts into a temp dir, then renames into place. On any failure nothing stays behind.

Tests: `parsesSha256sumLine`, `rejectsMismatchedName`, `rejectsBadHash`, `extractsOutpaceEntry` (fixture built in the test with the same layout as the release: `outpace-<v>-<target>/outpace`, plus LICENSE and README), `skipsPaxAndLongNameHeaders`, `installVerifiesChecksum` (a bad hash leaves no `engines/<v>`), `installSetsExecutable`, `listsInstalledSortedNewestFirst`. A manual one-off check runs `extractEntry` against the real v1.0.0 archive in the scratchpad.

### Task 4: Launch and settings (`core/Launch.kt`, `core/EngineSettings.kt`, `core/LogRing.kt`, `core/StreamsJson.kt`)

**Produces:**
- `fun elfIs64(file: File): Boolean` (byte 4 == 2).
- `fun launchCommand(api: Int, is64: Boolean, bin: String): List<String>`.
- `data class EngineSettings(val lan: Boolean, val port: Int, val seeding: Boolean, val inbound: Boolean, val portMapping: Boolean, val aceCompat: Boolean)` with `fun env(dataDir: String, cacheDir: String): Map<String, String>`. `OUTPACE_BIND` is `127.0.0.1:<port>` or `0.0.0.0:<port>`; the booleans are `true`/`false`.
- `class LogRing(capacity: Int) { fun add(line: String); fun snapshot(): List<String> }`.
- `data class StreamInfo(val id: String, val title: String?, val bitrate: Long?, val clients: Int)` and `fun parseStreams(json: String): List<StreamInfo>`.

Tests: `api29UsesLinker64`, `api29Uses32BitLinker`, `api28ExecsDirectly`, `elfClassFromHeader`, `envLoopbackByDefault`, `envLanBind`, `envFlags`, `ringDropsOldest`, `parsesStreamsPayload` (prefixed ids are kept), and `parsesEmptyStreams`.

### Task 5: Android app shell, service and screens

**Files:** `android/settings.gradle.kts`, `android/build.gradle.kts`, `android/gradle.properties`, the wrapper, `android/app/build.gradle.kts`, `AndroidManifest.xml`, `res/` (layouts, strings, banner, icon, `xml/network_security_config.xml`), plus:
- `Prefs.kt`: settings, active version, recent ids, last update check.
- `engine/EngineController.kt`: a process-wide singleton. It holds `state: EngineState` (Stopped, Starting, Running, Stopping, Crashed(reason)), the streams list, a `LogRing`, and listener registration. All mutation happens on the main thread.
- `engine/EngineService.kt`: the foreground service (`specialUse`). It runs start/stop/restart intents, spawns via `launchCommand`, pumps stderr into the ring, waits for health, polls `/streams` every 30 s, holds the wake lock while streams exist, runs idle stop, restarts automatically once, stops with `destroy()` (SIGTERM per Task 0) and force-kills after 5 s, and updates the notification.
- `net/Http.kt`: `get(url): String` and `download(url, dest, progress)` over `HttpURLConnection`, following redirects, with timeouts.
- `ui/MainActivity.kt`: state, version line, update hint, start/stop, paste box, recent list, active streams, and links to Versions, Settings and Logs.
- `ui/PlayActivity.kt`: handles `VIEW acestream:` and `SEND text/plain` plus internal play requests. It extracts the id, ensures the engine is running, then shows player, copy, share and HLS actions, firing the player chooser by default.
- `ui/VersionsActivity.kt`: the release list with badges, download with progress, activate, remove.
- `ui/SettingsActivity.kt` and `ui/LogsActivity.kt`.
- `BootReceiver.kt`.

The manifest must include `INTERNET`, `FOREGROUND_SERVICE`, `FOREGROUND_SERVICE_SPECIAL_USE`, `POST_NOTIFICATIONS` (requested at runtime on 33+), `WAKE_LOCK`, `RECEIVE_BOOT_COMPLETED`, and `REQUEST_IGNORE_BATTERY_OPTIMIZATIONS`. It also needs the special-use `<property>`, `LEANBACK_LAUNCHER` with a banner, `touchscreen required=false`, and an `acestream` scheme filter with `BROWSABLE`. Cleartext is allowed only for `127.0.0.1` and `localhost`.

Verify: `./gradlew :app:assembleDebug :app:testDebugUnitTest`, then a manual emulator pass covering first-run download (from a local release mirror or GitHub), start, `/healthz`, a play hand-off, stop, and the versions screen.

### Task 6: CI, release and wiki

- `android.yml`: add `armv7-linux-androideabi` to the build and smoke matrix, and add an `app` job that runs on `android/**` (`./gradlew testDebugUnitTest assembleRelease`, unsigned unless the secrets exist).
- `release.yml`: make the best-effort Android jobs a matrix over `aarch64-linux-android` and `armv7-linux-androideabi`, and add the APK build and upload with its `.sha256`.
- Wiki: create `Android-App.md` and link it from `_Sidebar.md` and `Home.md`; update `Installation.md` and `Releases-and-Versions.md`; run `python3 tools/wiki/check_links.py docs/wiki`.
- Gates: `python3 tools/hygiene/check_identifiers.py`, and confirm `cargo fmt --all --check` is unaffected (no Rust changes).
