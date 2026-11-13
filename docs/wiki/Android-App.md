# Android App

> **Unreleased:** the Android app first ships with the next release after v1.0.0. See
> [Releases and Versions](Releases-and-Versions) for publication status.

The outpace Android app runs the engine on a phone or an Android TV box without root and
without Termux. It downloads the engine release you choose, keeps it running in the background,
and hands streams to the video player you already use, such as VLC, MX Player, Just Player or
Kodi. It has no player of its own.

## Install

1. Download `outpace-android-<version>.apk` from the
   [releases page](https://github.com/jamezrin/outpace-stream/releases) and check it against its
   `.sha256` file.
2. Open it on the device and allow installing from that source when Android asks. On Android TV,
   copy it over with a USB drive or a file-transfer app, or install it from a computer with
   `adb install outpace-android-<version>.apk`.

The app needs Android 5 or newer, on 64-bit ARM (`arm64-v8a`) or 32-bit ARM (`armeabi-v7a`).
Many Android TV devices run 32-bit Android even on 64-bit chips; the app picks the right engine
build for you.

The APK is sideloaded only. It isn't on the Play Store, because Play policy doesn't allow apps
that download programs to run.

## First start

The APK contains no engine. On first start the main screen shows **Download engine**. It opens
**Engine versions** and offers the latest release built for your device. The app downloads the
archive, checks it against the release's `.sha256`, and installs it. After that, **Start engine**
runs it.

The engine listens on `127.0.0.1:6878`, the same address as on any other platform, so players on
the same device reach it with the usual [URLs](Playing-Streams).

## Playing a stream

You can start playback from:

- **A link.** Tap an `acestream://<id>` or `acestream:?content_id=<id>` link in a browser or chat
  app and choose outpace.
- **The share menu.** Share text that contains a content id or an `acestream:` link to outpace.
- **The main screen.** Paste a content id or link into the box and press **Play**.
- **Recent streams.** The main screen lists the last 10 content ids you played. This is the easy
  way to replay on a TV remote. **Clear recent** forgets them.

The app starts the engine if it isn't running, waits until it answers, then opens Android's
"Play with" chooser for `http://127.0.0.1:6878/streams/ace/cid:<id>`. Tick "Always" to go
straight to your favourite player next time. The same screen also has:

- **Open as HLS**, for players that handle `.m3u8` better than a raw MPEG-TS stream.
- **Copy URL** and **Share URL**, for players that don't appear in the chooser or for another
  device. To play on another device, turn on **Allow LAN access** first; the copied URL then
  uses this device's LAN address instead of `127.0.0.1`.

Only content ids are supported. Infohashes and magnet links are rejected, because the content id
path is the reliable one. A bare 40-character hex string is treated as a content id, as the
engine does.

The app plays live streams. VOD content ids are not handled yet.

## Engine versions

**Engine versions** lists outpace releases, newest first. Each row shows the release date and
badges:

| Badge | Meaning |
| --- | --- |
| running | The engine that is running now. |
| selected | The version that runs the next time the engine starts. |
| installed | Downloaded and ready on this device. |
| latest | The newest release. |
| pre-release | Hidden unless **Show pre-releases** is ticked. |
| not available for this device | That release has no Android build for this device. Releases before v1.0.0 have none, and 32-bit builds start with the release that ships the app. |

Choose a row to download it, switch to it or remove it. Switching while the engine runs restarts
it on the new version. Up to 3 versions can be installed at once. The running version can't be
removed.

The main screen shows the engine version and, when a newer release exists, a **vX.Y.Z available**
button. The app checks GitHub when the main screen opens, at most once a day, and never upgrades
the engine by itself.

GitHub limits how often its API can be called without an account, per IP address. Behind a VPN
or a shared mobile network that limit can run out. The full list then shows only the latest
and installed versions, with a short note. Downloads and the latest-version check don't use
that API and keep working.

## Settings

Settings apply the next time the engine starts. **Restart engine** applies them right away.

| Setting | Default | What it sets |
| --- | --- | --- |
| Allow LAN access | Off | [`OUTPACE_BIND`](Environment-Variables) to `0.0.0.0:<port>` instead of `127.0.0.1:<port>`. Use it only on a trusted network. |
| HTTP port | 6878 | The port in `OUTPACE_BIND`. |
| Seeding | On | [`OUTPACE_ENABLE_SEEDING`](Seeding-and-Peers) |
| Accept inbound peers | On | [`OUTPACE_ENABLE_INBOUND`](Seeding-and-Peers) |
| Router port mapping | Off | [`OUTPACE_ENABLE_PORT_MAPPING`](NAT-and-Port-Mapping) |
| AceStream-compatible API | Off | [`OUTPACE_ACE_COMPAT`](Playing-Streams#acestream-compatible-api) |
| Start on boot | On for TV, off for phones | Starts the engine when the device boots. |
| Stop after 10 minutes with no streams | On for phones, off for TV | Saves battery when nothing is playing. |

The engine's data and cache live in the app's private storage (`OUTPACE_DATA_DIR` and
`OUTPACE_CACHE_DIR`). Uninstalling the app removes them. The peer listener keeps its usual
default of `0.0.0.0:8621`, as on every platform; see [Seeding and Peers](Seeding-and-Peers).

## Background running and battery

While the engine runs, the app shows a notification with the stream count and a **Stop**
button. That notification is what keeps Android from closing the engine. On Android 13 and
newer, allow notifications when asked.

Some devices still stop background apps. If streams stop when the screen turns off, allow the app
to ignore battery optimisation (**Settings > Battery optimisation**). On TV, and when start on
boot is on, the app asks once.

The app holds a wake lock only while a stream is active. It checks the engine every 30 seconds
when no app screen is open, and every 2 seconds while one is.

## Logs and problems

**Logs** shows the engine's last 500 lines of output, plus the app's own lines starting with
`[app]`. **Copy** and **Share** help when you report a problem; remove content ids before posting
logs anywhere public.

- **"The engine exited (code N)"**: the app restarts a crashed engine once. If it exits again,
  it stays stopped so it can't loop. Check **Logs**, then start it again.
- **"No app on this device can play the stream"**: install a video player, or use **Copy URL**.
- **The engine shows up as `linker` in process lists**: on Android 10 and newer, apps can't run
  downloaded programs directly, so the app starts the engine through Android's own program
  loader (`/system/bin/linker64`, or `/system/bin/linker` for 32-bit). That is expected.

See also [Troubleshooting](Troubleshooting) and [Installation](Installation#android-with-termux-best-effort)
for running the engine under Termux instead.
