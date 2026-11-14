# Android TV

The [companion app](Android-App) runs on Android TV and Google TV. It is not on the Play Store,
so you install the signed APK from a computer on the same network. The steps below are the ones
that worked on a TCL 55T69C (Google TV, Android 12). Other sets use the same path when they
expose network `adb`; the menu names differ.

This installs the app, which then downloads an engine build for the television. It is not the
[Termux](Installation#android-with-termux-best-effort) install.

You need:

- the television and the computer on the same LAN;
- [Android platform-tools](https://developer.android.com/tools/releases/platform-tools) (`adb`) on the computer;
- a video player already installed on the television, such as VLC. The app has no player of its own.

## Download the APK

From the [Android app releases](https://github.com/outpace-stream/outpace-android/releases),
download `outpace-android-<version>.apk` and the matching `.sha256` file. Check the file before
installing it:

```bash
sha256sum -c outpace-android-0.1.0.apk.sha256
```

App releases start at v0.1.0. That package name is `io.github.outpacestream.android`.

## Turn on USB debugging

On the television:

1. Press **Home**, open **Settings → System → About**.
2. Select **Android TV OS build** and press OK seven times, until the set says you are a developer.
3. Go back to **System → Developer options**.
4. Turn on **USB debugging**.

Some sets also list **Wireless debugging**, with a pairing code and a port. Use that screen when
it is there. The TCL 55T69C does not have it. On that set, USB debugging is enough: the television
accepts a normal `adb connect` on port **5555**.

Find the television's address under **Settings → Network & Internet**, or in the router's client
list. The examples use `192.0.2.10`. Use the address of your set.

## Allow this computer

```bash
adb connect 192.0.2.10:5555
adb devices
```

`device` means the television accepted this computer. `unauthorized` means it is waiting for
**Allow USB debugging?**

That dialog is not drawn while the panel is off. A connection made in standby still marks the
computer unauthorized, and the prompt sits in a queue you cannot see. Turn the panel on, press
**Home**, then **Back** until the dialog is in front. Choose **Allow**. Tick **Always allow from
this computer** if you will install again from the same machine.

If Back never reveals a dialog, open **Developer options → Revoke USB debugging authorizations**,
return to **Home**, and run `adb connect` once. Connecting again and again stacks another prompt
each time; dismiss the stack with Back before the next attempt.

## Install and open

```bash
adb install -r outpace-android-0.1.0.apk
```

The app appears in the television's app row. Open it with the remote. The APK does not contain
the engine: the first screen offers **Download engine**, then **Start engine**. The app picks
the engine archive for this device (64-bit or 32-bit). 32-bit engine archives start with engine
v1.0.2.

Play from **Home** or **Search**, then pick the player in the chooser. **Open as HLS** is there
for players that handle `.m3u8` better than MPEG-TS. **Recently played** on Home is the easiest
way to start the same stream again from the remote. The rest of the screens are in the
[app user guide](https://github.com/outpace-stream/outpace-android/blob/main/docs/Android-App.md).

## When you are finished installing

USB debugging leaves port 5555 open on the LAN. Anyone on that network who can reach the
television can try to connect. Turn **USB debugging** off in Developer options when you no
longer need to install from the computer. Playback does not need it.
