# Android TV

The [companion app](Android-App) runs on Android TV and Google TV. It is not on the Play Store,
so you install the signed APK from a computer on the same network. The steps below are the ones
that worked on a TCL 55T69C (Google TV, Android 12). That set has no **Wireless debugging** row,
so the computer connects with `adb` on port 5555. A set that does list Wireless debugging is
paired first; that path is under [Turn on USB debugging](#turn-on-usb-debugging).

This installs the app, which then downloads an engine build for the television. It is not the
[Termux](Installation#android-with-termux-best-effort) install.

You need:

- the television and the computer on the same LAN;
- [Android platform-tools](https://developer.android.com/tools/releases/platform-tools) (`adb`) on the computer;
- a video player already installed on the television, such as VLC. The app has no player of its own.

## Download the APK

From the [Android app releases](https://github.com/outpace-stream/outpace-android/releases),
download `outpace-android-<version>.apk` and the matching `.sha256` file. Check the file before
installing it.

Linux:

```bash
sha256sum -c outpace-android-0.1.0.apk.sha256
```

macOS does not ship `sha256sum`. `shasum` reads the same file:

```bash
shasum -a 256 -c outpace-android-0.1.0.apk.sha256
```

Windows PowerShell prints `True` when the hash matches. `Get-FileHash` uses uppercase; the
comparison is case-insensitive:

```powershell
$hash = (Get-FileHash .\outpace-android-0.1.0.apk -Algorithm SHA256).Hash
$expected = (Get-Content .\outpace-android-0.1.0.apk.sha256 -Raw).Split()[0]
$hash -eq $expected
```

App releases start at v0.1.0. That package name is `io.github.outpacestream.android`.

## Turn on USB debugging

On the television:

1. Press **Home**, open **Settings → System → About**.
2. Select **Android TV OS build** and press OK seven times, until the set says you are a developer.
3. Go back to **System → Developer options**.
4. Turn on **USB debugging**.

The TCL 55T69C does not list **Wireless debugging**. On that set, USB debugging is enough: the
television accepts `adb connect` on port **5555**. Skip the pairing steps and continue at
[Allow this computer](#allow-this-computer).

A set that does list **Wireless debugging** does not use port 5555 until it is paired. Turn
**Wireless debugging** on, then open **Pair device with pairing code**. That is what shows the
pairing address, the pairing port, and the pairing code. The ports below are examples; use the
ones on the screen. The pairing port and the connection port are different.

```bash
adb pair 192.0.2.10:37123
```

Enter the code when `adb` asks. Then connect to the address and port shown for the connection,
not the pairing port:

```bash
adb connect 192.0.2.10:39555
adb devices
```

A paired connection is ready when that line says `device`. It does not show **Allow USB
debugging?** Do not follow the revoke steps below for this path: revoking authorizations drops
the pairing, and you pair again from **Pair device with pairing code**. Turn **Wireless
debugging** off when you finish installing.

Find the television's address under **Settings → Network & Internet**, or in the router's client
list. The examples use `192.0.2.10`. Use the address of your set.

## Allow this computer

On a set that uses port 5555, including the TCL 55T69C:

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
each time; dismiss the stack with Back before the next attempt. These steps are for the port
5555 path. On a set you paired with **Wireless debugging**, revoking authorizations removes the
pairing; open **Pair device with pairing code** and pair again instead of waiting for this dialog.

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
longer need to install from the computer, and turn **Wireless debugging** off too if you used it.
Playback does not need either one.
