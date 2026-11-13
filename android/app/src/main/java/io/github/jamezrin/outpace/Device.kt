package io.github.jamezrin.outpace

import android.app.UiModeManager
import android.content.Context
import android.content.pm.PackageManager
import android.content.res.Configuration
import android.os.Build
import io.github.jamezrin.outpace.core.Abi
import io.github.jamezrin.outpace.core.EngineStore
import java.io.File
import java.net.Inet4Address
import java.net.NetworkInterface

object Device {
    fun isTv(context: Context): Boolean {
        val ui = context.getSystemService(Context.UI_MODE_SERVICE) as UiModeManager
        return ui.currentModeType == Configuration.UI_MODE_TYPE_TELEVISION ||
            context.packageManager.hasSystemFeature(PackageManager.FEATURE_LEANBACK)
    }

    /** Release targets this device can run, most preferred first. */
    fun targets(): List<String> {
        val override = BuildConfig.ENGINE_TARGETS_OVERRIDE
        if (override.isNotEmpty()) return override.split(',')
        return Abi.targetsFor(Build.SUPPORTED_ABIS.toList())
    }

    /** A private IPv4 LAN address, for URLs that other devices open. */
    fun lanAddress(): String? = try {
        NetworkInterface.getNetworkInterfaces().toList()
            .filter { it.isUp && !it.isLoopback }
            .sortedBy { if (it.name.startsWith("wlan") || it.name.startsWith("eth")) 0 else 1 }
            .flatMap { it.inetAddresses.toList() }
            .firstOrNull { it is Inet4Address && it.isSiteLocalAddress }
            ?.hostAddress
    } catch (_: Exception) {
        null
    }
}

/** Where engine binaries, data and cache live inside app storage. */
object EngineFiles {
    fun store(context: Context) = EngineStore(context.filesDir)

    fun dataDir(context: Context) = File(context.filesDir, "data")

    fun cacheDir(context: Context) = File(context.cacheDir, "engine")

    fun downloadDir(context: Context) = File(context.cacheDir, "downloads")

    /** The version to run: the chosen one if still installed, else the newest installed. */
    fun activeVersion(context: Context): String? {
        val store = store(context)
        val chosen = Prefs(context).activeVersion
        return chosen?.takeIf(store::isInstalled) ?: store.installed().firstOrNull()
    }
}
