package io.github.jamezrin.outpace

import android.content.Context
import android.content.SharedPreferences
import io.github.jamezrin.outpace.core.ContentId
import io.github.jamezrin.outpace.core.EngineSettings

/** Everything the app remembers. Small values only; engine files live in [EngineFiles]. */
class Prefs(context: Context) {
    private val sp: SharedPreferences = context.applicationContext.getSharedPreferences("outpace", Context.MODE_PRIVATE)
    private val tv = Device.isTv(context)

    var settings: EngineSettings
        get() = EngineSettings(
            lan = sp.getBoolean("lan", false),
            port = sp.getInt("port", EngineSettings.DEFAULT_PORT),
            seeding = sp.getBoolean("seeding", true),
            inbound = sp.getBoolean("inbound", true),
            portMapping = sp.getBoolean("port_mapping", false),
            aceCompat = sp.getBoolean("ace_compat", false),
        )
        set(s) = sp.edit()
            .putBoolean("lan", s.lan).putInt("port", s.port).putBoolean("seeding", s.seeding)
            .putBoolean("inbound", s.inbound).putBoolean("port_mapping", s.portMapping)
            .putBoolean("ace_compat", s.aceCompat).apply()

    var activeVersion: String?
        get() = sp.getString("active_version", null)
        set(v) = sp.edit().putString("active_version", v).apply()

    var startOnBoot: Boolean
        get() = sp.getBoolean("start_on_boot", tv)
        set(v) = sp.edit().putBoolean("start_on_boot", v).apply()

    var idleStop: Boolean
        get() = sp.getBoolean("idle_stop", !tv)
        set(v) = sp.edit().putBoolean("idle_stop", v).apply()

    var showPrereleases: Boolean
        get() = sp.getBoolean("show_prereleases", false)
        set(v) = sp.edit().putBoolean("show_prereleases", v).apply()

    var batteryPromptShown: Boolean
        get() = sp.getBoolean("battery_prompt_shown", false)
        set(v) = sp.edit().putBoolean("battery_prompt_shown", v).apply()

    /** Latest release version from the redirect check, and when it was fetched. */
    var latestVersion: String?
        get() = sp.getString("latest_version", null)
        set(v) = sp.edit().putString("latest_version", v).putLong("latest_checked_at", System.currentTimeMillis()).apply()

    val latestCheckedAt: Long get() = sp.getLong("latest_checked_at", 0)

    var releaseListJson: String?
        get() = sp.getString("release_list", null)
        set(v) = sp.edit().putString("release_list", v).putLong("release_list_at", System.currentTimeMillis()).apply()

    val releaseListAt: Long get() = sp.getLong("release_list_at", 0)

    val recent: List<String>
        get() = sp.getString("recent", "").orEmpty().split(',').filter(ContentId::isValid)

    fun addRecent(id: String) {
        val list = (listOf(id) + recent.filter { it != id }).take(RECENT_MAX)
        sp.edit().putString("recent", list.joinToString(",")).apply()
    }

    fun clearRecent() = sp.edit().remove("recent").apply()

    companion object {
        const val RECENT_MAX = 10
        const val IDLE_STOP_MS = 10 * 60_000L
        const val DAY_MS = 24 * 60 * 60_000L
    }
}
