package io.github.jamezrin.outpace.net

import android.content.Context
import io.github.jamezrin.outpace.Device
import io.github.jamezrin.outpace.EngineFiles
import io.github.jamezrin.outpace.Prefs
import io.github.jamezrin.outpace.core.Release
import io.github.jamezrin.outpace.core.Releases
import java.io.File
import java.io.IOException

/**
 * Finds and installs engine releases. "Latest" uses the github.com redirect and downloads use
 * predictable URLs, so neither depends on the rate-limited API; only the full list does.
 */
class EngineReleases(private val context: Context) {
    private val prefs = Prefs(context)

    /** The latest release version, cached for a day unless [force]. */
    fun latest(force: Boolean = false): String? {
        val cached = prefs.latestVersion
        if (!force && cached != null && System.currentTimeMillis() - prefs.latestCheckedAt < Prefs.DAY_MS) return cached
        // A renamed or transferred repository answers with an extra redirect first; follow a few hops.
        var url = Releases.LATEST_URL
        var version: String? = null
        for (hop in 0 until MAX_REDIRECTS) {
            val location = Http.redirectLocation(url) ?: break
            version = Releases.versionFromLatestRedirect(location)
            if (version != null) break
            url = java.net.URL(java.net.URL(url), location).toString()
        }
        if (version == null) throw IOException("unexpected latest-release redirect")
        prefs.latestVersion = version
        return version
    }

    data class ListResult(val releases: List<Release>, val fromApi: Boolean, val error: String?)

    /** The release list, from the API (cached for a day) or, failing that, latest plus installed. */
    fun list(force: Boolean = false): ListResult {
        val cached = prefs.releaseListJson
        if (!force && cached != null && System.currentTimeMillis() - prefs.releaseListAt < Prefs.DAY_MS) {
            runCatching { return ListResult(Releases.parseList(cached), true, null) }
        }
        return try {
            val json = Http.get(Releases.LIST_URL, accept = "application/vnd.github+json")
            val parsed = Releases.parseList(json)
            prefs.releaseListJson = json
            ListResult(parsed, true, null)
        } catch (e: Exception) {
            val stale = cached?.let { runCatching { Releases.parseList(it) }.getOrNull() }
            if (stale != null) return ListResult(stale, true, describe(e))
            val versions = (listOfNotNull(runCatching { latest(force) }.getOrNull()) +
                EngineFiles.store(context).installed()).distinct()
            ListResult(versions.map { Release("v$it", it, "", false, emptySet()) }, false, describe(e))
        }
    }

    /**
     * The device target a release ships for. Uses the API's asset names when known, otherwise
     * probes the small `.sha256` files in ABI preference order. Returns (target, checksum text).
     */
    fun resolveTarget(release: Release): Pair<String, String>? {
        val targets = Device.targets()
        val candidates = if (release.assetNames.isNotEmpty()) {
            listOfNotNull(Releases.pickTarget(release, targets))
        } else {
            targets
        }
        for (t in candidates) {
            val url = Releases.downloadUrl(release.version, Releases.checksumName(release.version, t))
            try {
                return t to Http.get(url)
            } catch (e: HttpStatusException) {
                if (e.code != 404) throw e
            }
        }
        return null
    }

    /** Downloads, verifies and installs [release]; returns the installed version. */
    fun install(release: Release, cancelled: () -> Boolean, progress: (Long, Long) -> Unit): String {
        val (target, checksum) = resolveTarget(release) ?: throw IOException("no build of ${release.version} for this device")
        val name = Releases.archiveName(release.version, target)
        val dir = EngineFiles.downloadDir(context).apply { mkdirs() }
        val part = File(dir, "$name.part")
        try {
            Http.download(Releases.downloadUrl(release.version, name), part, cancelled, progress)
            EngineFiles.store(context).install(release.version, part, name, checksum)
            return release.version
        } finally {
            part.delete()
        }
    }

    companion object {
        private const val MAX_REDIRECTS = 4

        fun describe(e: Exception): String = when (e) {
            is HttpStatusException -> if (e.code == 403 || e.code == 429) "GitHub rate limit reached, try again later" else "HTTP ${e.code}"
            else -> e.message ?: e.javaClass.simpleName
        }
    }
}
