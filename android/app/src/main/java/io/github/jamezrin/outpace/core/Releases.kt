package io.github.jamezrin.outpace.core

import org.json.JSONArray
import org.json.JSONObject

/** One engine release as the app needs it. `assets` is empty when it came from the latest redirect. */
data class Release(
    val tag: String,
    val version: String,
    val publishedAt: String,
    val prerelease: Boolean,
    val assetNames: Set<String>,
)

object Releases {
    const val REPO = "jamezrin/outpace-stream"
    const val LIST_URL = "https://api.github.com/repos/$REPO/releases?per_page=30"
    const val LATEST_URL = "https://github.com/$REPO/releases/latest"

    fun archiveName(version: String, target: String) = "outpace-$version-$target.tar.gz"

    fun checksumName(version: String, target: String) = archiveName(version, target) + ".sha256"

    fun downloadUrl(version: String, name: String) = "https://github.com/$REPO/releases/download/v$version/$name"

    fun versionOf(tag: String): String = tag.removePrefix("v")

    /** Parses the GitHub list-releases payload, newest first, without drafts. */
    fun parseList(json: String): List<Release> {
        val array = JSONArray(json)
        val out = ArrayList<Release>(array.length())
        for (i in 0 until array.length()) {
            val o = array.getJSONObject(i)
            if (o.optBoolean("draft", false)) continue
            out += parseObject(o)
        }
        return out.sortedWith { a, b -> compareVersions(b.version, a.version) }
    }

    private fun parseObject(o: JSONObject): Release {
        val tag = o.getString("tag_name")
        val assets = o.optJSONArray("assets") ?: JSONArray()
        val names = HashSet<String>()
        for (i in 0 until assets.length()) names += assets.getJSONObject(i).getString("name")
        return Release(tag, versionOf(tag), o.optString("published_at", ""), o.optBoolean("prerelease", false), names)
    }

    /** `https://github.com/<repo>/releases/tag/v1.2.3` to `1.2.3`, or null for anything else. */
    fun versionFromLatestRedirect(location: String?): String? {
        if (location == null) return null
        val tag = location.substringAfter("/releases/tag/", "").substringBefore('?').trimEnd('/')
        if (tag.isEmpty() || !tag.startsWith("v")) return null
        val version = versionOf(tag)
        return version.takeIf { v -> v.isNotEmpty() && v.first().isDigit() }
    }

    /** The first device target (in ABI preference order) the release ships both files for. */
    fun pickTarget(release: Release, targets: List<String>): String? = targets.firstOrNull { t ->
        archiveName(release.version, t) in release.assetNames && checksumName(release.version, t) in release.assetNames
    }

    /** Numeric `X.Y.Z` comparison; a `-suffix` pre-release sorts below the plain version. */
    fun compareVersions(a: String, b: String): Int {
        val (coreA, preA) = split(a)
        val (coreB, preB) = split(b)
        for (i in 0 until maxOf(coreA.size, coreB.size)) {
            val c = (coreA.getOrElse(i) { 0 }).compareTo(coreB.getOrElse(i) { 0 })
            if (c != 0) return c
        }
        return when {
            preA == null && preB == null -> 0
            preA == null -> 1
            preB == null -> -1
            else -> preA.compareTo(preB)
        }
    }

    private fun split(v: String): Pair<List<Int>, String?> {
        val core = v.removePrefix("v").substringBefore('-')
        val pre = v.substringAfter('-', "").ifEmpty { null }
        return core.split('.').map { it.toIntOrNull() ?: 0 } to pre
    }
}
