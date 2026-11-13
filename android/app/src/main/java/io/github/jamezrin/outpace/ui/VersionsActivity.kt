package io.github.jamezrin.outpace.ui

import android.app.Activity
import android.app.AlertDialog
import android.os.Bundle
import android.view.View
import android.widget.Button
import android.widget.CheckBox
import android.widget.LinearLayout
import android.widget.ProgressBar
import android.widget.TextView
import io.github.jamezrin.outpace.Device
import io.github.jamezrin.outpace.EngineFiles
import io.github.jamezrin.outpace.Prefs
import io.github.jamezrin.outpace.R
import io.github.jamezrin.outpace.core.EngineStore
import io.github.jamezrin.outpace.core.Release
import io.github.jamezrin.outpace.core.Releases
import io.github.jamezrin.outpace.engine.EngineController
import io.github.jamezrin.outpace.engine.EngineState
import io.github.jamezrin.outpace.net.EngineReleases
import java.io.InterruptedIOException

class VersionsActivity : Activity() {
    private lateinit var prefs: Prefs
    private lateinit var releases: EngineReleases
    private lateinit var running: TextView
    private lateinit var latest: TextView
    private lateinit var note: TextView
    private lateinit var list: LinearLayout
    private lateinit var download: View
    private lateinit var downloadText: TextView
    private lateinit var downloadProgress: ProgressBar
    private var result: EngineReleases.ListResult? = null
    @Volatile private var cancel = false
    private var busy = false
    private val listener: () -> Unit = { renderHeader(); renderList() }

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setTitle(R.string.versions_title)
        setContentView(R.layout.activity_versions)
        prefs = Prefs(this)
        releases = EngineReleases(this)
        running = findViewById(R.id.running)
        latest = findViewById(R.id.latest)
        note = findViewById(R.id.note)
        list = findViewById(R.id.list)
        download = findViewById(R.id.download)
        downloadText = findViewById(R.id.download_text)
        downloadProgress = findViewById(R.id.download_progress)
        findViewById<Button>(R.id.refresh).setOnClickListener { load(force = true) }
        findViewById<Button>(R.id.download_cancel).setOnClickListener { cancel = true }
        findViewById<CheckBox>(R.id.show_pre).apply {
            isChecked = prefs.showPrereleases
            setOnCheckedChangeListener { _, checked -> prefs.showPrereleases = checked; renderList() }
        }
        load(force = false)
    }

    override fun onStart() {
        super.onStart()
        EngineController.addListener(listener)
        renderHeader()
    }

    override fun onStop() {
        EngineController.removeListener(listener)
        super.onStop()
    }

    override fun onDestroy() {
        cancel = true
        super.onDestroy()
    }

    private fun load(force: Boolean) {
        note.visibility = View.GONE
        background.execute {
            val r = releases.list(force)
            if (force || prefs.latestVersion == null) runCatching { releases.latest(force = true) }
            onMain {
                result = r
                if (r.error != null) {
                    note.visibility = View.VISIBLE
                    note.text = if (r.fromApi) getString(R.string.versions_error, r.error) else getString(R.string.versions_partial, r.error)
                }
                renderHeader()
                renderList()
                autoOfferFirstInstall()
            }
        }
    }

    private fun renderHeader() {
        running.text = getString(R.string.versions_running, EngineController.runningVersion?.let { "v$it" } ?: getString(R.string.versions_none_running))
        latest.text = getString(R.string.versions_latest, prefs.latestVersion?.let { "v$it" } ?: getString(R.string.versions_unknown))
    }

    private fun visibleReleases(): List<Release> {
        val r = result ?: return emptyList()
        val installed = EngineFiles.store(this).installed()
        val all = r.releases.filter { prefs.showPrereleases || !it.prerelease || it.version in installed }
        // Installed versions that have left the list (for example deleted releases) stay manageable.
        val extra = installed.filter { v -> all.none { it.version == v } }.map { Release("v$it", it, "", false, emptySet()) }
        return (all + extra).sortedWith { a, b -> Releases.compareVersions(b.version, a.version) }
    }

    private fun renderList() {
        list.removeAllViews()
        val store = EngineFiles.store(this)
        val active = EngineFiles.activeVersion(this)
        val targets = Device.targets()
        val fromApi = result?.fromApi == true
        for (rel in visibleReleases()) {
            val badges = ArrayList<String>()
            val installed = store.isInstalled(rel.version)
            if (rel.version == EngineController.runningVersion) badges += getString(R.string.badge_running)
            else if (rel.version == active) badges += getString(R.string.badge_active)
            if (installed) badges += getString(R.string.badge_installed)
            if (rel.version == prefs.latestVersion) badges += getString(R.string.badge_latest)
            if (rel.prerelease) badges += getString(R.string.badge_prerelease)
            // Only API entries carry asset names; entries from the latest redirect are probed on install.
            val fromList = fromApi && result?.releases?.contains(rel) == true
            val unavailable = !installed && fromList && Releases.pickTarget(rel, targets) == null
            if (unavailable) badges += getString(R.string.badge_unavailable)
            val date = rel.publishedAt.take(10)
            val label = buildString {
                append("v").append(rel.version)
                if (date.isNotEmpty()) append("  ").append(date)
                if (badges.isNotEmpty()) append("  (").append(badges.joinToString(", ")).append(")")
            }
            list.addRow(label, if (unavailable) null else ({ choose(rel) }))
        }
    }

    private fun choose(rel: Release) {
        if (busy) return
        val store = EngineFiles.store(this)
        val installed = store.isInstalled(rel.version)
        val options = ArrayList<Pair<String, () -> Unit>>()
        if (installed) {
            options += getString(R.string.version_use) to { activate(rel.version) }
            options += getString(R.string.version_remove) to { remove(rel.version) }
        } else {
            options += getString(R.string.version_install) to { install(rel) }
        }
        AlertDialog.Builder(this)
            .setTitle("v${rel.version}")
            .setItems(options.map { it.first }.toTypedArray()) { _, which -> options[which].second() }
            .setNegativeButton(R.string.cancel, null)
            .show()
    }

    private fun activate(version: String) {
        prefs.activeVersion = version
        val s = EngineController.state
        if ((s == EngineState.Running || s == EngineState.Starting) && EngineController.runningVersion != version) {
            EngineController.restart(this)
        }
        renderList()
    }

    private fun remove(version: String) {
        if (version == EngineController.runningVersion) {
            toast(getString(R.string.version_remove_running))
            return
        }
        EngineFiles.store(this).remove(version)
        if (prefs.activeVersion == version) prefs.activeVersion = null
        renderList()
    }

    private fun install(rel: Release) {
        val store = EngineFiles.store(this)
        if (store.installed().size >= EngineStore.MAX_INSTALLED) {
            toast(getString(R.string.version_full, EngineStore.MAX_INSTALLED))
            return
        }
        busy = true
        cancel = false
        download.visibility = View.VISIBLE
        downloadProgress.isIndeterminate = true
        downloadText.text = getString(R.string.version_downloading, rel.version, "")
        background.execute {
            val outcome = try {
                releases.install(rel, cancelled = { cancel }) { done, total ->
                    onMain {
                        val mb = "%.1f MB".format(done / 1_048_576.0)
                        downloadText.text = getString(R.string.version_downloading, rel.version, mb)
                        if (total > 0) {
                            downloadProgress.isIndeterminate = false
                            downloadProgress.progress = (done * 1000 / total).toInt()
                        }
                    }
                }
                null
            } catch (_: InterruptedIOException) {
                ""
            } catch (e: Exception) {
                EngineReleases.describe(e)
            }
            onMain {
                busy = false
                download.visibility = View.GONE
                when (outcome) {
                    null -> {
                        toast(getString(R.string.version_installed, rel.version))
                        activate(rel.version)
                    }
                    "" -> Unit
                    else -> toast(getString(R.string.version_failed, outcome))
                }
                renderList()
            }
        }
    }

    /** First run: with nothing installed, offer the latest release straight away. */
    private fun autoOfferFirstInstall() {
        if (busy || EngineFiles.store(this).installed().isNotEmpty()) return
        val latestVersion = prefs.latestVersion ?: return
        val rel = visibleReleases().firstOrNull { it.version == latestVersion } ?: Release("v$latestVersion", latestVersion, "", false, emptySet())
        if (rel.assetNames.isNotEmpty() && Releases.pickTarget(rel, Device.targets()) == null) return
        choose(rel)
    }
}
