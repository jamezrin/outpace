package io.github.jamezrin.outpace.ui

import android.Manifest
import android.app.Activity
import android.content.Intent
import android.content.pm.PackageManager
import android.os.Build
import android.os.Bundle
import android.view.KeyEvent
import android.view.View
import android.view.inputmethod.EditorInfo
import android.widget.Button
import android.widget.EditText
import android.widget.LinearLayout
import android.widget.TextView
import io.github.jamezrin.outpace.Device
import io.github.jamezrin.outpace.EngineFiles
import io.github.jamezrin.outpace.Prefs
import io.github.jamezrin.outpace.R
import io.github.jamezrin.outpace.core.Releases
import io.github.jamezrin.outpace.core.StreamUrls
import io.github.jamezrin.outpace.engine.EngineController
import io.github.jamezrin.outpace.engine.EngineState
import io.github.jamezrin.outpace.net.EngineReleases

class MainActivity : Activity() {
    private lateinit var prefs: Prefs
    private lateinit var state: TextView
    private lateinit var version: TextView
    private lateinit var update: Button
    private lateinit var startStop: Button
    private lateinit var url: TextView
    private lateinit var idInput: EditText
    private lateinit var recent: LinearLayout
    private lateinit var recentTitle: View
    private lateinit var clearRecent: Button
    private lateinit var streams: LinearLayout
    private val listener: () -> Unit = { render() }

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setContentView(R.layout.activity_main)
        prefs = Prefs(this)
        state = findViewById(R.id.state)
        version = findViewById(R.id.version)
        update = findViewById(R.id.update)
        startStop = findViewById(R.id.start_stop)
        url = findViewById(R.id.url)
        idInput = findViewById(R.id.id_input)
        recent = findViewById(R.id.recent)
        recentTitle = findViewById(R.id.recent_title)
        clearRecent = findViewById(R.id.clear_recent)
        streams = findViewById(R.id.streams)

        startStop.setOnClickListener { onStartStop() }
        update.setOnClickListener { open(VersionsActivity::class.java) }
        findViewById<Button>(R.id.play).setOnClickListener { playInput() }
        idInput.setOnEditorActionListener { _, action, event ->
            val enter = event?.keyCode == KeyEvent.KEYCODE_ENTER && event.action == KeyEvent.ACTION_UP
            if (action == EditorInfo.IME_ACTION_GO || enter) { playInput(); true } else false
        }
        clearRecent.setOnClickListener { prefs.clearRecent(); render() }
        findViewById<Button>(R.id.versions).setOnClickListener { open(VersionsActivity::class.java) }
        findViewById<Button>(R.id.settings).setOnClickListener { open(SettingsActivity::class.java) }
        findViewById<Button>(R.id.logs).setOnClickListener { open(LogsActivity::class.java) }
        startStop.requestFocus()
    }

    override fun onStart() {
        super.onStart()
        EngineController.addListener(listener)
        EngineController.screenShown()
        render()
        checkLatest()
    }

    override fun onStop() {
        EngineController.removeListener(listener)
        EngineController.screenHidden()
        super.onStop()
    }

    private fun open(cls: Class<*>) = startActivity(Intent(this, cls))

    private fun onStartStop() {
        val s = EngineController.state
        when {
            EngineFiles.activeVersion(this) == null -> open(VersionsActivity::class.java)
            s == EngineState.Running || s == EngineState.Starting -> EngineController.stop(this)
            s == EngineState.Stopping -> Unit
            else -> {
                askNotificationPermission()
                BatteryPrompt.maybeShow(this)
                EngineController.start(this)
            }
        }
    }

    private fun askNotificationPermission() {
        if (Build.VERSION.SDK_INT >= 33 && checkSelfPermission(Manifest.permission.POST_NOTIFICATIONS) != PackageManager.PERMISSION_GRANTED) {
            requestPermissions(arrayOf(Manifest.permission.POST_NOTIFICATIONS), 1)
        }
    }

    private fun playInput() {
        val text = idInput.text.toString().trim()
        if (text.isEmpty()) return
        startActivity(Intent(this, PlayActivity::class.java).putExtra(PlayActivity.EXTRA_TEXT, text))
    }

    private fun checkLatest() {
        background.execute {
            runCatching { EngineReleases(this).latest() }
            onMain { render() }
        }
    }

    private fun render() {
        val s = EngineController.state
        state.text = when (s) {
            EngineState.Stopped -> getString(R.string.state_stopped)
            EngineState.Starting -> getString(R.string.state_starting)
            EngineState.Running -> getString(R.string.state_running)
            EngineState.Stopping -> getString(R.string.state_stopping)
            is EngineState.Crashed -> getString(R.string.state_crashed, s.reason)
        }
        val active = EngineFiles.activeVersion(this)
        val shown = EngineController.runningVersion ?: active
        version.text = shown?.let { getString(R.string.engine_version, it) } ?: getString(R.string.engine_none)
        val latest = prefs.latestVersion
        if (latest != null && (shown == null || Releases.compareVersions(latest, shown) > 0)) {
            update.visibility = View.VISIBLE
            update.text = getString(R.string.engine_update_available, latest)
        } else {
            update.visibility = View.GONE
        }
        startStop.text = getString(
            when {
                active == null -> R.string.action_download_engine
                s == EngineState.Running || s == EngineState.Starting || s == EngineState.Stopping -> R.string.action_stop
                else -> R.string.action_start
            },
        )
        startStop.isEnabled = s != EngineState.Stopping
        val settings = prefs.settings
        val host = if (settings.lan) Device.lanAddress() ?: StreamUrls.LOOPBACK else StreamUrls.LOOPBACK
        url.text = getString(R.string.player_url, StreamUrls.base(host, settings.port))
        url.visibility = if (s == EngineState.Running) View.VISIBLE else View.GONE

        recent.removeAllViews()
        val ids = prefs.recent
        recentTitle.visibility = if (ids.isEmpty()) View.GONE else View.VISIBLE
        clearRecent.visibility = recentTitle.visibility
        val titles = EngineController.streams.associate { it.contentId to it.title }
        for (id in ids) {
            val label = titles[id]?.let { "$it ($id)" } ?: id
            recent.addRow(label) { startActivity(Intent(this, PlayActivity::class.java).putExtra(PlayActivity.EXTRA_TEXT, id)) }
        }

        streams.removeAllViews()
        val list = EngineController.streams
        if (list.isEmpty()) {
            streams.addRow(getString(R.string.streams_none), null)
        } else {
            for (st in list) {
                val label = resources.getQuantityString(R.plurals.stream_row, st.clients, st.title ?: getString(R.string.untitled), st.clients)
                val id = st.contentId
                streams.addRow(label, id?.let { cid -> { startActivity(Intent(this, PlayActivity::class.java).putExtra(PlayActivity.EXTRA_TEXT, cid)) } })
            }
        }
    }
}
