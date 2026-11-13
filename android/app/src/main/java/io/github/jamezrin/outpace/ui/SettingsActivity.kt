package io.github.jamezrin.outpace.ui

import android.app.Activity
import android.os.Bundle
import android.view.View
import android.widget.Button
import android.widget.EditText
import android.widget.Switch
import io.github.jamezrin.outpace.Prefs
import io.github.jamezrin.outpace.R
import io.github.jamezrin.outpace.core.EngineSettings
import io.github.jamezrin.outpace.engine.EngineController
import io.github.jamezrin.outpace.engine.EngineState

class SettingsActivity : Activity() {
    private lateinit var prefs: Prefs

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setTitle(R.string.settings_title)
        setContentView(R.layout.activity_settings)
        prefs = Prefs(this)
        val s = prefs.settings
        sw(R.id.lan).isChecked = s.lan
        sw(R.id.seeding).isChecked = s.seeding
        sw(R.id.inbound).isChecked = s.inbound
        sw(R.id.port_mapping).isChecked = s.portMapping
        sw(R.id.ace_compat).isChecked = s.aceCompat
        sw(R.id.boot).isChecked = prefs.startOnBoot
        sw(R.id.idle).isChecked = prefs.idleStop
        val port = findViewById<EditText>(R.id.port)
        port.setText(s.port.toString())
        findViewById<Button>(R.id.battery).apply {
            visibility = if (BatteryPrompt.isExempt(this@SettingsActivity)) View.GONE else View.VISIBLE
            setOnClickListener { BatteryPrompt.request(this@SettingsActivity) }
        }
        val restart = findViewById<Button>(R.id.restart)
        restart.setOnClickListener { if (save()) EngineController.restart(this) }
        restart.visibility = if (EngineController.state == EngineState.Running) View.VISIBLE else View.GONE
        findViewById<Button>(R.id.save).setOnClickListener { if (save()) finish() }
    }

    private fun sw(id: Int): Switch = findViewById(id)

    private fun save(): Boolean {
        val portField = findViewById<EditText>(R.id.port)
        val port = portField.text.toString().toIntOrNull()
        if (port == null || !EngineSettings.validPort(port)) {
            portField.error = getString(R.string.settings_port_invalid)
            portField.requestFocus()
            return false
        }
        prefs.settings = EngineSettings(
            lan = sw(R.id.lan).isChecked,
            port = port,
            seeding = sw(R.id.seeding).isChecked,
            inbound = sw(R.id.inbound).isChecked,
            portMapping = sw(R.id.port_mapping).isChecked,
            aceCompat = sw(R.id.ace_compat).isChecked,
        )
        prefs.startOnBoot = sw(R.id.boot).isChecked
        prefs.idleStop = sw(R.id.idle).isChecked
        return true
    }
}
