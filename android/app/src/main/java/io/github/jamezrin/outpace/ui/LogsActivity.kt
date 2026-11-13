package io.github.jamezrin.outpace.ui

import android.app.Activity
import android.content.Intent
import android.os.Bundle
import android.widget.Button
import android.widget.ScrollView
import android.widget.TextView
import io.github.jamezrin.outpace.R
import io.github.jamezrin.outpace.engine.EngineController

class LogsActivity : Activity() {
    private lateinit var text: TextView
    private lateinit var scroll: ScrollView
    private var shown = -1
    private val tick = object : Runnable {
        override fun run() {
            render()
            mainHandler.postDelayed(this, REFRESH_MS)
        }
    }

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setTitle(R.string.logs_title)
        setContentView(R.layout.activity_logs)
        text = findViewById(R.id.text)
        scroll = findViewById(R.id.scroll)
        findViewById<Button>(R.id.copy).setOnClickListener {
            copyText("outpace logs", logText())
            toast(getString(R.string.play_copied))
        }
        findViewById<Button>(R.id.share).setOnClickListener {
            val send = Intent(Intent.ACTION_SEND).setType("text/plain").putExtra(Intent.EXTRA_TEXT, logText())
            startActivity(Intent.createChooser(send, getString(R.string.logs_share)))
        }
    }

    override fun onStart() {
        super.onStart()
        mainHandler.post(tick)
    }

    override fun onStop() {
        mainHandler.removeCallbacks(tick)
        super.onStop()
    }

    private fun logText() = EngineController.log.snapshot().joinToString("\n")

    private fun render() {
        val lines = EngineController.log.snapshot()
        val signature = lines.size * 31 + (lines.lastOrNull()?.hashCode() ?: 0)
        if (signature == shown) return
        shown = signature
        text.text = if (lines.isEmpty()) getString(R.string.logs_empty) else lines.joinToString("\n")
        scroll.post { scroll.fullScroll(ScrollView.FOCUS_DOWN) }
    }

    private companion object {
        const val REFRESH_MS = 1_000L
    }
}
