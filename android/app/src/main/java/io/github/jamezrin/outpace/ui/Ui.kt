package io.github.jamezrin.outpace.ui

import android.app.Activity
import android.content.ClipData
import android.content.ClipboardManager
import android.content.Context
import android.os.Handler
import android.os.Looper
import android.widget.Button
import android.widget.LinearLayout
import android.widget.Toast
import java.util.concurrent.Executors

/** One shared background thread for screen work (network, disk). */
internal val background = Executors.newSingleThreadExecutor { r -> Thread(r, "ui-background") }
internal val mainHandler = Handler(Looper.getMainLooper())

internal fun Activity.onMain(block: () -> Unit) = runOnUiThread { if (!isFinishing) block() }

internal fun Context.toast(text: CharSequence) = Toast.makeText(this, text, Toast.LENGTH_LONG).show()

internal fun Context.copyText(label: String, text: String) {
    val cm = getSystemService(Context.CLIPBOARD_SERVICE) as ClipboardManager
    cm.setPrimaryClip(ClipData.newPlainText(label, text))
}

/** A full-width focusable row; plain buttons keep D-pad navigation predictable. */
internal fun LinearLayout.addRow(text: CharSequence, onClick: (() -> Unit)?): Button {
    val b = Button(context)
    b.text = text
    b.isAllCaps = false
    b.textAlignment = Button.TEXT_ALIGNMENT_VIEW_START
    b.isEnabled = onClick != null
    onClick?.let { action -> b.setOnClickListener { action() } }
    addView(b, LinearLayout.LayoutParams(LinearLayout.LayoutParams.MATCH_PARENT, LinearLayout.LayoutParams.WRAP_CONTENT))
    return b
}
