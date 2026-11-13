package io.github.jamezrin.outpace

import android.content.BroadcastReceiver
import android.content.Context
import android.content.Intent
import io.github.jamezrin.outpace.engine.EngineController

class BootReceiver : BroadcastReceiver() {
    override fun onReceive(context: Context, intent: Intent) {
        if (intent.action != Intent.ACTION_BOOT_COMPLETED) return
        if (!Prefs(context).startOnBoot || EngineFiles.activeVersion(context) == null) return
        EngineController.start(context)
    }
}
