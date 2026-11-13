package io.github.jamezrin.outpace.engine

import android.content.Context
import android.content.Intent
import android.os.Build
import android.os.Handler
import android.os.Looper
import io.github.jamezrin.outpace.core.EngineSettings
import io.github.jamezrin.outpace.core.LogRing
import io.github.jamezrin.outpace.core.StreamInfo
import java.util.concurrent.CopyOnWriteArraySet

sealed interface EngineState {
    data object Stopped : EngineState
    data object Starting : EngineState
    data object Running : EngineState
    data object Stopping : EngineState
    data class Crashed(val reason: String) : EngineState
}

/** Process-wide view of the engine, shared by the service and the screens. */
object EngineController {
    const val LOG_LINES = 500

    val log = LogRing(LOG_LINES)

    @Volatile var state: EngineState = EngineState.Stopped
        private set

    @Volatile var streams: List<StreamInfo> = emptyList()
        private set

    @Volatile var runningVersion: String? = null
        private set

    /** The settings the running engine started with; saved settings apply only after a restart. */
    @Volatile var runningSettings: EngineSettings? = null
        private set

    private val main = Handler(Looper.getMainLooper())
    private val listeners = CopyOnWriteArraySet<() -> Unit>()

    /** Screens currently showing engine status; while > 0 the service polls quickly. */
    @Volatile var visibleScreens = 0
        private set

    @Volatile internal var pollNow: (() -> Unit)? = null

    fun addListener(l: () -> Unit) {
        listeners += l
    }

    fun removeListener(l: () -> Unit) {
        listeners -= l
    }

    fun screenShown() {
        visibleScreens++
        pollNow?.invoke()
    }

    fun screenHidden() {
        visibleScreens = maxOf(0, visibleScreens - 1)
    }

    internal fun update(
        state: EngineState? = null,
        streams: List<StreamInfo>? = null,
        version: String? = null,
        settings: EngineSettings? = null,
        clearVersion: Boolean = false,
    ) {
        if (state != null) this.state = state
        if (streams != null) this.streams = streams
        if (version != null) runningVersion = version
        if (settings != null) runningSettings = settings
        if (clearVersion) {
            runningVersion = null
            runningSettings = null
        }
        notifyListeners()
    }

    /** Settings that describe where the engine is reachable now: the running ones, else the saved ones. */
    fun effectiveSettings(saved: EngineSettings): EngineSettings = runningSettings ?: saved

    internal fun notifyListeners() {
        main.post { listeners.forEach { it() } }
    }

    fun start(context: Context) {
        // Mark the start now, so a waiter never mistakes an earlier crash for this attempt's result.
        val s = state
        if (s != EngineState.Running && s != EngineState.Starting) update(EngineState.Starting)
        send(context, EngineService.ACTION_START)
    }

    fun restart(context: Context) = send(context, EngineService.ACTION_RESTART)

    fun stop(context: Context) {
        if (state == EngineState.Stopped) return
        send(context, EngineService.ACTION_STOP)
    }

    /** Blocks (background threads only) until the engine runs or fails; true when running. */
    fun awaitRunning(timeoutMs: Long): Boolean {
        val deadline = System.currentTimeMillis() + timeoutMs
        while (System.currentTimeMillis() < deadline) {
            when (state) {
                EngineState.Running -> return true
                is EngineState.Crashed -> return false
                else -> Thread.sleep(100)
            }
        }
        return state == EngineState.Running
    }

    private fun send(context: Context, action: String) {
        val intent = Intent(context, EngineService::class.java).setAction(action)
        if (action != EngineService.ACTION_STOP && Build.VERSION.SDK_INT >= 26) {
            context.startForegroundService(intent)
        } else {
            context.startService(intent)
        }
    }
}
