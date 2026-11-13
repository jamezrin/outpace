package io.github.jamezrin.outpace.core

/** The last [capacity] engine log lines. Thread-safe; old lines are dropped, never buffered. */
class LogRing(private val capacity: Int) {
    private val lines = ArrayDeque<String>(capacity)

    @Synchronized
    fun add(line: String) {
        if (lines.size == capacity) lines.removeFirst()
        lines.addLast(if (line.length > MAX_LINE) line.substring(0, MAX_LINE) else line)
    }

    @Synchronized
    fun snapshot(): List<String> = lines.toList()

    @Synchronized
    fun clear() = lines.clear()

    companion object {
        const val MAX_LINE = 2000
    }
}
