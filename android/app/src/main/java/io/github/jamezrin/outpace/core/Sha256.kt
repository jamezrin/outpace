package io.github.jamezrin.outpace.core

import java.io.File
import java.security.MessageDigest

object Sha256 {
    private val line = Regex("^([0-9a-fA-F]{64}) [ *](.+)$")

    /** Reads a `sha256sum` line and returns the hash, checking it names [expectedName]. */
    fun parse(text: String, expectedName: String): String {
        val entry = text.lineSequence().map { it.trim() }.firstOrNull { it.isNotEmpty() }
            ?: throw IllegalArgumentException("empty checksum file")
        val m = line.matchEntire(entry) ?: throw IllegalArgumentException("malformed checksum line")
        val name = m.groupValues[2].trim().substringAfterLast('/')
        require(name == expectedName) { "checksum is for $name, expected $expectedName" }
        return m.groupValues[1].lowercase()
    }

    fun of(file: File): String {
        val digest = MessageDigest.getInstance("SHA-256")
        file.inputStream().use { input ->
            val buf = ByteArray(64 * 1024)
            while (true) {
                val n = input.read(buf)
                if (n < 0) break
                digest.update(buf, 0, n)
            }
        }
        return digest.digest().joinToString("") { "%02x".format(it) }
    }
}
