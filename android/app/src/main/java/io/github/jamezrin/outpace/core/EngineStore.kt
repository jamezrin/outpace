package io.github.jamezrin.outpace.core

import java.io.File
import java.io.IOException

/** Installed engine versions under `<root>/engines/<version>/outpace`. */
class EngineStore(root: File) {
    private val engines = File(root, "engines")

    fun installed(): List<String> = (engines.listFiles() ?: emptyArray())
        .filter { it.isDirectory && !it.name.startsWith(".") && File(it, BINARY).isFile }
        .map { it.name }
        .sortedWith { a, b -> Releases.compareVersions(b, a) }

    fun binary(version: String): File = File(File(engines, version), BINARY)

    fun isInstalled(version: String) = binary(version).isFile

    /**
     * Verifies [archive] against [checksumText], extracts the engine and installs it as [version].
     * On any failure nothing is left under `engines/`, and an existing install is untouched.
     */
    fun install(version: String, archive: File, archiveName: String, checksumText: String): File {
        require(safeVersion(version)) { "invalid version" }
        val expected = Sha256.parse(checksumText, archiveName)
        val actual = Sha256.of(archive)
        if (actual != expected) throw IOException("checksum mismatch for $archiveName")
        engines.mkdirs()
        val staging = File(engines, ".staging-$version")
        staging.deleteRecursively()
        try {
            if (!staging.mkdirs()) throw IOException("cannot create $staging")
            val bin = File(staging, BINARY)
            if (!TarGz.extract(archive, BINARY, bin)) throw IOException("archive has no $BINARY")
            if (!bin.setExecutable(true, true)) throw IOException("cannot mark engine executable")
            val target = File(engines, version)
            target.deleteRecursively()
            if (!staging.renameTo(target)) throw IOException("cannot move engine into place")
            return File(target, BINARY)
        } finally {
            staging.deleteRecursively()
        }
    }

    fun remove(version: String) {
        require(safeVersion(version)) { "invalid version" }
        File(engines, version).deleteRecursively()
    }

    companion object {
        const val BINARY = "outpace"
        const val MAX_INSTALLED = 3

        fun safeVersion(v: String) = v.isNotEmpty() && v.length <= 64 && v.first().isDigit() &&
            v.all { it.isLetterOrDigit() || it == '.' || it == '-' }
    }
}
