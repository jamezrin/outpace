package io.github.jamezrin.outpace.core

import org.junit.Assert.assertTrue
import org.junit.Assume.assumeTrue
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import java.io.File

/**
 * Installs a real release archive. Skipped unless OUTPACE_ANDROID_REAL_ARCHIVE points at a
 * downloaded `outpace-<v>-<target>.tar.gz` with its `.sha256` next to it.
 */
class RealArchiveTest {
    @get:Rule val tmp = TemporaryFolder()

    @Test fun installsRealReleaseArchive() {
        val path = System.getenv("OUTPACE_ANDROID_REAL_ARCHIVE")
        assumeTrue("OUTPACE_ANDROID_REAL_ARCHIVE not set", path != null)
        val archive = File(path!!)
        val version = archive.name.removePrefix("outpace-").substringBefore('-')
        val bin = EngineStore(tmp.root).install(version, archive, archive.name, File("$path.sha256").readText())
        assertTrue(bin.length() > 1_000_000)
        assertTrue(Launch.elfIs64(bin))
    }
}
