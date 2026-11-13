package io.github.jamezrin.outpace.core

import org.junit.Assert.assertArrayEquals
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import java.io.File
import java.io.IOException

class ArchiveTest {
    @get:Rule val tmp = TemporaryFolder()

    private val engine = byteArrayOf(0x7f, 'E'.code.toByte(), 'L'.code.toByte(), 'F'.code.toByte(), 2) + ByteArray(1500) { it.toByte() }
    private val archiveName = "outpace-1.0.0-aarch64-linux-android.tar.gz"

    /** Same layout as the release workflow: `outpace-<v>-<target>/{outpace,LICENSE,README.md}`. */
    private fun releaseArchive(): File = TarFixture()
        .file("outpace-1.0.0-aarch64-linux-android/", ByteArray(0), '5')
        .file("outpace-1.0.0-aarch64-linux-android/LICENSE", "license".toByteArray())
        .file("outpace-1.0.0-aarch64-linux-android/outpace", engine)
        .file("outpace-1.0.0-aarch64-linux-android/README.md", "readme".toByteArray())
        .write(tmp.newFile(archiveName))

    private fun checksumFor(f: File) = "${Sha256.of(f)}  ${f.name}\n"

    @Test fun parsesSha256sumLine() {
        val hash = "a".repeat(64)
        assertEquals(hash, Sha256.parse("$hash  $archiveName\n", archiveName))
        assertEquals(hash, Sha256.parse("${hash.uppercase()} *$archiveName", archiveName))
    }

    @Test(expected = IllegalArgumentException::class)
    fun rejectsMismatchedName() { Sha256.parse("${"a".repeat(64)}  other.tar.gz", archiveName) }

    @Test(expected = IllegalArgumentException::class)
    fun rejectsBadHash() { Sha256.parse("${"a".repeat(63)}  $archiveName", archiveName) }

    @Test fun extractsOutpaceEntry() {
        val dest = File(tmp.root, "out")
        assertTrue(TarGz.extract(releaseArchive(), "outpace", dest))
        assertArrayEquals(engine, dest.readBytes())
    }

    @Test fun missingEntryReturnsFalse() {
        val a = TarFixture().file("x/README.md", "r".toByteArray()).write(tmp.newFile("a.tar.gz"))
        assertFalse(TarGz.extract(a, "outpace", File(tmp.root, "out")))
    }

    @Test fun skipsPaxAndLongNameHeaders() {
        val long = "outpace-1.0.0-" + "x".repeat(120) + "/outpace"
        val a = TarFixture().longName(long, engine).write(tmp.newFile("long.tar.gz"))
        assertTrue(TarGz.extract(a, "outpace", File(tmp.root, "o1")))
        assertArrayEquals(engine, File(tmp.root, "o1").readBytes())
        val b = TarFixture().paxPath("dir/outpace", engine).write(tmp.newFile("pax.tar.gz"))
        assertTrue(TarGz.extract(b, "outpace", File(tmp.root, "o2")))
        assertArrayEquals(engine, File(tmp.root, "o2").readBytes())
    }

    @Test fun installSetsExecutable() {
        val store = EngineStore(tmp.newFolder("files"))
        val a = releaseArchive()
        val bin = store.install("1.0.0", a, archiveName, checksumFor(a))
        assertTrue(bin.canExecute())
        assertEquals(listOf("1.0.0"), store.installed())
        assertTrue(Launch.elfIs64(bin))
    }

    @Test fun installVerifiesChecksum() {
        val root = tmp.newFolder("files")
        val store = EngineStore(root)
        val a = releaseArchive()
        try {
            store.install("1.0.0", a, archiveName, "${"0".repeat(64)}  $archiveName")
            throw AssertionError("expected a checksum failure")
        } catch (_: IOException) {
        }
        assertEquals(emptyList<String>(), store.installed())
        assertFalse(File(root, "engines/1.0.0").exists())
        assertEquals(0, File(root, "engines").listFiles()?.size ?: 0)
    }

    @Test fun failedExtractKeepsExistingInstall() {
        val store = EngineStore(tmp.newFolder("files"))
        val good = releaseArchive()
        store.install("1.0.0", good, archiveName, checksumFor(good))
        val bad = TarFixture().file("x/README.md", "r".toByteArray()).write(File(tmp.root, "bad.tar.gz"))
        try {
            store.install("1.0.0", bad, "bad.tar.gz", checksumFor(bad))
            throw AssertionError("expected a missing-entry failure")
        } catch (_: IOException) {
        }
        assertTrue(store.isInstalled("1.0.0"))
    }

    @Test fun listsInstalledSortedNewestFirst() {
        val root = tmp.newFolder("files")
        for (v in listOf("0.9.0", "1.10.0", "1.2.0")) File(root, "engines/$v").apply { mkdirs() }.resolve("outpace").writeText("x")
        File(root, "engines/.staging-2.0.0").mkdirs()
        assertEquals(listOf("1.10.0", "1.2.0", "0.9.0"), EngineStore(root).installed())
    }

    @Test fun rejectsUnsafeVersion() {
        assertFalse(EngineStore.safeVersion("../x"))
        assertFalse(EngineStore.safeVersion(""))
        assertTrue(EngineStore.safeVersion("1.0.0-rc.1"))
    }
}
