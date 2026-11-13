package io.github.jamezrin.outpace.core

import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test

class ReleasesTest {
    private fun release(tag: String, vararg assets: String, draft: Boolean = false, pre: Boolean = false) = """
        {"tag_name":"$tag","draft":$draft,"prerelease":$pre,"published_at":"2026-11-13T10:08:27Z",
         "assets":[${assets.joinToString(",") { """{"name":"$it","browser_download_url":"https://example.invalid/$it","size":1}""" }}]}
    """.trimIndent()

    private val payload = "[" + listOf(
        release("v0.9.0", "outpace-0.9.0-x86_64-unknown-linux-musl.tar.gz"),
        release("v1.0.0", "outpace-1.0.0-aarch64-linux-android.tar.gz", "outpace-1.0.0-aarch64-linux-android.tar.gz.sha256"),
        release("v1.1.0", "x", draft = true),
        release("v1.1.0-rc.1", pre = true),
    ).joinToString(",") + "]"

    @Test fun parsesGithubPayload() {
        val list = Releases.parseList(payload)
        assertEquals(listOf("1.1.0-rc.1", "1.0.0", "0.9.0"), list.map { it.version })
        assertTrue(list[0].prerelease)
        assertEquals("2026-11-13T10:08:27Z", list[1].publishedAt)
    }

    @Test fun dropsDrafts() = assertTrue(Releases.parseList(payload).none { it.version == "1.1.0" })

    @Test fun versionStripsV() = assertEquals("1.0.0", Releases.versionOf("v1.0.0"))

    @Test fun comparesNumerically() = assertTrue(Releases.compareVersions("1.10.0", "1.9.0") > 0)

    @Test fun prereleaseSortsLower() {
        assertTrue(Releases.compareVersions("1.1.0-rc.1", "1.1.0") < 0)
        assertTrue(Releases.compareVersions("1.1.0-rc.1", "1.0.0") > 0)
        assertEquals(0, Releases.compareVersions("v1.0.0", "1.0.0"))
    }

    @Test fun picksFirstTargetWithAsset() {
        val r = Releases.parseList(payload).first { it.version == "1.0.0" }
        assertEquals("aarch64-linux-android", Releases.pickTarget(r, listOf("aarch64-linux-android", "armv7-linux-androideabi")))
    }

    @Test fun fallsBackToNextAbi() {
        val r = Release("v1.2.0", "1.2.0", "", false, setOf(
            "outpace-1.2.0-armv7-linux-androideabi.tar.gz", "outpace-1.2.0-armv7-linux-androideabi.tar.gz.sha256",
        ))
        assertEquals("armv7-linux-androideabi", Releases.pickTarget(r, Abi.targetsFor(listOf("arm64-v8a", "armeabi-v7a", "armeabi"))))
    }

    @Test fun nullWhenNoAsset() =
        assertNull(Releases.pickTarget(Releases.parseList(payload).last(), listOf("aarch64-linux-android")))

    @Test fun nullWithoutChecksum() {
        val r = Release("v1.2.0", "1.2.0", "", false, setOf("outpace-1.2.0-aarch64-linux-android.tar.gz"))
        assertNull(Releases.pickTarget(r, listOf("aarch64-linux-android")))
    }

    @Test fun latestRedirect() {
        assertEquals("1.0.0", Releases.versionFromLatestRedirect("https://github.com/o/r/releases/tag/v1.0.0"))
        assertNull(Releases.versionFromLatestRedirect("https://github.com/o/r/releases"))
        assertNull(Releases.versionFromLatestRedirect(null))
    }

    @Test fun downloadUrlUsesTag() = assertEquals(
        "https://github.com/jamezrin/outpace-stream/releases/download/v1.0.0/outpace-1.0.0-aarch64-linux-android.tar.gz",
        Releases.downloadUrl("1.0.0", Releases.archiveName("1.0.0", "aarch64-linux-android")),
    )

    @Test fun dropsTranslatedAbis() = assertEquals(
        listOf("x86_64-linux-android", "i686-linux-android"),
        Abi.targetsFor(listOf("x86_64", "arm64-v8a", "x86", "armeabi-v7a")),
    )

    @Test fun keepsArm32OnArm64() = assertEquals(
        listOf("aarch64-linux-android", "armv7-linux-androideabi"),
        Abi.targetsFor(listOf("arm64-v8a", "armeabi-v7a", "armeabi")),
    )

    @Test fun abiMapping() =
        assertEquals(listOf("armv7-linux-androideabi"), Abi.targetsFor(listOf("armeabi-v7a", "armeabi", "mips")))
}
