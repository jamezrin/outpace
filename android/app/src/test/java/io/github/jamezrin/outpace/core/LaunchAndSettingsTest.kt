package io.github.jamezrin.outpace.core

import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNull
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder

class LaunchAndSettingsTest {
    @get:Rule val tmp = TemporaryFolder()

    @Test fun api29UsesLinker64() =
        assertEquals(listOf("/system/bin/linker64", "/e/outpace", "serve"), Launch.command(29, true, "/e/outpace"))

    @Test fun api29Uses32BitLinker() =
        assertEquals(listOf("/system/bin/linker", "/e/outpace", "serve"), Launch.command(35, false, "/e/outpace"))

    @Test fun api28ExecsDirectly() = assertEquals(listOf("/e/outpace", "serve"), Launch.command(28, true, "/e/outpace"))

    @Test fun elfClassFromHeader() {
        val f = tmp.newFile("e32")
        f.writeBytes(byteArrayOf(0x7f, 'E'.code.toByte(), 'L'.code.toByte(), 'F'.code.toByte(), 1, 1))
        assertFalse(Launch.elfIs64(f))
    }

    @Test(expected = IllegalArgumentException::class)
    fun rejectsNonElf() {
        val f = tmp.newFile("txt")
        f.writeText("#!/bin/sh\n")
        Launch.elfIs64(f)
    }

    @Test fun envLoopbackByDefault() {
        val env = EngineSettings().env("/d", "/c")
        assertEquals("127.0.0.1:6878", env["OUTPACE_BIND"])
        assertEquals("/d", env["OUTPACE_DATA_DIR"])
        assertEquals("/c", env["OUTPACE_CACHE_DIR"])
    }

    @Test fun envLanBind() = assertEquals("0.0.0.0:7000", EngineSettings(lan = true, port = 7000).env("/d", "/c")["OUTPACE_BIND"])

    @Test fun envFlags() {
        val env = EngineSettings(seeding = false, inbound = false, portMapping = true, aceCompat = true).env("/d", "/c")
        assertEquals("false", env["OUTPACE_ENABLE_SEEDING"])
        assertEquals("false", env["OUTPACE_ENABLE_INBOUND"])
        assertEquals("true", env["OUTPACE_ENABLE_PORT_MAPPING"])
        assertEquals("true", env["OUTPACE_ACE_COMPAT"])
    }

    @Test fun ringDropsOldest() {
        val ring = LogRing(3)
        for (i in 1..5) ring.add("line $i")
        assertEquals(listOf("line 3", "line 4", "line 5"), ring.snapshot())
    }

    @Test fun ringTruncatesLongLines() {
        val ring = LogRing(1)
        ring.add("x".repeat(5000))
        assertEquals(LogRing.MAX_LINE, ring.snapshot().single().length)
    }

    @Test fun parsesStreamsPayload() {
        val id = "0123456789abcdef0123456789abcdef01234567"
        val json = """{"streams":[
            {"network":"ace","id":"cid:$id","clients":2,"metadata":{"title":"News","bitrate":4000000,"categories":[]}},
            {"network":"ace","id":"ih:$id","clients":0,"metadata":{"title":null,"bitrate":null,"categories":[]}}]}"""
        val s = StreamsJson.parse(json)
        assertEquals(StreamInfo("cid:$id", "News", 4_000_000, 2), s[0])
        assertEquals(id, s[0].contentId)
        assertNull(s[1].title)
        assertNull(s[1].bitrate)
        assertNull(s[1].contentId)
    }

    @Test fun parsesEmptyStreams() = assertEquals(emptyList<StreamInfo>(), StreamsJson.parse("""{"streams":[]}"""))
}
