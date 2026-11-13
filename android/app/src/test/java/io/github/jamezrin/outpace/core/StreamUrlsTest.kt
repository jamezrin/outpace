package io.github.jamezrin.outpace.core

import org.junit.Assert.assertEquals
import org.junit.Test

class StreamUrlsTest {
    private val id = "0123456789abcdef0123456789abcdef01234567"

    @Test fun liveUrl() = assertEquals("http://127.0.0.1:6878/streams/ace/cid:$id", StreamUrls.live("127.0.0.1", 6878, id))

    @Test fun hlsUrl() = assertEquals("http://192.168.1.5:7000/streams/ace/cid:$id.m3u8", StreamUrls.hls("192.168.1.5", 7000, id))

    @Test fun healthUsesLoopback() = assertEquals("http://127.0.0.1:6878/healthz", StreamUrls.health(6878))
}
