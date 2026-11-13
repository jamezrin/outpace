package io.github.jamezrin.outpace.core

/** URLs a player uses to reach a stream on the local engine. */
object StreamUrls {
    const val LOOPBACK = "127.0.0.1"

    fun base(host: String, port: Int): String = "http://$host:$port"

    fun live(host: String, port: Int, id: String): String = "${base(host, port)}/streams/ace/cid:$id"

    fun hls(host: String, port: Int, id: String): String = live(host, port, id) + ".m3u8"

    fun health(port: Int): String = "${base(LOOPBACK, port)}/healthz"

    fun streams(port: Int): String = "${base(LOOPBACK, port)}/streams"
}
