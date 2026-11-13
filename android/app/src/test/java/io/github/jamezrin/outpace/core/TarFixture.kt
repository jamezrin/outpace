package io.github.jamezrin.outpace.core

import java.io.ByteArrayOutputStream
import java.io.File
import java.util.zip.GZIPOutputStream

/** Writes small tar.gz archives with ustar, GNU long-name and pax headers for tests. */
class TarFixture {
    private val out = ByteArrayOutputStream()

    fun file(name: String, data: ByteArray, type: Char = '0') = apply { entry(name, data, type) }

    fun longName(name: String, data: ByteArray) = apply {
        entry("././@LongLink", name.toByteArray() + 0, 'L')
        entry(name.take(99), data, '0')
    }

    fun paxPath(name: String, data: ByteArray) = apply {
        val body = "path=$name\n"
        var len = body.length + 3
        while ("$len $body".length != len) len = "$len $body".length
        entry("PaxHeaders/x", "$len $body".toByteArray(), 'x')
        entry("short-name", data, '0')
    }

    fun write(dest: File): File {
        out.write(ByteArray(1024))
        GZIPOutputStream(dest.outputStream()).use { it.write(out.toByteArray()) }
        return dest
    }

    private fun entry(name: String, data: ByteArray, type: Char) {
        val h = ByteArray(512)
        name.toByteArray().copyInto(h, 0, 0, minOf(100, name.length))
        "0000755".toByteArray().copyInto(h, 100)
        String.format("%011o", data.size).toByteArray().copyInto(h, 124)
        h[156] = type.code.toByte()
        "ustar ".toByteArray().copyInto(h, 257)
        " ".toByteArray().copyInto(h, 263)
        for (i in 148 until 156) h[i] = ' '.code.toByte()
        val sum = h.sumOf { it.toInt() and 0xff }
        String.format("%06o", sum).toByteArray().copyInto(h, 148)
        h[154] = 0
        out.write(h)
        out.write(data)
        val pad = (512 - data.size % 512) % 512
        out.write(ByteArray(pad))
    }
}
