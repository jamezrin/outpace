package io.github.jamezrin.outpace.core

import java.io.EOFException
import java.io.File
import java.io.InputStream
import java.util.zip.GZIPInputStream

/** Just enough tar to pull one regular file out of a release archive (ustar, GNU and pax headers). */
object TarGz {
    private const val BLOCK = 512

    /**
     * Copies the first regular file whose path is [fileName] or ends in `/[fileName]` to [dest].
     * Returns false when the archive has no such entry.
     */
    fun extract(archive: File, fileName: String, dest: File): Boolean =
        GZIPInputStream(archive.inputStream().buffered(), 64 * 1024).use { extract(it, fileName, dest) }

    fun extract(input: InputStream, fileName: String, dest: File): Boolean {
        val header = ByteArray(BLOCK)
        var nextName: String? = null
        var zeroBlocks = 0
        while (true) {
            if (!readBlock(input, header)) return false
            if (header.all { it.toInt() == 0 }) {
                if (++zeroBlocks == 2) return false
                continue
            }
            zeroBlocks = 0
            val size = parseSize(header)
            val type = header[156].toInt().toChar()
            when (type) {
                'L' -> nextName = cString(readBytes(input, size))
                'x' -> nextName = paxPath(readBytes(input, size)) ?: nextName
                'g' -> skip(input, padded(size))
                else -> {
                    val name = (nextName ?: headerName(header)).removePrefix("./")
                    nextName = null
                    if ((type == '0' || type == '\u0000') && (name == fileName || name.endsWith("/$fileName"))) {
                        dest.outputStream().use { out -> copy(input, out, size) }
                        return true
                    }
                    skip(input, padded(size))
                }
            }
        }
    }

    private fun headerName(h: ByteArray): String {
        val name = cString(h.copyOfRange(0, 100))
        val ustar = String(h, 257, 5, Charsets.US_ASCII) == "ustar"
        val prefix = if (ustar) cString(h.copyOfRange(345, 500)) else ""
        return if (prefix.isEmpty()) name else "$prefix/$name"
    }

    private fun parseSize(h: ByteArray): Long {
        if (h[124].toInt() and 0x80 != 0) {
            var v = 0L
            for (i in 125 until 136) v = (v shl 8) or (h[i].toLong() and 0xff)
            return v
        }
        val s = String(h, 124, 12, Charsets.US_ASCII).trim { it == ' ' || it == '\u0000' }
        return if (s.isEmpty()) 0 else s.toLong(8)
    }

    private fun paxPath(data: ByteArray): String? {
        var i = 0
        while (i < data.size) {
            val space = (i until data.size).firstOrNull { data[it] == ' '.code.toByte() } ?: return null
            val len = String(data, i, space - i, Charsets.US_ASCII).toIntOrNull() ?: return null
            if (len <= 0 || i + len > data.size) return null
            val record = String(data, space + 1, i + len - space - 2, Charsets.UTF_8)
            if (record.startsWith("path=")) return record.removePrefix("path=")
            i += len
        }
        return null
    }

    private fun cString(b: ByteArray): String {
        val end = b.indexOf(0).let { if (it < 0) b.size else it }
        return String(b, 0, end, Charsets.UTF_8)
    }

    private fun padded(size: Long) = (size + BLOCK - 1) / BLOCK * BLOCK

    private fun readBytes(input: InputStream, size: Long): ByteArray {
        require(size in 0..1_048_576) { "oversized tar metadata entry" }
        val out = ByteArray(size.toInt())
        readFully(input, out, out.size)
        skip(input, padded(size) - size)
        return out
    }

    private fun copy(input: InputStream, out: java.io.OutputStream, size: Long) {
        val buf = ByteArray(64 * 1024)
        var left = size
        while (left > 0) {
            val n = input.read(buf, 0, minOf(buf.size.toLong(), left).toInt())
            if (n < 0) throw EOFException("truncated tar entry")
            out.write(buf, 0, n)
            left -= n
        }
    }

    private fun readBlock(input: InputStream, block: ByteArray): Boolean {
        var off = 0
        while (off < block.size) {
            val n = input.read(block, off, block.size - off)
            if (n < 0) {
                if (off == 0) return false
                throw EOFException("truncated tar header")
            }
            off += n
        }
        return true
    }

    private fun readFully(input: InputStream, b: ByteArray, len: Int) {
        var off = 0
        while (off < len) {
            val n = input.read(b, off, len - off)
            if (n < 0) throw EOFException("truncated tar entry")
            off += n
        }
    }

    private fun skip(input: InputStream, count: Long) {
        var left = count
        val buf = ByteArray(BLOCK)
        while (left > 0) {
            val n = input.read(buf, 0, minOf(buf.size.toLong(), left).toInt())
            if (n < 0) throw EOFException("truncated tar archive")
            left -= n
        }
    }
}
