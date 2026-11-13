package io.github.jamezrin.outpace.net

import io.github.jamezrin.outpace.BuildConfig
import java.io.File
import java.io.IOException
import java.io.InterruptedIOException
import java.net.HttpURLConnection
import java.net.URL

class HttpStatusException(val code: Int, url: String) : IOException("HTTP $code for $url")

/** Blocking HTTP helpers. Call from a background thread only. */
object Http {
    private const val CONNECT_TIMEOUT = 10_000
    private const val READ_TIMEOUT = 20_000
    private const val MAX_TEXT = 4 * 1024 * 1024

    private fun open(url: String, readTimeout: Int = READ_TIMEOUT): HttpURLConnection =
        (URL(url).openConnection() as HttpURLConnection).apply {
            connectTimeout = CONNECT_TIMEOUT
            this.readTimeout = readTimeout
            setRequestProperty("User-Agent", "outpace-android/${BuildConfig.VERSION_NAME}")
        }

    fun get(url: String, accept: String? = null, readTimeout: Int = READ_TIMEOUT): String {
        val c = open(url, readTimeout)
        try {
            if (accept != null) c.setRequestProperty("Accept", accept)
            if (c.responseCode !in 200..299) throw HttpStatusException(c.responseCode, url)
            val bytes = c.inputStream.use { it.readBytesBounded(MAX_TEXT) }
            return String(bytes, Charsets.UTF_8)
        } finally {
            c.disconnect()
        }
    }

    /** The `Location` of a redirect, without following it. */
    fun redirectLocation(url: String): String? {
        val c = open(url)
        try {
            c.instanceFollowRedirects = false
            c.requestMethod = "HEAD"
            val code = c.responseCode
            if (code !in 300..399) throw HttpStatusException(code, url)
            return c.getHeaderField("Location")
        } finally {
            c.disconnect()
        }
    }

    /** Streams [url] to [dest], reporting (done, total) bytes; total is -1 when unknown. */
    fun download(url: String, dest: File, cancelled: () -> Boolean, progress: (Long, Long) -> Unit) {
        val c = open(url)
        try {
            if (c.responseCode !in 200..299) throw HttpStatusException(c.responseCode, url)
            val total = c.getHeaderField("Content-Length")?.toLongOrNull() ?: -1L
            c.inputStream.use { input ->
                dest.outputStream().use { out ->
                    val buf = ByteArray(64 * 1024)
                    var done = 0L
                    var lastReport = 0L
                    while (true) {
                        if (cancelled()) throw InterruptedIOException("cancelled")
                        val n = input.read(buf)
                        if (n < 0) break
                        out.write(buf, 0, n)
                        done += n
                        if (done - lastReport >= 256 * 1024) {
                            progress(done, total)
                            lastReport = done
                        }
                    }
                    progress(done, total)
                    if (total >= 0 && done != total) throw IOException("download truncated")
                }
            }
        } finally {
            c.disconnect()
        }
    }

    private fun java.io.InputStream.readBytesBounded(limit: Int): ByteArray {
        val out = java.io.ByteArrayOutputStream()
        val buf = ByteArray(16 * 1024)
        while (true) {
            val n = read(buf)
            if (n < 0) break
            out.write(buf, 0, n)
            if (out.size() > limit) throw IOException("response too large")
        }
        return out.toByteArray()
    }
}
