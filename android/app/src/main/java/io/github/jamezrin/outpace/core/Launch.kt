package io.github.jamezrin.outpace.core

import java.io.File

object Launch {
    /** Android 10 blocks exec() of app files, and is also the first release whose loader runs programs. */
    const val LOADER_MIN_API = 29

    fun elfIs64(file: File): Boolean {
        val header = ByteArray(5)
        val n = file.inputStream().use { it.read(header) }
        require(n == 5 && header[0] == 0x7f.toByte() && header[1] == 'E'.code.toByte()) { "not an ELF file" }
        return header[4].toInt() == 2
    }

    fun command(api: Int, is64: Boolean, binary: String, args: List<String> = listOf("serve")): List<String> =
        if (api >= LOADER_MIN_API) {
            listOf(if (is64) "/system/bin/linker64" else "/system/bin/linker", binary) + args
        } else {
            listOf(binary) + args
        }
}
