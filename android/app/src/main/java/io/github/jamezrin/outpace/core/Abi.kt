package io.github.jamezrin.outpace.core

/** Maps Android ABIs to the Rust targets release archives are named after. */
object Abi {
    private val targets = mapOf(
        "arm64-v8a" to "aarch64-linux-android",
        "armeabi-v7a" to "armv7-linux-androideabi",
        "x86_64" to "x86_64-linux-android",
        "x86" to "i686-linux-android",
    )

    fun targetFor(abi: String): String? = targets[abi]

    private fun family(abi: String) = if (abi.startsWith("x86")) "x86" else if (abi.startsWith("arm")) "arm" else abi

    /**
     * Targets this device can run, most preferred first (from `Build.SUPPORTED_ABIS`). ABIs from
     * another CPU family are binary translation for apps; the system loader cannot run those
     * executables, so they are dropped.
     */
    fun targetsFor(abis: List<String>): List<String> {
        val native = abis.firstOrNull()?.let(::family) ?: return emptyList()
        return abis.filter { family(it) == native }.mapNotNull(::targetFor).distinct()
    }
}
