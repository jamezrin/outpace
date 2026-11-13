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

    /** Targets this device can run, most preferred first (from `Build.SUPPORTED_ABIS`). */
    fun targetsFor(abis: List<String>): List<String> = abis.mapNotNull(::targetFor).distinct()
}
