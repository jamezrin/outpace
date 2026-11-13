package io.github.jamezrin.outpace.core

data class EngineSettings(
    val lan: Boolean = false,
    val port: Int = DEFAULT_PORT,
    val seeding: Boolean = true,
    val inbound: Boolean = true,
    val portMapping: Boolean = false,
    val aceCompat: Boolean = false,
) {
    fun env(dataDir: String, cacheDir: String): Map<String, String> = linkedMapOf(
        "OUTPACE_BIND" to "${if (lan) "0.0.0.0" else StreamUrls.LOOPBACK}:$port",
        "OUTPACE_DATA_DIR" to dataDir,
        "OUTPACE_CACHE_DIR" to cacheDir,
        "OUTPACE_ENABLE_SEEDING" to seeding.toString(),
        "OUTPACE_ENABLE_INBOUND" to inbound.toString(),
        "OUTPACE_ENABLE_PORT_MAPPING" to portMapping.toString(),
        "OUTPACE_ACE_COMPAT" to aceCompat.toString(),
    )

    companion object {
        const val DEFAULT_PORT = 6878

        fun validPort(p: Int) = p in 1024..65535
    }
}
