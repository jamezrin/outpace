package io.github.jamezrin.outpace.core

import org.json.JSONObject

/** One entry of `GET /streams`. `id` keeps the engine's prefix (`cid:` or `ih:`). */
data class StreamInfo(val id: String, val title: String?, val bitrate: Long?, val clients: Int) {
    /** The bare content id, when the stream is addressed by one. */
    val contentId: String? get() = id.removePrefix("cid:").takeIf { id.startsWith("cid:") }
}

object StreamsJson {
    fun parse(json: String): List<StreamInfo> {
        val array = JSONObject(json).optJSONArray("streams") ?: return emptyList()
        return (0 until array.length()).map { i ->
            val o = array.getJSONObject(i)
            val meta = o.optJSONObject("metadata")
            StreamInfo(
                id = o.getString("id"),
                title = meta?.optString("title")?.takeIf { meta.has("title") && !meta.isNull("title") && it.isNotBlank() },
                bitrate = meta?.takeIf { it.has("bitrate") && !it.isNull("bitrate") }?.getLong("bitrate"),
                clients = o.optInt("clients", 0),
            )
        }
    }
}
