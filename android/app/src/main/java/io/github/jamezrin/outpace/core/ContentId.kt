package io.github.jamezrin.outpace.core

/** Pulls an AceStream content id out of a link, a shared message or a pasted string. */
object ContentId {
    sealed interface Result {
        data class Id(val hex: String) : Result

        /** The text names an infohash, which the app does not hand off. */
        data object Infohash : Result

        data object None : Result
    }

    private const val HEX = "[0-9a-fA-F]"
    private val queryId = Regex("(?i)[?&](?:content_id|id)=($HEX{40})(?!$HEX)")
    private val schemeId = Regex("(?i)acestream://(?:cid:)?($HEX{40})(?!$HEX)")
    private val explicitInfohash =
        Regex("(?i)(acestream:\\S*[?&]infohash=|magnet:\\?\\S*btih:|acestream://ih:|(?<![0-9a-z])ih:$HEX{40})")
    private val bareId = Regex("(?<!$HEX)($HEX{40})(?!$HEX)")

    fun extract(text: String): Result {
        queryId.find(text)?.let { return Result.Id(it.groupValues[1].lowercase()) }
        schemeId.find(text)?.let { return Result.Id(it.groupValues[1].lowercase()) }
        if (explicitInfohash.containsMatchIn(text)) return Result.Infohash
        bareId.find(text)?.let { return Result.Id(it.groupValues[1].lowercase()) }
        return Result.None
    }

    fun isValid(hex: String): Boolean = hex.length == 40 && hex.all { it in '0'..'9' || it in 'a'..'f' }
}
