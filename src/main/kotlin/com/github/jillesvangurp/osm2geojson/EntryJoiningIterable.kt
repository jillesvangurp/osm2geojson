package com.github.jillesvangurp.osm2geojson

import com.github.jillesvangurp.common.ImmutableEntry

class EntryJoiningIterable(
    leftLines: Iterable<String>,
    rightLines: Iterable<String>
) : Iterable<EntryJoiningIterable.JoinedEntries> {
    private val leftIter = leftLines.map { parse(it) }.iterator()
    private val rightIter = rightLines.map { parse(it) }.iterator()

    override fun iterator(): Iterator<JoinedEntries> = object : Iterator<JoinedEntries> {
        private var leftPeek: Map.Entry<String, String>? = if (leftIter.hasNext()) leftIter.next() else null
        private var rightPeek: Map.Entry<String, String>? = if (rightIter.hasNext()) rightIter.next() else null

        override fun hasNext(): Boolean {
            while (leftPeek != null && rightPeek != null) {
                val cmp = leftPeek!!.key.compareTo(rightPeek!!.key)
                when {
                    cmp < 0 -> leftPeek = if (leftIter.hasNext()) leftIter.next() else null
                    cmp > 0 -> rightPeek = if (rightIter.hasNext()) rightIter.next() else null
                    else -> return true
                }
            }
            return false
        }

        override fun next(): JoinedEntries {
            if (!hasNext()) throw NoSuchElementException()
            val key = leftPeek!!.key
            val left = mutableListOf<Map.Entry<String, String>>()
            val right = mutableListOf<Map.Entry<String, String>>()
            while (leftPeek != null && leftPeek!!.key == key) {
                left += leftPeek!!
                leftPeek = if (leftIter.hasNext()) leftIter.next() else null
            }
            while (rightPeek != null && rightPeek!!.key == key) {
                right += rightPeek!!
                rightPeek = if (rightIter.hasNext()) rightIter.next() else null
            }
            return JoinedEntries(left, right)
        }
    }

    data class JoinedEntries(
        val left: List<Map.Entry<String, String>>,
        val right: List<Map.Entry<String, String>>
    )

    companion object {
        private fun parse(line: String): Map.Entry<String, String> {
            val idx = line.indexOf(';')
            val key = line.substring(0, idx)
            val value = line.substring(idx + 1)
            return ImmutableEntry(key, value)
        }
    }
}
