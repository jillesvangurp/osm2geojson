package com.github.jillesvangurp.mergesort

import com.github.jillesvangurp.common.ImmutableEntry

class EntryParsingProcessor {
    fun process(line: String): Map.Entry<String, String> {
        val idx = line.indexOf(';')
        val key = line.substring(0, idx)
        val value = line.substring(idx + 1)
        return ImmutableEntry(key, value)
    }
}
