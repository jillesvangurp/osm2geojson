package com.github.jillesvangurp.mergesort

import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.`is`
import org.testng.annotations.Test

class EntryParsingProcessorTest {
    @Test
    fun shouldParseEntries() {
        val processor = EntryParsingProcessor()
        val entry = processor.process("foo;bar")
        assertThat(entry.key, `is`("foo"))
        assertThat(entry.value, `is`("bar"))
    }
}
