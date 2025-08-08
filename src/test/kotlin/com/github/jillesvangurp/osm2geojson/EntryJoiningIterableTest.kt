package com.github.jillesvangurp.osm2geojson

import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.`is`
import org.testng.annotations.BeforeMethod
import org.testng.annotations.Test

class EntryJoiningIterableTest {
    private lateinit var it: EntryJoiningIterable

    private val left = listOf(
        "1;one",
        "1;ein",
        "2;two",
        "3;drei",
        "3;three",
        "3;drie",
        "4;four",
        "5;five"
    )

    private val right = listOf(
        "1;111111",
        "1;111",
        "1;1",
        "2;22222",
        "2;22",
        "5;5555",
        "6;6666"
    )

    @BeforeMethod
    fun before() {
        it = EntryJoiningIterable(left, right)
    }

    @Test
    fun shouldJoinTwoSortedMultiMapsAndProduceRightNumberOfEntries() {
        assertThat(it.count().toLong(), `is`(3L))
    }

    @Test
    fun shouldProduceCorrectJoinedElements() {
        val iterator = it.iterator()
        var next = iterator.next()
        assertThat(next.left.size, `is`(2))
        assertThat(next.right.size, `is`(3))
        next = iterator.next()
        assertThat(next.left.size, `is`(1))
        assertThat(next.right.size, `is`(2))
        next = iterator.next()
        assertThat(next.left.size, `is`(1))
        assertThat(next.right.size, `is`(1))
    }
}
