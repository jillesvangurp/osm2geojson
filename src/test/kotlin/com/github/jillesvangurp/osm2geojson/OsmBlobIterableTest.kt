package com.github.jillesvangurp.osm2geojson

import com.jillesvangurp.iterables.LineIterable
import org.testng.Assert.assertFalse
import org.testng.Assert.assertTrue
import org.testng.annotations.Test
import java.io.StringReader

class OsmBlobIterableTest {
    @Test
    fun shouldIterateOSM() {
        LineIterable(StringReader("foo\nbar\n<node>\n42</node>\nxxxxx")).use { lineIt ->
            val osmIt = OsmBlobIterable(lineIt)
            val iterator = osmIt.iterator()
            assertTrue(iterator.next().contains("42"))
            assertFalse(iterator.hasNext())
        }
    }
}
