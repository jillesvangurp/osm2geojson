package com.github.jillesvangurp.osm2geojson;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.io.StringReader;
import java.util.Iterator;

import org.testng.annotations.Test;

import com.jillesvangurp.iterables.LineIterable;

@Test
public class OsmBlobIterableTest {
    public void shouldIteradeOSM() {
        LineIterable it = new LineIterable(new StringReader("foo\nbar\n<node>\n42</node>\nxxxxx"));
        OsmBlobIterable osmIt = new OsmBlobIterable(it);
        Iterator<String> iterator = osmIt.iterator();
        assertTrue(iterator.next().contains("42"));
        assertFalse(iterator.hasNext());
    }
}
