package com.github.jillesvangurp.osm2geojson;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.util.Map.Entry;

import org.testng.annotations.Test;

@Test
public class EntryParsingProcessorTest {

    public void shouldParseEntries() {
        EntryParsingProcessor processor = new EntryParsingProcessor();
        Entry<String, String> entry = processor.process("foo;bar");
        assertThat(entry.getKey(), is("foo"));
        assertThat(entry.getValue(), is("bar"));
    }
}
