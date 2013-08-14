package com.github.jillesvangurp.osm2geojson;

import java.io.IOException;

import org.testng.annotations.Test;

import com.github.jillesvangurp.mergesort.SortingWriter;
import com.google.common.collect.TreeMultimap;

@Test
public class OsmJoinTest {
    class TreeMapSortingWriter extends SortingWriter {
        TreeMultimap<String, String> output = TreeMultimap.create();
        public TreeMapSortingWriter() throws IOException {
            super(null, null, 1);
        }

        @Override
        public void put(String key, String value) {
            output.put(key, value);
        }
    }

}
