package com.github.jillesvangurp.osm2geojson;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

import java.io.IOException;
import java.util.Arrays;

import org.testng.annotations.Test;

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

    public void shouldMergeNodeJsonWithWayIds() throws IOException {
        OsmJoin osmJoin = new OsmJoin();
        TreeMapSortingWriter sortingWriter = new TreeMapSortingWriter();
        osmJoin.mergeNodeJsonWithWayIds(Arrays.asList("5;node5","7;node7","10;node10"), Arrays.asList("5;1","6;2","7;4","10;1"), sortingWriter);
        assertThat(sortingWriter.output.get("1"), containsInAnyOrder("node5","node10"));
        assertThat(sortingWriter.output.get("4"), containsInAnyOrder("node7"));
    }

}
