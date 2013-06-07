package com.github.jillesvangurp.osm2geojson;

import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test
public class MergingPairIterableTest {
    private static final class IntegerComparator implements Comparator<Integer> {
        @Override
        public int compare(Integer o1, Integer o2) {
            return o1.compareTo(o2);
        }
    }

    @DataProvider
    public Object[][] params() {
        return new Object[][] {
                {asList(1,3,5),asList(2,4,6),asList(1,2,3,4,5,6)},
                {asList(1,2,3),asList(4,5,6),asList(1,2,3,4,5,6)},
                {asList(4,5,6),asList(1,2,3),asList(1,2,3,4,5,6)},
                {asList(1,3,5),asList(),asList(1,3,5)},
                {asList(),asList(1,3,5),asList(1,3,5)},
                {asList(),asList(),asList()}
        };
    }

    @Test(dataProvider="params")
    public void shouldMergeIterablesCorrectly(List<Integer> left,List<Integer> right,List<Integer> expected) {
        Iterator<Integer> it = new MergingPairIterable<>(left, right, new IntegerComparator()).iterator();
        for(Integer i: expected) {
            assertThat(it.next(), is(i));
        }
        assertThat(it.hasNext(), is(false));
    }
}
