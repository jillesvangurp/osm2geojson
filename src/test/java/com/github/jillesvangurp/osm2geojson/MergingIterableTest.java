package com.github.jillesvangurp.osm2geojson;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test
public class MergingIterableTest {
    private static final class IntegerComparator implements Comparator<Integer> {
        @Override
        public int compare(Integer o1, Integer o2) {
            return o1.compareTo(o2);
        }
    }

    @SuppressWarnings("unchecked")
    @DataProvider
    public Object[][] params() {
        return new Object[][] {
                {list(list(1,3,5),list(2,4,6)),list(1,2,3,4,5,6)},
                {list(list(1,3,5),list(2,4,6),list(7,8,9)),list(1,2,3,4,5,6,7,8,9)},
        };
    }

    @SuppressWarnings("unchecked")
    <T> List<T> list(T...values) {
        ArrayList<T> result = new ArrayList<>();
        for(T v: values) {
            result.add(v);
        }
        return result;
    }

    @Test(dataProvider="params")
    public void shouldMergeIterablesCorrectly(List<Iterable<Integer>> iterables,List<Integer> expected) {
        Iterator<Integer> it = new MergingIterable<>(iterables, new IntegerComparator()).iterator();
        for(Integer i: expected) {
            assertThat(it.next(), is(i));
        }
        assertThat(it.hasNext(), is(false));
    }
}
