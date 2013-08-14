package com.github.jillesvangurp.osm2geojson;

import static com.jillesvangurp.iterables.Iterables.count;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.util.Arrays;
import java.util.List;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.github.jillesvangurp.osm2geojson.EntryJoiningIterable.JoinedEntries;

@Test
public class EntryJoiningIterableTest {

    List<String> left=Arrays.asList(
            "1;one",
            "1;ein",
            "2;two",
            "3;drei",
            "3;three",
            "3;drie",
            "4;four",
            "5;five"
            );

    List<String> right=Arrays.asList(
            "1;111111",
            "1;111",
            "1;1",
            "2;22222",
            "2;22",
            "5;5555",
            "6;6666"
            );

    private EntryJoiningIterable it;

    @BeforeMethod
    public void beforeMethod() {
        it = new EntryJoiningIterable(left,right);
    }
    public void shouldJoinTwoSortedMultiMapsAndProduceRightNumberOfEntries() {
        assertThat(count(it), is(3l));
    }

    public void shouldProduceCorrectJoinedElements() {
        JoinedEntries next = it.iterator().next();
        assertThat(next.left.size(), is(2));
        assertThat(next.right.size(), is(3));
        next = it.iterator().next();
        assertThat(next.left.size(), is(1));
        assertThat(next.right.size(), is(2));
        next = it.iterator().next();
        assertThat(next.left.size(), is(1));
        assertThat(next.right.size(), is(1));
    }
}
