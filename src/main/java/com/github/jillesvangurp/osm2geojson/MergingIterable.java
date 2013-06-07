package com.github.jillesvangurp.osm2geojson;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang.Validate;


/**
 * Merges a list of sorted iterables.
 *
 * @param <T>
 */
public class MergingIterable<T> implements Iterable<T>{

    private final List<Iterable<T>> iterables;
    private final Comparator<T> comparator;

    public MergingIterable(List<Iterable<T>> iterables, Comparator<T> comparator) {
        Validate.isTrue(iterables.size() >= 1, "should have at least one iterable");
        this.iterables = iterables;
        this.comparator = comparator;
    }

    @Override
    public Iterator<T> iterator() {
        if(iterables.size() >=2 ) {
            Iterable<T> last = iterables.remove(iterables.size()-1);
            return compose(last,iterables).iterator();
        } else {
            return iterables.get(0).iterator();
        }
    }

    private Iterable<T> compose(Iterable<T> it, List<Iterable<T>> iterables) {
        if(iterables.size() == 1) {
            return new MergingPairIterable<>(it, iterables.get(0), comparator);
        } else {
            Iterable<T> last = iterables.remove(iterables.size()-1);
            return new MergingPairIterable<T>(it, compose(last,iterables), comparator);
        }
    }
}
