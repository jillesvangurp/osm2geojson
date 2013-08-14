package com.github.jillesvangurp.mergesort;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;


/**
 * Merges a list of sorted iterables.
 *
 * @param <T>
 */
public class MergingIterable<T> implements Iterable<T>{

    private final List<? extends Iterable<T>> iterables;
    private final Comparator<T> comparator;

    public MergingIterable(List<? extends Iterable<T>> iterables, Comparator<T> comparator) {
        this.iterables = iterables;
        this.comparator = comparator;
    }

    @Override
    public Iterator<T> iterator() {
        if(iterables.size() >=2 ) {
            Iterable<T> last = iterables.remove(iterables.size()-1);
            return compose(last,iterables).iterator();
        } else if(iterables.size() ==1) {
            return iterables.get(0).iterator();
        } else {
            return new EmptyIterator<T>();
        }
    }

    private Iterable<T> compose(Iterable<T> it, List<? extends Iterable<T>> iterables) {
        if(iterables.size() == 1) {
            return new MergingPairIterable<>(it, iterables.get(0), comparator);
        } else {
            Iterable<T> last = iterables.remove(iterables.size()-1);
            return new MergingPairIterable<T>(it, compose(last,iterables), comparator);
        }
    }
}
