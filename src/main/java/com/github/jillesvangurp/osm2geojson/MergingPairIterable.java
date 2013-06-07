package com.github.jillesvangurp.osm2geojson;

import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;

import com.jillesvangurp.iterables.PeekableIterator;

/**
 * Merges two iterables that are each sorted into a single iterable.
 * @param <J>
 */
public class MergingPairIterable<J> implements Iterable<J> {
    private final Iterable<J> left;
    private final Iterable<J> right;
    private final Comparator<J> comparator;

    public MergingPairIterable(Iterable<J> left, Iterable<J> right, Comparator<J> comparator) {
        this.left = left;
        this.right = right;
        this.comparator = comparator;
    }

    @Override
    public Iterator<J> iterator() {
        final PeekableIterator<J> leftIter = new PeekableIterator<>(left.iterator());
        final PeekableIterator<J> rightIter = new PeekableIterator<>(right.iterator());
        return new Iterator<J>() {
            J next=null;

            @Override
            public boolean hasNext() {
                if(next != null) {
                    return true;
                }
                if(leftIter.hasNext() && rightIter.hasNext()) {
                    J ln = leftIter.peek();
                    J rn = rightIter.peek();
                    if(comparator.compare(ln,rn) <= 0) {
                        next=leftIter.next();
                    } else {
                        next=rightIter.next();
                    }
                    return true;

                } else if(leftIter.hasNext()) {
                    next=leftIter.next();
                    return true;
                } else if(rightIter.hasNext()) {
                    next=rightIter.next();
                    return true;
                } else {
                    return false;
                }
            }

            @Override
            public J next() {
                if(hasNext()) {
                    J result=next;
                    next=null;
                    return result;
                } else {
                    throw new NoSuchElementException();
                }
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException("remove not supported");
            }
        };
    }

}