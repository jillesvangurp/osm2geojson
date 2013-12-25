package com.github.jillesvangurp.mergesort;

import static com.jillesvangurp.iterables.Iterables.map;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;

import com.jillesvangurp.iterables.LineIterable;
import com.jillesvangurp.iterables.PeekableIterator;

/**
 * Merges a number of iterables of entries that are sorted on their key to produce s single iterable that yields the entries in a sorted order.
 */
public class MergingEntryIterable implements Iterable<Entry<String,String>>{

    private final List<LineIterable> iterables;

    public MergingEntryIterable(List<LineIterable> iterables ) {
        this.iterables = iterables;
    }

    @Override
    public Iterator<Entry<String, String>> iterator() {
        // use a priority queue to ensure that the iterable with the next entry is at the had of the queue.
        int size = iterables.size();
        if(size == 0) {
            return new ArrayList<Entry<String, String>>().iterator();
        }
        final PriorityQueue<PeekableIterator<Entry<String,String>>> iterators = new PriorityQueue<>(size, new Comparator<PeekableIterator<Entry<String,String>>>() {

            @Override
            public int compare(PeekableIterator<Entry<String, String>> o1, PeekableIterator<Entry<String, String>> o2) {
                String key1 = o1.peek().getKey();
                String key2 = o2.peek().getKey();
                return key1.compareTo(key2);
            }
        });

        for(LineIterable it: iterables) {
            // use peekable iterators so that we can inspect the next value of each iterator before it is yielded
            PeekableIterator<Entry<String,String>> peekableIterator = new PeekableIterator<Entry<String,String>>(map(it, new EntryParsingProcessor()));
            if(peekableIterator.hasNext()) {
                iterators.add(peekableIterator);
            }
        }
        return new Iterator<Entry<String,String>>() {
            private Entry<String,String> next=null;

            @Override
            public boolean hasNext() {
                if(next != null) {
                    return true;
                } else {
                    if(iterators.size()>0) {
                        // get the iterator with the next entry from the priority queue
                        PeekableIterator<Entry<String, String>> iterator = iterators.poll();
                        next = iterator.next();
                        if(iterator.hasNext()) {
                            // insert it back into the priority queue
                            iterators.offer(iterator);
                        }
                    }
                }
                return next != null;
            }

            @Override
            public Entry<String, String> next() {
                if(hasNext()) {
                    Entry<String,String> result = next;
                    next=null;
                    return result;
                } else {
                    throw new NoSuchElementException();
                }
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }
}
