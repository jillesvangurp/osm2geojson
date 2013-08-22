package com.github.jillesvangurp.mergesort;

import static com.jillesvangurp.iterables.Iterables.map;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;

import com.github.jillesvangurp.osm2geojson.EntryParsingProcessor;
import com.jillesvangurp.iterables.LineIterable;
import com.jillesvangurp.iterables.PeekableIterator;

public class MergingEntryIterable implements Iterable<Entry<String,String>>{

    private final List<LineIterable> iterables;

    public MergingEntryIterable(List<LineIterable> iterables ) {
        this.iterables = iterables;
    }

    @Override
    public Iterator<Entry<String, String>> iterator() {
        final PriorityQueue<PeekableIterator<Entry<String,String>>> iterators = new PriorityQueue<>(iterables.size(), new Comparator<PeekableIterator<Entry<String,String>>>() {

            @Override
            public int compare(PeekableIterator<Entry<String, String>> o1, PeekableIterator<Entry<String, String>> o2) {
                String key1 = o1.peek().getKey();
                String key2 = o2.peek().getKey();
                return key1.compareTo(key2);
            }
        });

        for(LineIterable it: iterables) {
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
                        PeekableIterator<Entry<String, String>> iterator = iterators.poll();
                        next = iterator.next();
                        if(iterator.hasNext()) {
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
