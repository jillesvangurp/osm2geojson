package com.github.jillesvangurp.osm2geojson;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.NoSuchElementException;

import org.apache.commons.lang.StringUtils;

import com.github.jillesvangurp.osm2geojson.EntryJoiningIterable.JoinedEntries;
import com.jillesvangurp.iterables.LineIterable;
import com.jillesvangurp.iterables.PeekableIterator;
import com.jillesvangurp.iterables.Processor;

public class EntryJoiningIterable implements Iterable<JoinedEntries> {
    private final PeekableIterator<Entry<String,String>> left;
    private final PeekableIterator<Entry<String,String>> right;

    EntryJoiningIterable(Iterable<String> l, Iterable<String> r) {
        left=OsmJoin.peekableEntryIterable(l);
        right=OsmJoin.peekableEntryIterable(r);
    }

    public static void join(String leftMapFile, String rightMapFile, Processor<JoinedEntries, Boolean> processor) {
        try {
            try(LineIterable l= LineIterable.openGzipFile(leftMapFile)) {
                try(LineIterable r= LineIterable.openGzipFile(rightMapFile)) {
                    EntryJoiningIterable iterable = new EntryJoiningIterable(l, r);
                    OsmJoin.processIt(iterable, processor, 100, 9, 1000);
                }
            }
        } catch (IOException e) {
        }
    }

    @Override
    public Iterator<JoinedEntries> iterator() {
        return new Iterator<JoinedEntries>() {
            JoinedEntries next = null;

            @Override
            public boolean hasNext() {
                if(next != null) {
                    return true;
                } else {
                    while(left.hasNext() && right.hasNext()) {
                        next=new JoinedEntries();
                        Entry<String, String> leftEntry = left.next();
                        String leftKey = leftEntry.getKey();

                        while(right.hasNext() && !(leftKey.compareTo(right.peek().getKey())<=0)) {
                            // skip right entries until we find a matchin row
                            right.next();
                        }
                        while(right.hasNext() && leftKey.equals(right.peek().getKey())) {
                            next.right.add(right.next());
                        }
                        if(next.right.size() > 0) {
                            next.left.add(leftEntry);
                            // add any left entries with same id
                            while(left.hasNext() && leftKey.equals(left.peek().getKey())) {
                                next.left.add(left.next());
                            }
                            // we found a valid joined entry!
                            return true;
                        }
                    }
                }

                return false;
            }

            @Override
            public JoinedEntries next() {
                if(hasNext()) {
                    JoinedEntries result = next;
                    next = null;
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

    static class JoinedEntries {
        List<Entry<String,String>> left=new ArrayList<>();
        List<Entry<String,String>> right=new ArrayList<>();
        @Override
        public String toString() {
            return "left:\n\t"+StringUtils.join(left,"\n\t")+"\nright:\n\t"+StringUtils.join(right,"\n\t");
        }
    }
}