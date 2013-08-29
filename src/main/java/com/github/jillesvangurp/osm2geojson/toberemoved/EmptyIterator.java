package com.github.jillesvangurp.osm2geojson.toberemoved;

import java.util.Iterator;
import java.util.NoSuchElementException;

final class EmptyIterator<T> implements Iterator<T> {
    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public T next() {
        throw new NoSuchElementException();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}