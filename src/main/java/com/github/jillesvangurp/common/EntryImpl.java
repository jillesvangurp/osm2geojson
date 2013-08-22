package com.github.jillesvangurp.common;

import java.util.Map.Entry;

public final class EntryImpl implements Entry<String, String> {
    private final String value;
    private final String key;

    public EntryImpl(String value, String key) {
        this.value = value;
        this.key = key;
    }

    @Override
    public String getKey() {
        return key;
    }

    @Override
    public String getValue() {
        return value;
    }

    @Override
    public String setValue(String value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
        return key + ";" + value;
    }
}