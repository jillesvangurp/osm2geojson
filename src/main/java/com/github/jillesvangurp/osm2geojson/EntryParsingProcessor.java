package com.github.jillesvangurp.osm2geojson;

import java.util.Map.Entry;

import com.jillesvangurp.iterables.Processor;

public class EntryParsingProcessor implements Processor<String, Entry<String,String>> {

    @Override
    public Entry<String, String> process(String line) {
        int idx = line.indexOf(';');
        final String key=line.substring(0,idx);
        final String value=line.substring(idx+1);
        return new Entry<String,String>() {

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
        };

    }

}