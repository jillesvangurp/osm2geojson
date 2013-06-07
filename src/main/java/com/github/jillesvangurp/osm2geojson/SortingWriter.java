package com.github.jillesvangurp.osm2geojson;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.commons.io.FileUtils;

import com.jillesvangurp.iterables.LineIterable;

/**
 * Takes key value parameters and produces a file with lines of key;value sorted by key. Implements merge sort and uses
 * a temp directory to store in between files so it can sort more data than fits into memory.
 */
public class SortingWriter implements Closeable {
    private final String output;
    private final int bucketSize;
    private int currentBucket = 0;
    private final List<String> bucketFiles = new ArrayList<>();

    TreeMap<String, String> bucket = new TreeMap<>();
    private final String tempDir;

    public SortingWriter(String tempDir, String output, int bucketSize) throws IOException {
        this.tempDir = tempDir;
        this.output = output;
        this.bucketSize = bucketSize;
        FileUtils.forceMkdir(new File(tempDir));
    }

    public void put(String key, String value) {
        if (bucket.size() >= bucketSize) {
            flushBucket();
        }
        bucket.put(key, value);
    }

    private void flushBucket() {
        File file = new File(tempDir, "bucket-" + currentBucket + ".gz");
        currentBucket++;
        try (BufferedWriter bw = OsmProcessor.gzipFileWriter(file.getAbsolutePath())) {
            for (Entry<String, String> e : bucket.entrySet()) {
                bw.write(e.getKey() + ";" + e.getValue() + "\n");
            }
            bucketFiles.add(file.getAbsolutePath());
            bucket.clear();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void close() throws IOException {
        if (bucket.size() > 0) {
            flushBucket();
        }
        List<LineIterable> lineIterables = new ArrayList<>();
        try {
            for (String file : bucketFiles) {
                lineIterables.add(LineIterable.openGzipFile(file));
            }
            MergingIterable<String> mergeIt = new MergingIterable<String>(lineIterables, new Comparator<String>() {

                @Override
                public int compare(String line1, String line2) {
                    return key(line1).compareTo(key(line2));
                }

                private String key(String line) {
                    int idx = line.indexOf(';');
                    if (idx < 0) {
                        throw new IllegalStateException("line has no key " + line);
                    }
                    return line.substring(0, idx);
                }
            });
            try (BufferedWriter bw = OsmProcessor.gzipFileWriter(output)) {
                for (String line : mergeIt) {
                    bw.write(line + '\n');
                }
            }
        } finally {
            for (LineIterable li : lineIterables) {
                li.close();
            }
        }
    }
}
