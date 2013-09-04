package com.github.jillesvangurp.mergesort;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.jillesvangurp.common.ResourceUtil;
import com.github.jillesvangurp.metrics.LoggingCounter;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.TreeMultimap;
import com.jillesvangurp.iterables.LineIterable;

/**
 * Takes key value parameters and produces a file with lines of key;value sorted by key. Implements merge sort and uses
 * a temp directory to store in between files so it can sort more data than fits into memory.
 */
public class SortingWriter implements Closeable {
    private static Logger LOG=LoggerFactory.getLogger(SortingWriter.class);

    private final String output;
    private final int bucketSize;
    private int currentBucket = 0;
    private final List<String> bucketFiles = new ArrayList<>();

    Multimap<String, String> bucket = Multimaps.synchronizedMultimap(TreeMultimap.<String,String>create());
    private final String tempDir;

    ReadWriteLock bucketLock = new ReentrantReadWriteLock();

    private final LoggingCounter loggingCounter;

    public SortingWriter(String tempDir, String output, int bucketSize) throws IOException {
        this.tempDir = tempDir;
        this.output = output;
        this.bucketSize = bucketSize;
        if(StringUtils.isNotEmpty(tempDir)) {
            FileUtils.forceMkdir(new File(tempDir));
        }
        loggingCounter = LoggingCounter.counter(LOG, "sort buckets " + output , "lines", 100000);
    }

    public void put(String key, String value) {
        if (bucket.size() >= bucketSize) {
            flushBucket(false);
        }
        bucketLock.readLock().lock();
        try {
            boolean added = bucket.put(key, value);
            if(!added) {
                LOG.warn("failed to add " +key+";"+value);
            } else {
                loggingCounter.inc();
            }
        } finally {
            bucketLock.readLock().unlock();
        }
    }

    private void flushBucket(boolean skipSizeCheck) {
        Multimap<String, String> oldBucket=null;
        int bucketNr=-1;
        bucketLock.writeLock().lock();
        try {
            if(bucket.size() > bucketSize || skipSizeCheck) {
                // atomically switch over the bucket and then write the old one
                oldBucket = bucket;
                bucketNr= currentBucket;
                currentBucket++;
                bucket = Multimaps.synchronizedMultimap(TreeMultimap.<String,String>create());
            }
        } finally {
            bucketLock.writeLock().unlock();
        }
        if(oldBucket != null) {
            File file = new File(tempDir, "bucket-" + bucketNr + ".gz");

            try (BufferedWriter bw = ResourceUtil.gzipFileWriter(file.getAbsolutePath())) {
                for (Entry<String, String> e : oldBucket.entries()) {
                    bw.write(e.getKey() + ";" + e.getValue() + "\n");
                }
                bucketFiles.add(file.getAbsolutePath());
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }
    }

    public static String key(String line) {
        int idx = line.indexOf(';');
        if (idx < 0) {
            throw new IllegalStateException("line has no key " + line);
        }
        return line.substring(0, idx);
    }

    @Override
    public void close() throws IOException {
        if (bucket.size() > 0) {
            flushBucket(true);
        }
        loggingCounter.close();
        LoggingCounter mergeCounter = LoggingCounter.counter(LOG, "merge buckets into "  + output, " lines", 100000);
        List<LineIterable> lineIterables = new ArrayList<>();
        try {
            for (String file : bucketFiles) {
                lineIterables.add(LineIterable.openGzipFile(file));
            }
            LOG.info("merging " + lineIterables.size() + " buckets");

            MergingEntryIterable mergeIt = new MergingEntryIterable(lineIterables);

            try (BufferedWriter bw = ResourceUtil.gzipFileWriter(output)) {
                for (Entry<String, String> entry : mergeIt) {
                    bw.write(entry.toString() + '\n');
                    mergeCounter.inc();
                }
            }
        } finally {
            for (LineIterable li : lineIterables) {
                li.close();
            }
            mergeCounter.close();
            FileUtils.deleteDirectory(new File(tempDir));
        }
    }
}
