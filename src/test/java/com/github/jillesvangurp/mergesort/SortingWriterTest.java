package com.github.jillesvangurp.mergesort;

import static com.jillesvangurp.iterables.Iterables.consume;
import static com.jillesvangurp.iterables.Iterables.count;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.io.Files;
import com.jillesvangurp.iterables.ConcurrentProcessingIterable;
import com.jillesvangurp.iterables.Iterables;
import com.jillesvangurp.iterables.LineIterable;
import com.jillesvangurp.iterables.Processor;

@Test
public class SortingWriterTest {

    private String tempDir;

    @BeforeMethod
    public void beforeMethod() {
        tempDir = Files.createTempDir().getAbsolutePath();
    }

    public void shouldSort() throws IOException {
        String outputFile = new File(tempDir,"out.gz").getAbsolutePath();
        ArrayList<String> written = new ArrayList<>();
        try(SortingWriter sortingWriter = new SortingWriter(tempDir+"/work", outputFile, 3)) {
            for(int i=0;i<=1000;i++) {
                sortingWriter.put("" + i%5, "-");

                written.add(""+i%5+";-");
            }
        }
        Collections.sort(written);
        ArrayList<String> read = readItems(outputFile);
        assertThat(read, is(written));
    }

    private ArrayList<String> readItems(String outputFile) throws IOException {
        ArrayList<String> read = new ArrayList<>();
        try(LineIterable it=LineIterable.openGzipFile(outputFile)) {
            for(String s:it) {
                read.add(s);
            }
        }
        return read;
    }

    public void shouldSortConcurrently() throws IOException {
        String outputFile = new File(tempDir,"out2.gz").getAbsolutePath();


        Iterable<Integer> it = Iterables.toIterable(new Iterator<Integer>() {
            int max=30000;
            int current=0;

            @Override
            public boolean hasNext() {
                return current<max;
            }

            @Override
            public Integer next() {
                return current++;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }});


        final CopyOnWriteArrayList<String> written = new CopyOnWriteArrayList<>();
        try(SortingWriter sortingWriter = new SortingWriter(tempDir+"/work", outputFile, 50)) {
            Processor<Integer,Integer> processor = new Processor<Integer,Integer>() {

                @Override
                public Integer process(Integer i) {
                    sortingWriter.put("" + i%5, ""+i);
                    written.add(""+i%5+";"+i);
                    return i;
                }
            };
            try(ConcurrentProcessingIterable<Integer, Integer> processConcurrently = Iterables.processConcurrently(it, processor, 2, 50, 4)) {
                consume(processConcurrently);
            }
        }
        ArrayList<String> copy = new ArrayList<>(written);
        Collections.sort(copy);
        ArrayList<String> read = readItems(outputFile);
        assertThat(read.size(), is(copy.size()));
        for(int i=0; i<copy.size();i++) {
            String keyR = read.get(i).split(";")[0];
            String keyW = copy.get(i).split(";")[0];
            // note values might be in different order but should be sorted by key
            assertThat(keyR, is(keyW));
        }
    }

    public void shouldNotLoseEntries() throws IOException {
        String outputFile = new File(tempDir,"out.gz").getAbsolutePath();

        long lines = 20000;
        try(SortingWriter sortingWriter = new SortingWriter(tempDir+"/work", outputFile, 1000)) {
            for(int i=0; i< lines;i++) {
                String key = RandomStringUtils.randomAlphanumeric(2); // should cause some duplicate keys
                String value = RandomStringUtils.randomAlphanumeric(200);
                sortingWriter.put(key, value);
            }
        }
        assertThat(count(LineIterable.openGzipFile(outputFile)),is(lines));
    }

    @AfterMethod
    public void afterMethod() throws IOException {
        FileUtils.forceDelete(new File(tempDir));
    }
}
