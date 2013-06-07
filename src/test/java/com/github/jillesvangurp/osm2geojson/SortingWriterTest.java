package com.github.jillesvangurp.osm2geojson;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.commons.io.FileUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.io.Files;
import com.jillesvangurp.iterables.LineIterable;

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
        try(SortingWriter sortingWriter = new SortingWriter(tempDir, outputFile, 3)) {
            for(int i=0;i<=1000;i++) {
                sortingWriter.put("" + i%5, "-");

                written.add(""+i%5+";-");
            }
        }
        Collections.sort(written);
        try(LineIterable it=LineIterable.openGzipFile(outputFile)) {
            ArrayList<String> read = new ArrayList<>();
            for(String s:it) {
                read.add(s);
            }
            assertThat(read, is(written));
        }
    }

    @AfterMethod
    public void afterMethod() throws IOException {
        FileUtils.forceDelete(new File(tempDir));
    }
}
