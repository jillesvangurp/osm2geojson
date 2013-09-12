package com.github.jillesvangurp.common;


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;


/**
 * Collection of static methods for working with files that combine commonly used constructor calls for opening files for
 * reading or writing.
 */
public class ResourceUtil {
    public static final Charset UTF8 = Charset.forName("UTF-8");

    public static BufferedWriter gzipFileWriter(String file) throws IOException {
        return new BufferedWriter(new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(file)), Charset.forName("utf-8")));
    }

    public static BufferedWriter gzipFileWriter(File file) throws IOException {
        return new BufferedWriter(new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(file)), Charset.forName("utf-8")),64*1024);
    }

    public static BufferedReader gzipFileReader(String file) throws IOException {
        return new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(file)),UTF8));
    }

    public static BufferedReader gzipFileReader(File file) throws IOException {
        return new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(file)),UTF8));
    }

    public static BufferedWriter fileWriter(String file) throws IOException {
        return new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file), UTF8));
    }

    public static BufferedReader fileReader(String file) throws IOException {
        return new BufferedReader(new InputStreamReader(new FileInputStream(file),UTF8));
    }

    public static BufferedWriter fileWriter(File file) throws IOException {
        return new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file), UTF8));
    }

    public static BufferedReader fileReader(File file) throws IOException {
        return new BufferedReader(new InputStreamReader(new FileInputStream(file),UTF8));
    }

    public static InputStreamReader bzip2Reader(String fileName) throws IOException, FileNotFoundException {
        return new InputStreamReader(new BZip2CompressorInputStream(new FileInputStream(fileName),true), UTF8);
    }

    public static BufferedReader resource(String resourcePath) throws IOException {
        InputStream is = ResourceUtil.class.getClassLoader().getResourceAsStream(resourcePath);
        if(is != null) {
            return new BufferedReader(new InputStreamReader(is,UTF8));
        } else {
            throw new IllegalArgumentException("resource " + resourcePath + " does not exist");
        }
    }

    public static String string(BufferedReader r) {
        try {
            try {
                StringBuilder buf = new StringBuilder();
                String line;
                while((line=r.readLine()) != null) {
                    buf.append(line +'\n');
                }
                return buf.toString();
            } finally {
                r.close();
            }
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }
    public static String string(InputStream is) {
        return string(new BufferedReader(new InputStreamReader(is, UTF8)));
    }
}
