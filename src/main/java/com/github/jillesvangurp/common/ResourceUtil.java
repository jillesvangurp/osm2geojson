package com.github.jillesvangurp.common;


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.lang.StringUtils;


public class ResourceUtil {
    public static final Charset UTF8 = Charset.forName("UTF-8");
    private static final String SEPARATOR = "-";

    public static String fileName(String source, String extension, String...qualifiers) {
        String baseDir = getBaseDir();
        String baseName = baseName(source, qualifiers);
        String timestamp = DateUtil.formatFileDate();
        return new File(baseDir, baseName + timestamp + "." + extension).getAbsolutePath();
    }

    private static String baseName(String source, String... qualifiers) {
        String extra="";
        if(qualifiers.length > 0) {
            extra = StringUtils.join(qualifiers, SEPARATOR) + SEPARATOR;
        }
        String baseName = source + SEPARATOR + extra;
        return baseName;
    }
    public static String getBaseDir() {
        String baseDir = System.getProperty("baseDir");
        if(StringUtils.isNotBlank(baseDir)) {
            File file = new File(baseDir);
            if(!file.exists()) {
                throw new IllegalArgumentException("base directory " + baseDir + " does not exist");
            }
            if(!file.isDirectory()) {
                throw new IllegalArgumentException("path " + baseDir + " is not a directory; baseDir should be an existing directory");
            }
        } else {
            baseDir = ".";
        }
        return baseDir;
    }

    public static BufferedWriter gzipFileWriter(String source, String extension, String...qualifiers) throws IOException {
        String fileName = fileName(source, extension, qualifiers);
        return gzipFileWriter(fileName);
    }

    public static BufferedWriter gzipFileWriter(String file) throws IOException {
        return new BufferedWriter(new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(file)), Charset.forName("utf-8")));
    }

    public static BufferedWriter gzipFileWriter(File file) throws IOException {
        return new BufferedWriter(new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(file)), Charset.forName("utf-8")),64*1024);
    }

    public static BufferedReader gzipFileReader(final String source, final String extension, final String...qualifiers) throws IOException {
        String baseDir = getBaseDir();
        File file = new File(baseDir);
        String[] files = file.list(new FilenameFilter() {

            @Override
            public boolean accept(File dir, String name) {
                if(name.endsWith(extension) && name.startsWith(baseName(source, qualifiers))) {
                    return true;
                } else {
                    return false;
                }
            }
        });

        Arrays.sort(files);

        if(file.length() == 0) {
            throw new IllegalArgumentException("no files found in " + baseDir + "matching " + baseName(source, qualifiers) + "*."+extension);
        } else {
            return gzipFileReader(new File(baseDir,files[files.length-1]));
        }
    }

    public static File latestFile(String baseDir, final String source, final String extension, final String...qualifiers) {
        File file = new File(baseDir);
        String[] files = file.list(new FilenameFilter() {

            @Override
            public boolean accept(File dir, String name) {
                if(name.endsWith(extension) && name.startsWith(baseName(source, qualifiers))) {
                    return true;
                } else {
                    return false;
                }
            }
        });

        Arrays.sort(files);

        if(file.length() == 0) {
            throw new IllegalArgumentException("no files found in " + baseDir + "matching " + baseName(source, qualifiers) + "*."+extension);
        } else {
            return new File(baseDir,files[files.length-1]);
        }
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

    /**
     * @param r
     * @return string content for the reader
     */
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
