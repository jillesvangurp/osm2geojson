package com.github.jillesvangurp.mergesort

import com.jillesvangurp.iterables.LineIterable
import org.apache.commons.io.FileUtils
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.`is`
import org.testng.annotations.AfterMethod
import org.testng.annotations.BeforeMethod
import org.testng.annotations.Test
import java.io.File
import java.nio.file.Files

class SortingWriterTest {
    private fun randomAlphanumeric(len: Int): String {
        val chars = ('a'..'z') + ('A'..'Z') + ('0'..'9')
        return (1..len).map { chars.random() }.joinToString("")
    }

    private lateinit var tempDir: File

    @BeforeMethod
    fun beforeMethod() {
        tempDir = Files.createTempDirectory("osm2geojson").toFile()
    }

    @AfterMethod
    fun afterMethod() {
        FileUtils.deleteDirectory(tempDir)
    }

    @Test
    fun shouldSort() {
        val outputFile = File(tempDir, "out.gz").absolutePath
        val written = mutableListOf<String>()
        SortingWriter(File(tempDir, "work").absolutePath, outputFile, 3).use { sortingWriter ->
            for (i in 0..1000) {
                sortingWriter.put("${i % 5}", "-")
                written.add("${i % 5};-")
            }
        }
        written.sort()
        val read = readItems(outputFile)
        assertThat(read, `is`(written))
    }

    private fun readItems(outputFile: String): ArrayList<String> {
        val read = arrayListOf<String>()
        LineIterable.openGzipFile(outputFile).use { it.forEach { line -> read.add(line) } }
        return read
    }

    @Test
    fun shouldNotLoseEntries() {
        val outputFile = File(tempDir, "out.gz").absolutePath
        val lines = 20000
        SortingWriter(File(tempDir, "work").absolutePath, outputFile, 1000).use { sortingWriter ->
            repeat(lines) {
                val key =randomAlphanumeric(2)
                val value =randomAlphanumeric(200)
                sortingWriter.put(key, value)
            }
        }
        LineIterable.openGzipFile(outputFile).use {
            assertThat(it.count(), `is`(lines))
        }
    }

    @Test
    fun shouldHandleNoElements() {
        val outputFile = File(tempDir, "out.gz").absolutePath
        SortingWriter(File(tempDir, "work").absolutePath, outputFile, 1000).use { }
        LineIterable.openGzipFile(outputFile).use {
            assertThat(it.count(), `is`(0))
        }
    }
}
