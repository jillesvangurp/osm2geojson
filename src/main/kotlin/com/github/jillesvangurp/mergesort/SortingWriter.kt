package com.github.jillesvangurp.mergesort

import com.github.jillesvangurp.common.ResourceUtil
import com.github.jillesvangurp.metrics.LoggingCounter
import org.slf4j.LoggerFactory
import java.io.BufferedReader
import java.io.Closeable
import java.io.File
import java.util.PriorityQueue
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.read
import kotlin.concurrent.write

class SortingWriter(
    private val tempDir: String,
    private val output: String,
    private val bucketSize: Int
) : Closeable {
    init {
        if (tempDir.isNotEmpty()) {
            File(tempDir).mkdirs()
        }
    }
    private val log = LoggerFactory.getLogger(SortingWriter::class.java)
    private var currentBucket = 0
    private val bucketFiles = mutableListOf<String>()
    private var bucket: MutableMap<String, MutableList<String>> = sortedMapOf()
    private val bucketLock = ReentrantReadWriteLock()
    private val loggingCounter = LoggingCounter.counter(log, "sort buckets $output", "lines", 100000)

    fun put(key: String, value: String) {
        if (bucket.size >= bucketSize) {
            flushBucket(false)
        }
        bucketLock.read {
            bucket.getOrPut(key) { mutableListOf() }.add(value)
            loggingCounter.inc()
        }
    }

    private fun flushBucket(skipSizeCheck: Boolean) {
        var oldBucket: Map<String, List<String>>? = null
        var bucketNr = -1
        bucketLock.write {
            if (bucket.size > bucketSize || skipSizeCheck) {
                oldBucket = bucket
                bucketNr = currentBucket++
                bucket = sortedMapOf()
            }
        }
        if (oldBucket != null) {
            val file = File(tempDir, "bucket-$bucketNr.gz")
            ResourceUtil.gzipFileWriter(file.absolutePath).use { bw ->
                for ((k, vs) in oldBucket!!) {
                    for (v in vs) {
                        bw.write("$k;$v\n")
                    }
                }
            }
            bucketFiles.add(file.absolutePath)
        }
    }

    override fun close() {
        if (bucket.isNotEmpty()) {
            flushBucket(true)
        }
        loggingCounter.close()
        val mergeCounter = LoggingCounter.counter(log, "merge buckets into $output", " lines", 100000)
        try {
            ResourceUtil.gzipFileWriter(output).use { out ->
                for ((k, v) in mergeBuckets()) {
                    out.write("$k;$v\n")
                    mergeCounter.inc()
                }
            }
        } finally {
            mergeCounter.close()
            bucketFiles.forEach { File(it).delete() }
            File(tempDir).delete()
        }
    }

    private fun mergeBuckets(): Sequence<Pair<String, String>> = sequence {
        data class Source(val reader: BufferedReader, var line: String) : Comparable<Source> {
            val key: String get() = line.substringBefore(';')
            val value: String get() = line.substringAfter(';')
            override fun compareTo(other: Source): Int = key.compareTo(other.key)
        }
        val queue = PriorityQueue<Source>()
        val readers = mutableListOf<BufferedReader>()
        for (file in bucketFiles) {
            val reader = ResourceUtil.gzipFileReader(file)
            val line = reader.readLine()
            if (line != null) {
                readers.add(reader)
                queue.add(Source(reader, line))
            } else {
                reader.close()
            }
        }
        while (queue.isNotEmpty()) {
            val src = queue.poll()
            yield(src.key to src.value)
            val next = src.reader.readLine()
            if (next != null) {
                src.line = next
                queue.add(src)
            } else {
                src.reader.close()
            }
        }
        readers.forEach { runCatching { it.close() } }
    }
}
