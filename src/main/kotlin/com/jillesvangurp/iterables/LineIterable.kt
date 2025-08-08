package com.jillesvangurp.iterables

import com.github.jillesvangurp.common.ResourceUtil
import java.io.BufferedReader
import java.io.Closeable
import java.io.Reader

class LineIterable(private val reader: BufferedReader) : Iterable<String>, Closeable {
    override fun iterator(): Iterator<String> = reader.lineSequence().iterator()
    override fun close() = reader.close()

    companion object {
        @JvmStatic
        fun openGzipFile(path: String): LineIterable = LineIterable(ResourceUtil.gzipFileReader(path))
        operator fun invoke(reader: Reader): LineIterable = LineIterable(reader.buffered())
    }
}
