package com.github.jillesvangurp.osm2geojson

import com.jillesvangurp.iterables.LineIterable

class OsmBlobIterable(private val lineIterable: LineIterable) : Iterable<String> {
    override fun iterator(): Iterator<String> {
        val it = lineIterable.iterator()
        return object : Iterator<String> {
            var next: String? = null
            val buf = StringBuilder()

            override fun hasNext(): Boolean {
                if (next != null) return true
                while (it.hasNext()) {
                    var line = it.next()
                    if (line.isNotEmpty()) {
                        val stripped = line.trim()
                        when {
                            stripped.startsWith("<node") -> {
                                buf.setLength(0)
                                buf.append(line)
                                if (!stripped.endsWith("/>") ) {
                                    while (it.hasNext()) {
                                        line = it.next()
                                        buf.append(line)
                                        if (line.trim().endsWith("</node>")) break
                                    }
                                }
                                next = buf.toString().trim()
                                return true
                            }
                            stripped.startsWith("<way") -> {
                                buf.setLength(0)
                                buf.append(line)
                                if (!stripped.endsWith("/>") ) {
                                    while (it.hasNext()) {
                                        line = it.next()
                                        buf.append(line)
                                        if (line.trim().endsWith("</way>")) break
                                    }
                                }
                                next = buf.toString().trim()
                                return true
                            }
                            stripped.startsWith("<relation") -> {
                                buf.setLength(0)
                                buf.append(line)
                                if (!stripped.endsWith("/>") ) {
                                    while (it.hasNext()) {
                                        line = it.next()
                                        buf.append(line)
                                        if (line.trim().endsWith("</relation>")) break
                                    }
                                }
                                next = buf.toString().trim()
                                return true
                            }
                        }
                    }
                }
                return false
            }

            override fun next(): String {
                if (!hasNext()) throw NoSuchElementException()
                val result = next!!
                next = null
                return result
            }
        }
    }
}
