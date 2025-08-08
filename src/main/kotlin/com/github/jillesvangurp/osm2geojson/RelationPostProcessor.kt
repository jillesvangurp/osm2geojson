package com.github.jillesvangurp.osm2geojson

import com.github.jillesvangurp.mergesort.EntryParsingProcessor
import com.github.jillesvangurp.common.ResourceUtil
import com.github.jsonj.JsonArray
import com.github.jsonj.JsonObject
import com.github.jsonj.tools.JsonBuilder
import com.github.jsonj.tools.JsonParser
import com.jillesvangurp.iterables.LineIterable
import com.github.jillesvangurp.metrics.LoggingCounter
import org.slf4j.LoggerFactory
import java.io.BufferedWriter
import java.io.Closeable
import java.io.IOException
import java.util.Map.Entry

/**
 * Process joined relation json and emit simple geojson.
 *
 * This focuses on multi polygon relations and avoids holding large
 * intermediate structures in memory by processing relations one by one.
 */
class RelationPostProcessor(private val parser: JsonParser) {
    private val log = LoggerFactory.getLogger(RelationPostProcessor::class.java)
    private val entryParsingProcessor = EntryParsingProcessor()

    var dir: String = "./"
        private set

    fun setDirectory(dir: String): RelationPostProcessor {
        this.dir = if (dir.endsWith("/")) dir else "$dir/"
        return this
    }

    interface JsonWriter : Closeable {
        @Throws(IOException::class)
        fun add(json: JsonObject)
    }

    private fun createJsonWriter(): JsonWriter {
        val location = dir + OSM_RELATIONS_GZ
        return object : JsonWriter {
            private val out: BufferedWriter = ResourceUtil.gzipFileWriter(location)
            override fun add(json: JsonObject) {
                out.append(json.toString()).append('\n')
            }
            override fun close() = out.close()
        }
    }

    fun process() {
        LoggingCounter.counter(log, "process relations", "relations", 1000).use { counter ->
            LineIterable.openGzipFile(dir + OsmJoin.REL_ID_COMPLETE_JSON).use { lineIterable ->
                createJsonWriter().use { writer ->
                    for (line in lineIterable) {
                        val entry = entryParsingProcessor.process(line)
                        val rel = parser.parse(entry.value).asObject()
                        relationToGeoJson(rel)?.let {
                            writer.add(it)
                            counter.inc()
                        }
                    }
                }
            }
        }
    }

    internal fun relationToGeoJson(input: JsonObject): JsonObject? {
        val id = input.getString("id") ?: return null
        val name = input.getString("tags", "name") ?: return null
        val geometry = buildGeometry(input) ?: return null
        return JsonBuilder.`object`()
            .put("id", "osmrelation/$id")
            .put("title", name)
            .put("geometry", geometry)
            .get()
    }

    private fun buildGeometry(rel: JsonObject): JsonObject? {
        val polygons = JsonArray()
        val ways = rel.getArray("ways") ?: return null
        for (way in ways.objects()) {
            val coords = JsonArray()
            val nodes = way.getArray("nodes") ?: continue
            for (node in nodes.objects()) {
                val l = node.getArray("l") ?: continue
                coords.add(l)
            }
            if (coords.size > 0 && coords[0] != coords[coords.size - 1]) {
                coords.add(coords[0])
            }
            val ring = JsonArray()
            ring.add(coords)
            polygons.add(ring)
        }
        return when (polygons.size) {
            0 -> null
            1 -> JsonBuilder.`object`().put("type", "Polygon").put("coordinates", polygons[0]).get()
            else -> JsonBuilder.`object`().put("type", "MultiPolygon").put("coordinates", polygons).get()
        }
    }

    companion object {
        private const val OSM_RELATIONS_GZ = "osm-relations.gz"
    }
}
