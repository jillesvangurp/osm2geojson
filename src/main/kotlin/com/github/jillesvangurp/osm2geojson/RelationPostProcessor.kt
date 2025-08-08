package com.github.jillesvangurp.osm2geojson

import com.github.jillesvangurp.common.ResourceUtil
import com.github.jillesvangurp.mergesort.EntryParsingProcessor
import com.github.jillesvangurp.metrics.LoggingCounter
import com.jillesvangurp.iterables.LineIterable
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonArray
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import org.slf4j.LoggerFactory
import java.io.BufferedWriter
import java.io.Closeable
import java.io.IOException

/**
 * Process joined relation json and emit simple geojson.
 *
 * This focuses on multi polygon relations and avoids holding large
 * intermediate structures in memory by processing relations one by one.
 */
class RelationPostProcessor(private val json: Json = Json) {
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
                        val rel = json.parseToJsonElement(entry.value).jsonObject
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
        val id = input["id"]?.jsonPrimitive?.content ?: return null
        val name = input["tags"]?.jsonObject?.get("name")?.jsonPrimitive?.content ?: return null
        val geometry = buildGeometry(input) ?: return null
        return buildJsonObject {
            put("id", JsonPrimitive("osmrelation/$id"))
            put("title", JsonPrimitive(name))
            put("geometry", geometry)
        }
    }

    private fun buildGeometry(rel: JsonObject): JsonObject? {
        val polygons = mutableListOf<JsonElement>()
        val ways = rel["ways"]?.jsonArray ?: return null
        for (wayElement in ways) {
            val way = wayElement.jsonObject
            val coords = mutableListOf<JsonElement>()
            val nodes = way["nodes"]?.jsonArray ?: continue
            for (nodeElement in nodes) {
                val l = nodeElement.jsonObject["l"]?.jsonArray ?: continue
                coords.add(l)
            }
            if (coords.isNotEmpty() && coords.first() != coords.last()) {
                coords.add(coords.first())
            }
            val ring = buildJsonArray {
                add(buildJsonArray {
                    coords.forEach { add(it) }
                })
            }
            polygons.add(ring)
        }
        return when (polygons.size) {
            0 -> null
            1 -> buildJsonObject {
                put("type", JsonPrimitive("Polygon"))
                put("coordinates", polygons[0])
            }
            else -> buildJsonObject {
                put("type", JsonPrimitive("MultiPolygon"))
                put("coordinates", buildJsonArray {
                    polygons.forEach { add(it) }
                })
            }
        }
    }

    companion object {
        private const val OSM_RELATIONS_GZ = "osm-relations.gz"
    }
}

