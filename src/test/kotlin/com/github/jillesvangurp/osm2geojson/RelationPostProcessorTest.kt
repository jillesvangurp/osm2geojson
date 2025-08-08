package com.github.jillesvangurp.osm2geojson

import io.kotest.matchers.shouldBe
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import org.testng.annotations.Test

class RelationPostProcessorTest {
    private val json = Json

    @Test
    fun shouldConvertRelationToGeoJson() {
        val relationJson = """
            {
              "id": 1,
              "tags": {"name": "Test"},
              "ways": [
                {"nodes": [
                  {"l": [0.0,0.0]},
                  {"l": [1.0,0.0]},
                  {"l": [1.0,1.0]},
                  {"l": [0.0,1.0]},
                  {"l": [0.0,0.0]}
                ]}
              ]
            }
        """.trimIndent()
        val relation = json.parseToJsonElement(relationJson).jsonObject
        val processor = RelationPostProcessor(json)
        val geo = processor.relationToGeoJson(relation)!!
        geo["id"]!!.jsonPrimitive.content shouldBe "osmrelation/1"
        geo["geometry"]!!.jsonObject["type"]!!.jsonPrimitive.content shouldBe "Polygon"
    }
}

