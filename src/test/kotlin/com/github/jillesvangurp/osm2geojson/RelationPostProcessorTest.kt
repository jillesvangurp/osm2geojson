package com.github.jillesvangurp.osm2geojson

import com.github.jsonj.tools.JsonParser
import io.kotest.matchers.shouldBe
import org.testng.annotations.Test

class RelationPostProcessorTest {
    private val parser = JsonParser()

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
        val relation = parser.parse(relationJson).asObject()
        val processor = RelationPostProcessor(parser)
        val geo = processor.relationToGeoJson(relation)!!
        geo.getString("id") shouldBe "osmrelation/1"
        geo.getObject("geometry").getString("type") shouldBe "Polygon"
    }
}
