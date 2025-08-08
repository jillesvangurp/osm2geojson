package com.github.jillesvangurp.osm2geojson

import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.`is`
import org.testng.Assert.assertTrue
import org.testng.annotations.DataProvider
import org.testng.annotations.Test

class OsmJoinTest {
    @DataProvider
    fun sampleNodes(): Array<Array<Any>> = arrayOf(
        arrayOf("<node id=\"25737250\" lat=\"51.5121071\" lon=\"-0.1130375\" timestamp=\"2010-12-10T23:35:50Z\" version=\"3\" changeset=\"6613493\" user=\"Welshie\" uid=\"508\"/>", 51.5121071, -0.1130375),
        arrayOf("<node id=\"25737250\" lat=\"-51.5121071\" lon=\"-0.1130375\" timestamp=\"2010-12-10T23:35:50Z\" version=\"3\" changeset=\"6613493\" user=\"Welshie\" uid=\"508\"/>", -51.5121071, -0.1130375),
        arrayOf("<node id=\"25737250\" lat=\"-51.5121071\" lon=\"0.1130375\" timestamp=\"2010-12-10T23:35:50Z\" version=\"3\" changeset=\"6613493\" user=\"Welshie\" uid=\"508\"/>", -51.5121071, 0.1130375)
    )

    @Test(dataProvider = "sampleNodes")
    fun shouldFindCoordinate(nodexml: String, latitude: Double, longitude: Double) {
        val latMatch = OsmJoin.latPattern.find(nodexml)
        assertTrue(latMatch != null)
        assertThat(latMatch!!.groupValues[1], `is`("$latitude"))
        val lonMatch = OsmJoin.lonPattern.find(nodexml)
        assertTrue(lonMatch != null)
        assertThat(lonMatch!!.groupValues[1], `is`("$longitude"))
    }
}
