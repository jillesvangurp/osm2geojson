package com.github.jillesvangurp.osm2geojson;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.testng.Assert.assertTrue;

import java.util.regex.Matcher;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test
public class OsmJoinTest {

    @DataProvider
    public Object[][] sampleNodes() {
        return new Object[][] {
                {"<node id=\"25737250\" lat=\"51.5121071\" lon=\"-0.1130375\" timestamp=\"2010-12-10T23:35:50Z\" version=\"3\" changeset=\"6613493\" user=\"Welshie\" uid=\"508\"/>", 51.5121071,-0.1130375},
                {"<node id=\"25737250\" lat=\"-51.5121071\" lon=\"-0.1130375\" timestamp=\"2010-12-10T23:35:50Z\" version=\"3\" changeset=\"6613493\" user=\"Welshie\" uid=\"508\"/>", -51.5121071,-0.1130375},
                {"<node id=\"25737250\" lat=\"-51.5121071\" lon=\"0.1130375\" timestamp=\"2010-12-10T23:35:50Z\" version=\"3\" changeset=\"6613493\" user=\"Welshie\" uid=\"508\"/>", -51.5121071,0.1130375}
        };
    }

    @Test(dataProvider="sampleNodes")
    public void shouldFindCoordinate(String nodexml, double latitude, double longitude) {
        Matcher latMatcher = OsmJoin.latPattern.matcher(nodexml);
        assertTrue(latMatcher.find());
        assertThat(latMatcher.group(1), is(""+latitude));
        Matcher lonMatcher = OsmJoin.lonPattern.matcher(nodexml);
        assertTrue(lonMatcher.find());
        assertThat(lonMatcher.group(1), is(""+longitude));
    }
}
