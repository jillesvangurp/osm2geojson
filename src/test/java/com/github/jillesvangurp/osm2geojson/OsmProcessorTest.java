package com.github.jillesvangurp.osm2geojson;

import static com.github.jsonj.tools.JsonBuilder.array;
import static com.github.jsonj.tools.JsonBuilder.object;
import static com.github.jsonj.tools.JsonBuilder.primitive;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.util.Arrays;

import org.apache.commons.lang.StringUtils;
import org.testng.annotations.Test;

import com.github.jsonj.JsonArray;
import com.github.jsonj.JsonObject;
import com.jillesvangurp.geo.GeoGeometry;

@Test
public class OsmProcessorTest {

    public void test() {
        JsonObject way1 = TestFixture.lineStringWay(TestFixture.poly1segment1, null);
        JsonObject way2 = TestFixture.lineStringWay(TestFixture.poly1segment2, null);
        JsonObject way3 = TestFixture.lineStringWay(TestFixture.poly2segment1, null);
        JsonObject way4 = TestFixture.lineStringWay(TestFixture.poly2segment2, null);
        JsonObject way5 = TestFixture.lineStringWay(TestFixture.poly3segment1, null);
        JsonObject way6 = TestFixture.lineStringWay(TestFixture.poly3segment2, null);
        JsonObject way7 = TestFixture.lineStringWay(TestFixture.poly3segment3, null);
        JsonObject way8 = TestFixture.lineStringWay(TestFixture.poly3segment4, null);

        JsonObject r = object()
                .put("type", "multipolygon")
                .put("members", array(
                        TestFixture.member(way1, "way", "outer"),
                        TestFixture.member(way2, "way", "outer"),
                        TestFixture.member(way3, "way", "inner"),
                        TestFixture.member(way4, "way", "inner"),
                        TestFixture.member(way5, "way", "outer"),
                        TestFixture.member(way6, "way", "outer"),
                        TestFixture.member(way7, "way", "outer"),
                        TestFixture.member(way8, "way", "outer")
                        ))
                .get();

        JsonArray multiPolygon = OsmProcessor.calculateMultiPolygonForRelation(r);
        assertThat(multiPolygon.size(), is(2));
    }

    public void shouldConvertRelationWithOneOuterWayPolygon() {
        JsonObject way = TestFixture.polygonWay(TestFixture.poly1, null);

        JsonObject r = object()
                .put("type", "multipolygon")
                .put("members", array(
                            TestFixture.member(way, "way", "outer")
                        ))
                .get();

        JsonArray multiPolygon = OsmProcessor.calculateMultiPolygonForRelation(r);
        assertThat(multiPolygon.get(0).asArray().get(0).asArray(), is(TestFixture.convertDoubleArray(TestFixture.poly1)));
    }

    public void shouldConvertRelationWithOneOuterWayPolygonWithHole() {
        JsonObject way1 = TestFixture.polygonWay(TestFixture.poly1, null);
        JsonObject way2 = TestFixture.polygonWay(TestFixture.poly2, null);

        JsonObject r = object()
                .put("type", "multipolygon")
                .put("members", array(
                        TestFixture.member(way1, "way", "outer"),
                        TestFixture.member(way2, "way", "inner")
                        ))
                .get();

        JsonArray multiPolygon = OsmProcessor.calculateMultiPolygonForRelation(r);
        assertThat(multiPolygon.get(0).asArray().get(0).asArray(), is(TestFixture.convertDoubleArray(TestFixture.poly1)));
        assertThat(multiPolygon.get(0).asArray().get(1).asArray(), is(TestFixture.convertDoubleArray(TestFixture.poly2)));
    }


    public void shouldConvertRelationWithTwoOuterLineStrings() {
        JsonObject way1 = TestFixture.lineStringWay(TestFixture.poly1segment1, null);
        JsonObject way2 = TestFixture.lineStringWay(TestFixture.poly1segment2, null);

        JsonObject r = object()
                .put("type", "multipolygon")
                .put("members", array(
                        TestFixture.member(way1, "way", "outer"),
                        TestFixture.member(way2, "way", "outer")
                        ))
                .get();

        JsonArray multiPolygon = OsmProcessor.calculateMultiPolygonForRelation(r);
        assertThat(multiPolygon.get(0).asArray().get(0).asArray(), is(TestFixture.convertDoubleArray(TestFixture.poly1)));
    }

    public void shouldConvertRelationWithTwoOuterAndInnerLineStrings() {
        JsonObject way1 = TestFixture.lineStringWay(TestFixture.poly1segment1, null);
        JsonObject way2 = TestFixture.lineStringWay(TestFixture.poly1segment2, null);
        JsonObject way3 = TestFixture.lineStringWay(TestFixture.poly2segment1, null);
        JsonObject way4 = TestFixture.lineStringWay(TestFixture.poly2segment2, null);

        JsonObject r = object()
                .put("type", "multipolygon")
                .put("members", array(
                        TestFixture.member(way1, "way", "outer"),
                        TestFixture.member(way2, "way", "outer"),
                        TestFixture.member(way3, "way", "inner"),
                        TestFixture.member(way4, "way", "inner")
                        ))
                .get();

        JsonArray multiPolygon = OsmProcessor.calculateMultiPolygonForRelation(r);
        assertThat(multiPolygon.get(0).asArray().get(0).asArray(), is(TestFixture.convertDoubleArray(TestFixture.poly1)));
        assertThat(multiPolygon.get(0).asArray().get(1).asArray(), is(TestFixture.convertDoubleArray(TestFixture.poly2)));
    }

    static class TestFixture {
        private static JsonArray convertDoubleArray(double[][] coordinates) {
            JsonArray points = array();
            for (int i=0;i<coordinates.length ; i++) {
                double[] values = coordinates[i];
                JsonArray subArray = array();
                for (int j = 0; j < values.length; j++) {
                    subArray.add(primitive(values[j]));
                }
                points.add(subArray);
            }
            return points;
        }

        static double[][] poly1 = new double[][] {{1.0,1.0}, {1.0,2.0}, {2.0,2.0},{2.0,1.0},{1.0,1.0}};
        static double[][] poly1segment1=Arrays.copyOfRange(poly1, 0, 3);
        static double[][] poly1segment2=Arrays.copyOfRange(poly1, 2, poly1.length);

        static double[][] poly2 = new double[][] {{1.1,1.1}, {1.1,1.9}, {1.9,1.9},{1.9,1.1},{1.1,1.1}};
        static double[][] poly2segment1=Arrays.copyOfRange(poly2, 0, 4);
        static double[][] poly2segment2=Arrays.copyOfRange(poly2, 3, poly2.length);

        static double[][] poly3 = GeoGeometry.circle2polygon(12, 52, 13, 1000);
        static double[][] poly3segment1=Arrays.copyOfRange(poly3, 0, 4);
        static double[][] poly3segment2=Arrays.copyOfRange(poly3, 3, 7);
        static double[][] poly3segment3=Arrays.copyOfRange(poly3, 6, 10);
        static double[][] poly3segment4=Arrays.copyOfRange(poly3, 9, poly3.length);

        static JsonObject polygonWay(double[][] twoDPolygon, JsonObject tags) {
            JsonObject way;
            if(tags == null) {
                way = new JsonObject();
            } else {
                way = tags.deepClone();
            }
            JsonArray polygon = array();
            polygon.add(convertDoubleArray(twoDPolygon));
            way.put("geometry", object().put("type","Polygon").put("coordinates", polygon).get());

            return way;
        }

        static JsonObject lineStringWay(double[][] twoDPolygon, JsonObject tags) {
            JsonObject way;
            if(tags == null) {
                way = new JsonObject();
            } else {
                way = tags.deepClone();
            }
            way.put("geometry", object().put("type","LineString").put("coordinates", convertDoubleArray(twoDPolygon)).get());

            return way;
        }

        static JsonObject member(JsonObject element, String type, String role) {
            JsonObject member = element.deepClone();
            member.put("type", type);
            if(StringUtils.isNotEmpty(role)) {
                member.put("role", role);
            }
            return member;
        }
    }
}
