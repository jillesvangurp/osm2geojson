/**
 * Copyright (c) 2013, Jilles van Gurp
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package com.github.jillesvangurp.osm2geojson;

import static com.github.jsonj.tools.JsonBuilder.array;
import static com.github.jsonj.tools.JsonBuilder.object;
import static com.github.jsonj.tools.JsonBuilder.primitive;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import javax.xml.xpath.XPathExpressionException;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.lang.StringUtils;
import org.xml.sax.SAXException;

import com.github.jillesvangurp.persistentcachingmap.PersistentCachingMap;
import com.github.jsonj.JsonArray;
import com.github.jsonj.JsonElement;
import com.github.jsonj.JsonObject;
import com.github.jsonj.tools.JsonParser;
import com.github.jsonj.tools.TypeSupport;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.jillesvangurp.geo.GeoGeometry;
import com.jillesvangurp.iterables.ConcurrentProcessingIterable;
import com.jillesvangurp.iterables.Iterables;
import com.jillesvangurp.iterables.LineIterable;
import com.jillesvangurp.iterables.Processor;

public class OsmProcessor {
    private static final String RELATIONS_GZ = "relations.gz";
    private static final String WAYS_GZ = "ways.gz";
    private static final String NODES_GZ = "nodes.gz";
    // these tag names are either depended on by this class or noisy; so simply drop them if we encounter them
    private static final Set<String> BLACKLISTED_TAGS = Sets.newTreeSet(Arrays.asList("created_by", "source", "id","type", "latitude", "longitude", "location",
            "members", "ways", "relations", "l","geometry","role"));

    private static final JsonParser parser = new JsonParser();

    public static void main(String[] args) throws IOException {
        String osmXml;
        if (args.length > 0) {
            osmXml = args[0];
        } else {
            osmXml = "/Users/jilles/data/brandenburg.osm.bz2";
        }
        long now = System.currentTimeMillis();

        JsonObjectCodec codec = new JsonObjectCodec(new JsonParser());
        try (PersistentCachingMap<Long, JsonObject> nodeKv = new PersistentCachingMap<>("osmkv-node", codec, 1000)) {
            try (PersistentCachingMap<Long, JsonObject> wayKv = new PersistentCachingMap<>("osmkv-way", codec, 1000)) {
                try (PersistentCachingMap<Long, JsonObject> relationKv = new PersistentCachingMap<>("osmkv-relation", codec, 1000)) {
//                    processOsm(nodeKv, wayKv, relationKv, osmXml);
//                    exportGeoJson(nodeKv, wayKv, relationKv);
                    fixGeometryInRelations(RELATIONS_GZ);

                    // filterWays();
                }
                System.out.println("closed relationKv");
            }
            System.out.println("closed wayKv");
        }
        System.out.println("closed nodeKv");
        System.out.println("Exiting after " + (System.currentTimeMillis() - now) + "ms.");
    }

    private static void fixGeometryInRelations(String relationsGz) throws IOException {
        try (BufferedWriter out = gzipFileWriter("relation-fixed.gz")) {
            try (LineIterable it = LineIterable.openGzipFile(RELATIONS_GZ)) {
                for (String line : it) {
                    JsonObject relation = parser.parse(line).asObject();
                    String type = relation.getString("type");
                    if ("multipolygon".equalsIgnoreCase(type)) {
                        try {
                            JsonArray multiPolygon = calculateMultiPolygonForRelation(relation);
                            relation.put("geometry", object().put("type", "MultiPolygon").put("coordinates", multiPolygon).get());
                            if(relation.getArray("members").size() == 0) {
                                relation.remove("members");
                            }
                            out.write(relation.toString());
                            out.newLine();
                        } catch (Exception e) {
                            System.err.println("skipping member: " + e.getMessage() + "\n" + relation.toString());
                        }
                    } else {
                        System.err.println("unsupported relation type " + type);
                    }
                }
            }
        }
    }

    static JsonArray calculateMultiPolygonForRelation(JsonObject relation) {
        // only call on multipolygon relations
        JsonArray members = relation.getArray("members");
        JsonArray multiPolygon = array();

        Map<String,JsonObject> lineStrings = Maps.newHashMap();

        int nrOfTags=0;
        for(Entry<String, JsonElement> e: relation.entrySet()) {
            if(!BLACKLISTED_TAGS.contains(e.getKey())) {
                nrOfTags++;
            }

        }

        // collect the LineStrings that need to be connected
        Iterator<JsonElement> iterator = members.iterator();
        while (iterator.hasNext()) {
            JsonObject member = iterator.next().asObject();

            String role = member.getString("role");
            // copy outer tags to relation if missing
            if(nrOfTags == 0 && "outer".equalsIgnoreCase(role)) {
                for(Entry<String, JsonElement> e: member.entrySet()) {
                    String key = e.getKey();
                    if(!BLACKLISTED_TAGS.contains(key) && !relation.containsKey(key)) {
                        relation.put(key, e.getValue());
                    }
                }
            }


            JsonObject geometry = member.getObject("geometry");
            String geometryType = geometry.getString("type");
            if("LineString".equalsIgnoreCase(geometryType)) {
                JsonArray coordinates = geometry.getArray("coordinates");
                String first = coordinates.first().toString();
                lineStrings.put(first, member);
                iterator.remove();
            }
        }

        Set<JsonObject> connectedSegments = Sets.newHashSet();
        // connect the segments
        while (lineStrings.size() > 0) {
            // take the first segment
            Entry<String, JsonObject> entry = lineStrings.entrySet().iterator().next();
            JsonObject member = entry.getValue();
            // remove it from the map
            String first = entry.getKey();
            lineStrings.remove(first);
            JsonObject geometry = member.getObject("geometry");
            JsonArray coordinates = geometry.getArray("coordinates");
            String last = coordinates.last().toString();
            if(first.equals(last)) {
                // already a polygon
            } else {
                JsonObject segment;
                // find the segments that can be connected
                while((segment = lineStrings.get(last)) != null) {
                    JsonArray segmentCoordinates = segment.getArray("geometry","coordinates");
                    segmentCoordinates.remove(0);
                    coordinates.addAll(segmentCoordinates);
                    // remove the segment from the map
                    lineStrings.remove(last);
                    last=coordinates.last().toString();
                }
            }
            connectedSegments.add(member);
        }

        // add the connected segments as polygon way members
        for (JsonObject member: connectedSegments) {
            JsonObject geometry = member.getObject("geometry");
            JsonArray coordinates = geometry.getArray("coordinates");
            geometry.put("type", "Polygon");
            // polygons are 3d arrays
            JsonArray newCoordinates = array();
            newCoordinates.add(coordinates);
            JsonArray firstPoly = newCoordinates.first().asArray();
            JsonElement firstCoordinate = firstPoly.first();
            JsonElement lastCoordinate = firstPoly.last();
            if(!firstCoordinate.equals(lastCoordinate)) {
                // close the polygon
                firstPoly.add(firstCoordinate);
            }
            geometry.put("coordinates", newCoordinates);
            // add the member
            members.add(member);
        }

        // add all the outer polygons to the multipolygon
        Iterator<JsonElement> it = members.iterator();
        while (it.hasNext()) {
            JsonObject member = it.next().asObject();
            String role = member.getString("role");
            if("outer".equals(role)) {
                multiPolygon.add(member.getArray("geometry","coordinates"));
                it.remove();
            }
        }
        // now add the inner polygons to the right outerpolygon in the multipolygon
        it = members.iterator();
        while (it.hasNext()) {
            JsonObject member = it.next().asObject();
            String role = member.getString("role");
            if("inner".equals(role)) {
                JsonArray coordinates = member.getArray("geometry","coordinates");
                findOuterPolygon(multiPolygon,coordinates);
                it.remove();
            }
        }

        return multiPolygon;
    }


    private static void findOuterPolygon(JsonArray multiPolygon, JsonArray inner) {
        for(JsonElement p: multiPolygon) {
            JsonArray polygon = p.asArray();
            JsonArray outer = polygon.first().asArray();
            JsonElement innerPolygon = inner.first();
            double[] polygonCenter = GeoGeometry.getPolygonCenter(TypeSupport.convertTo2DDoubleArray(innerPolygon.asArray()));
            if(GeoGeometry.polygonContains(polygonCenter, TypeSupport.convertTo2DDoubleArray(outer))) {
                polygon.add(innerPolygon);
            }
        }
    }

    private static void expandPoint(JsonObject o) {
        JsonArray coordinate = o.getArray("l");
        o.put("geometry", object().put("type", "Point").put("coordinates", coordinate).get());
        o.remove("l");
    }

    private static void exportGeoJson(PersistentCachingMap<Long, JsonObject> nodeKv, PersistentCachingMap<Long, JsonObject> wayKv,
            PersistentCachingMap<Long, JsonObject> relationKv) throws IOException {
        exportMap(nodeKv, new Filter() {

            @Override
            public boolean ok(JsonObject o) {
                expandPoint(o);
                return StringUtils.isNotEmpty(o.getString("name"));
            }
        }, NODES_GZ);
        System.out.println("Exported nodes");
        exportMap(wayKv, new Filter() {

            @Override
            public boolean ok(JsonObject o) {
                return o.get("incomplete") == null && StringUtils.isNotEmpty(o.getString("name")) && o.get("relations") == null;
            }
        }, WAYS_GZ);
        System.out.println("Exported ways");
        exportMap(relationKv, new Filter() {

            @Override
            public boolean ok(JsonObject o) {
                return o.get("incomplete") == null;
            }
        }, RELATIONS_GZ);
        System.out.println("Exported relations");
    }

    interface Filter {
        boolean ok(JsonObject o);
    }

    private static void exportMap(PersistentCachingMap<Long, JsonObject> map, Filter f, String file) throws IOException {
        try (BufferedWriter out = gzipFileWriter(file)) {
            for (Entry<Long, JsonObject> entry : map) {
                JsonObject object = entry.getValue();
                if (f.ok(object)) {
                    out.write(object.toString());
                    out.newLine();
                }
            }
        }
    }

    private static void processOsm(final PersistentCachingMap<Long, JsonObject> nodeKv, final PersistentCachingMap<Long, JsonObject> wayKv,
            final PersistentCachingMap<Long, JsonObject> relationKv, String osmXml) {
        final Pattern idPattern = Pattern.compile("id=\"([0-9]+)");
        final Pattern latPattern = Pattern.compile("lat=\"([0-9]+(\\.[0-9]+)?)");
        final Pattern lonPattern = Pattern.compile("lon=\"([0-9]+(\\.[0-9]+)?)");
        final Pattern kvPattern = Pattern.compile("k=\"(.*?)\"\\s+v=\"(.*?)\"");
        final Pattern ndPattern = Pattern.compile("nd ref=\"([0-9]+)");
        final Pattern memberPattern = Pattern.compile("member type=\"(.*?)\" ref=\"([0-9]+)\" role=\"(.*?)\"");

        try (LineIterable lineIterable = new LineIterable(bzip2Reader(osmXml));) {
            OpenStreetMapBlobIterable osmIterable = new OpenStreetMapBlobIterable(lineIterable);

            Processor<String, Boolean> processor = new Processor<String, Boolean>() {

                @Override
                public Boolean process(String input) {
                    try {
                        if (input.startsWith("<node")) {
                            processNode(nodeKv, input);
                        } else if (input.startsWith("<way")) {
                            processWay(nodeKv, wayKv, input);
                        } else if (input.startsWith("<relation")) {
                            processRelation(nodeKv, wayKv, relationKv, input);
                        } else {
                            throw new IllegalStateException("unexpected node type " + input);
                        }
                        return true;
                    } catch (SAXException e) {
                        e.printStackTrace();
                        return false;
                    } catch (XPathExpressionException e) {
                        e.printStackTrace();
                        return false;
                    }
                }

                private void processNode(final PersistentCachingMap<Long, JsonObject> nodeKv, String input) throws XPathExpressionException, SAXException {
                    Matcher idm = idPattern.matcher(input);
                    Matcher latm = latPattern.matcher(input);
                    Matcher lonm = lonPattern.matcher(input);
                    Matcher kvm = kvPattern.matcher(input);
                    if (idm.find()) {
                        long id = Long.valueOf(idm.group(1));
                        if (latm.find() && lonm.find()) {
                            double latitude = Double.valueOf(latm.group(1));
                            double longitude = Double.valueOf(lonm.group(1));
                            JsonObject node = object().put("id", id).put("l", array(latitude, longitude)).get();
                            while (kvm.find()) {
                                String name = kvm.group(1);
                                if (!BLACKLISTED_TAGS.contains(name)) {
                                    node.put(name, kvm.group(2));
                                }
                            }
                            nodeKv.put(id, node);
                        } else {
                            System.err.println("no lat/lon for " + id);
                        }

                    } else {
                        System.err.println("no id");
                    }
                }

                private void processWay(PersistentCachingMap<Long, JsonObject> nodeKv, PersistentCachingMap<Long, JsonObject> wayKv, String input)
                        throws XPathExpressionException, SAXException {

                    Matcher idm = idPattern.matcher(input);
                    Matcher kvm = kvPattern.matcher(input);
                    Matcher ndm = ndPattern.matcher(input);

                    if (idm.find()) {
                        long id = Long.valueOf(idm.group(1));
                        JsonObject way = object().put("id", id).get();
                        while (kvm.find()) {
                            String name = kvm.group(1);
                            if (!BLACKLISTED_TAGS.contains(name)) {
                                way.put(name, kvm.group(2));
                            }
                        }
                        JsonArray coordinates = array();
                        while (ndm.find()) {
                            Long ref = Long.valueOf(ndm.group(1));
                            JsonObject node = nodeKv.get(ref);
                            if (node != null) {
                                JsonObject clone = node.deepClone();
                                clone.getOrCreateArray("ways").add(primitive(id));
                                // store a deep clone with a ref to the relation
                                nodeKv.put(ref, clone);

                                coordinates.add(node.getArray("l"));
                            } else {
                                way.put("incomplete", true);
                            }
                        }
                        if (coordinates.size() > 1 && coordinates.get(0).equals(coordinates.get(coordinates.size() - 1))) {
                            // geojson polygon is 3d array of primary polygon + holes
                            JsonArray polygon = array();
                            polygon.add(coordinates);
                            way.put("geometry", object().put("type", "Polygon").put("coordinates", polygon).get());
                        } else {
                            // geojson linestring is 2d array of points
                            way.put("geometry", object().put("type", "LineString").put("coordinates", coordinates).get());
                        }
                        wayKv.put(id, way);

                    }
                }

                private void processRelation(PersistentCachingMap<Long, JsonObject> nodeKv, PersistentCachingMap<Long, JsonObject> wayKv,
                        final PersistentCachingMap<Long, JsonObject> relationKv, String input) throws XPathExpressionException, SAXException {
                    Matcher idm = idPattern.matcher(input);
                    Matcher kvm = kvPattern.matcher(input);
                    Matcher mm = memberPattern.matcher(input);

                    if (idm.find()) {
                        long id = Long.valueOf(idm.group(1));
                        JsonObject relation = object().put("id", id).get();
                        while (kvm.find()) {
                            String name = kvm.group(1);
                            if (!BLACKLISTED_TAGS.contains(name)) {
                                relation.put(name, kvm.group(2));
                            }
                        }

                        JsonArray members = array();
                        while (mm.find()) {
                            String type = mm.group(1);
                            Long ref = Long.valueOf(mm.group(2));
                            String role = mm.group(3);
                            if ("way".equalsIgnoreCase(type)) {
                                JsonObject way = wayKv.get(ref);
                                if (way != null) {
                                    JsonObject clone = way.deepClone();
                                    clone.getOrCreateArray("relations").add(primitive(id));
                                    // store a deep clone with a ref to the relation
                                    wayKv.put(ref, clone);
                                    way.put("type", type);
                                    way.put("role", role);
                                    members.add(way);
                                } else {
                                    relation.put("incomplete", true);
                                }
                            } else if ("node".equalsIgnoreCase(type)) {
                                JsonObject node = nodeKv.get(ref);
                                if (node != null) {
                                    JsonObject clone = node.deepClone();
                                    clone.getOrCreateArray("relations").add(primitive(id));
                                    // store a deep clone with a ref to the relation
                                    nodeKv.put(ref, clone);
                                    node.put("type", type);
                                    node.put("role", role);
                                    expandPoint(node);
                                    members.add(node);
                                } else {
                                    relation.put("incomplete", true);
                                }
                            }
                        }
                        if (members.size() > 0) {
                            relation.put("members", members);
                        }
                        relationKv.put(id, relation);
                    }
                }
            };

            long count = 0;
            try (ConcurrentProcessingIterable<String, Boolean> concurrentProcessingIterable = Iterables.processConcurrently(osmIterable, processor, 1000, 8,
                    100)) {
                for (@SuppressWarnings("unused")
                Boolean ok : concurrentProcessingIterable) {
                    // do nothing
                    if (count % 10000 == 0) {
                        System.out.println("processed " + count);
                    }
                    count++;
                }
            }
            System.out.println("done");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static InputStreamReader bzip2Reader(String fileName) throws IOException, FileNotFoundException {
        return new InputStreamReader(new BZip2CompressorInputStream(new FileInputStream(fileName)), Charset.forName("UTF-8"));
    }

    public static BufferedWriter gzipFileWriter(String file) throws IOException {
        return new BufferedWriter(new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(file)), Charset.forName("utf-8")));
    }

    public static BufferedReader gzipFileReader(File file) throws IOException {
        return new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(file)), Charset.forName("utf-8")));
    }

}
