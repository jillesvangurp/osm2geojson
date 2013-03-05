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
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private static final Logger LOG = LoggerFactory.getLogger(OsmProcessor.class);

    private static final String RELATIONS_GZ = "relations.gz";
    private static final String WAYS_GZ = "ways.gz";
    private static final String NODES_GZ = "nodes.gz";
    // these tag names are either depended on by this class or noisy; so simply drop them if we encounter them
    static final Set<String> BLACKLISTED_TAGS = Sets.newTreeSet(Arrays.asList("created_by", "source", "id","type", "latitude", "longitude", "location",
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

                    LOG.info("parse the xml into json and inline any nodes into ways and ways+nodes into relations. Note: processing ways and relations tends to be slower.");
                    processOsm(nodeKv, wayKv, relationKv, osmXml);


                    LOG.info("export json files");
                    exportGeoJson(nodeKv, wayKv, relationKv);

                    LOG.info("post process the exported relations to reconstruct proper geojson multipolygons out of the inner and outer way segments");
                    fixGeometryInRelations(RELATIONS_GZ);
                }
                LOG.info("closed relationKv");
            }
            LOG.info("closed wayKv");
        }
        LOG.info("closed nodeKv");
        LOG.info("Exiting after " + (System.currentTimeMillis() - now) + "ms.");
    }

    private static void processOsm(final PersistentCachingMap<Long, JsonObject> nodeKv, final PersistentCachingMap<Long, JsonObject> wayKv,
            final PersistentCachingMap<Long, JsonObject> relationKv, String osmXml) {

        try (LineIterable lineIterable = new LineIterable(bzip2Reader(osmXml));) {
            OpenStreetMapBlobIterable osmIterable = new OpenStreetMapBlobIterable(lineIterable);

            Processor<String, String> processor = new OsmBlobProcessor(nodeKv, relationKv, wayKv);

            long count = 0;
            long nodes = 0;
            long ways = 0;
            long relations = 0;
            // process the blobs concurrently. You may have to tweak the parameters depending on how many cores & memory you have
            try (ConcurrentProcessingIterable<String, String> concurrentProcessingIterable = Iterables.processConcurrently(osmIterable, processor, 1000, 8,
                    100)) {
                for (String blobType : concurrentProcessingIterable) {


                    if (count % 10000 == 0) {
                        LOG.info("processed " + count + " blobs: " + nodes + " nodes, " + ways + " ways, " + relations + " relations");
                    }
                    if("node".equals(blobType)) {
                        nodes++;
                    } else if("way".equals(blobType)) {
                        ways++;
                    } else if("relation".equals(blobType)) {
                        relations++;
                    }
                    count++;
                }
            }
            LOG.info("done");
        } catch (IOException e) {
            e.printStackTrace();
        }
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
        LOG.info("Exported nodes");
        exportMap(wayKv, new Filter() {

            @Override
            public boolean ok(JsonObject o) {
                return o.get("incomplete") == null && StringUtils.isNotEmpty(o.getString("name")) && o.get("relations") == null;
            }
        }, WAYS_GZ);
        LOG.info("Exported ways");
        exportMap(relationKv, new Filter() {

            @Override
            public boolean ok(JsonObject o) {
                return o.get("incomplete") == null;
            }
        }, RELATIONS_GZ);
        LOG.info("Exported relations");
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

    static void expandPoint(JsonObject o) {
        JsonArray coordinate = o.getArray("l");
        o.put("geometry", object().put("type", "Point").put("coordinates", coordinate).get());
        o.remove("l");
    }

    private static void fixGeometryInRelations(String relationsGz) throws IOException {
        try (BufferedWriter out = gzipFileWriter("relation-multipolygons.gz")) {
            try (LineIterable it = LineIterable.openGzipFile(RELATIONS_GZ)) {
                for (String line : it) {
                    JsonObject relation = parser.parse(line).asObject();
                    String type = relation.getString("type");
                    // only look at multipolygon
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
                            LOG.warn("skipping member: " + e.getMessage() + "\n" + relation.toString());
                        }
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

                // remove the linestring from the members
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
