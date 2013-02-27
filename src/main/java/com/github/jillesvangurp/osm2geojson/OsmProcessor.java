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

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.xpath.XPathExpressionException;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.xml.sax.SAXException;

import com.github.jillesvangurp.persistentcachingmap.PersistentCachingMap;
import com.github.jsonj.JsonArray;
import com.github.jsonj.JsonObject;
import com.github.jsonj.tools.JsonParser;
import com.jillesvangurp.iterables.ConcurrentProcessingIterable;
import com.jillesvangurp.iterables.Iterables;
import com.jillesvangurp.iterables.LineIterable;
import com.jillesvangurp.iterables.Processor;

public class OsmProcessor {
    public static void main(String[] args) throws IOException {
        String osmXml;
        if(args.length>0) {
            osmXml = args[0];
        } else {
            osmXml = "/Users/jilles/data/brandenburg.osm.bz2";
        }
        long now = System.currentTimeMillis();

        JsonObjectCodec codec = new JsonObjectCodec(new JsonParser());
        try (PersistentCachingMap<Long, JsonObject> nodeKv = new PersistentCachingMap<>("osmkv-node", codec, 1000)) {
            try (PersistentCachingMap<Long, JsonObject> wayKv = new PersistentCachingMap<>("osmkv-way", codec, 1000)) {
                try (PersistentCachingMap<Long, JsonObject> relationKv = new PersistentCachingMap<>("osmkv-relation", codec, 500)) {
                    processOsm(nodeKv, wayKv, relationKv, osmXml);
                }
                System.out.println("closed relationKv");
            }
            System.out.println("closed wayKv");
        }
        System.out.println("closed nodeKv");
        System.out.println("Exiting after " + (System.currentTimeMillis() - now) + "ms.");
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
                            JsonObject tags = new JsonObject();
                            while (kvm.find()) {
                                tags.put(kvm.group(1), kvm.group(2));

                            }
                            if (tags.size() > 0) {
                                node.put("t", tags);
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
                        JsonObject tags = new JsonObject();
                        while (kvm.find()) {
                            tags.put(kvm.group(1), kvm.group(2));

                        }
                        if (tags.size() > 0) {
                            way.put("t", tags);
                        }
                        JsonArray coordinates = array();
                        while (ndm.find()) {
                            Long ref = Long.valueOf(ndm.group(1));
                            JsonObject node = nodeKv.get(ref);
                            if (node != null) {
                                coordinates.add(node.getArray("l"));
                            } else {
//                                System.err.println("way " + id + " has illegal node ref " + ref);
                            }
                        }
                        if (coordinates.size() > 1 && coordinates.get(0).equals(coordinates.get(coordinates.size() - 1))) {
                            way.put("location", object().put("type", "Polygon").put("coordinates", array(coordinates)).get());
                        } else {
                            way.put("location", object().put("type", "LineString").put("coordinates", coordinates).get());
                        }
                        wayKv.put(id, way);

                    }
                }

                private void processRelation(PersistentCachingMap<Long, JsonObject> nodeKv, PersistentCachingMap<Long, JsonObject> wayKv,
                        final PersistentCachingMap<Long, JsonObject> relationKv, String input) throws XPathExpressionException, SAXException {
                    Matcher idm = idPattern.matcher(input);
                    Matcher kvm = kvPattern.matcher(input);
                    Matcher mm = memberPattern.matcher(input);

                    if(idm.find()) {
                        long id = Long.valueOf(idm.group(1));
                        JsonObject relation = object().put("id", id).get();
                        JsonObject tags = new JsonObject();
                        while (kvm.find()) {
                            tags.put(kvm.group(1), kvm.group(2));

                        }
                        if (tags.size() > 0) {
                            relation.put("t", tags);
                        }

                        JsonArray members = array();
                        while(mm.find()) {
                            String type=mm.group(1);
                            Long ref=Long.valueOf(mm.group(2));
                            String role=mm.group(3);
                            if ("way".equalsIgnoreCase(type)) {
                                JsonObject way = wayKv.get(ref);
                                if (way != null) {
                                    way.put("type", type);
                                    way.put("role", role);
                                    members.add(way);
                                } else {
//                                    System.err.println("relation " + id + " has illegal way ref " + ref);
                                }
                            } else if ("node".equalsIgnoreCase(type)) {
                                JsonObject node = nodeKv.get(ref);
                                if (node != null) {
                                    node.put("type", type);
                                    node.put("role", role);
                                    members.add(node);
                                } else {
//                                    System.err.println("relation " + id + " has illegal node ref " + ref);
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
            try(ConcurrentProcessingIterable<String, Boolean> concurrentProcessingIterable = Iterables.processConcurrently(osmIterable, processor, 1000, 8, 100)) {
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

    static InputStreamReader bzip2Reader(String fileName) throws IOException, FileNotFoundException {
        return new InputStreamReader(new BZip2CompressorInputStream(new FileInputStream(fileName)), Charset.forName("UTF-8"));
    }
}
