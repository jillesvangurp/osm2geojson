package com.github.jillesvangurp.osm2geojson;

import static com.github.jsonj.tools.JsonBuilder.array;
import static com.github.jsonj.tools.JsonBuilder.object;
import static com.github.jsonj.tools.JsonBuilder.primitive;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.xpath.XPathExpressionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import com.github.jillesvangurp.persistentcachingmap.PersistentCachingMap;
import com.github.jsonj.JsonArray;
import com.github.jsonj.JsonObject;
import com.jillesvangurp.iterables.Processor;

/**
 * Processor that converts osm xml blobs to json blobs using regular expressions and stores the json blobs in the
 * right persistent map (one each for node, way, and relation).
 *
 * The maps are used for relation and way types to look up any contained objects (nodes and ways).
 *
 * The processor returns true if it was successful and false otherwise.
 */
public class OsmBlobProcessor implements Processor<String, String> {
    private static final Logger LOG = LoggerFactory.getLogger(OsmBlobProcessor.class);

    final Pattern idPattern = Pattern.compile("id=\"([0-9]+)");
    final Pattern latPattern = Pattern.compile("lat=\"([0-9]+(\\.[0-9]+)?)");
    final Pattern lonPattern = Pattern.compile("lon=\"([0-9]+(\\.[0-9]+)?)");
    final Pattern kvPattern = Pattern.compile("k=\"(.*?)\"\\s+v=\"(.*?)\"");
    final Pattern ndPattern = Pattern.compile("nd ref=\"([0-9]+)");
    final Pattern memberPattern = Pattern.compile("member type=\"(.*?)\" ref=\"([0-9]+)\" role=\"(.*?)\"");

    private final PersistentCachingMap<Long, JsonObject> nodeKv;
    private final PersistentCachingMap<Long, JsonObject> relationKv;
    private final PersistentCachingMap<Long, JsonObject> wayKv;

    OsmBlobProcessor(PersistentCachingMap<Long, JsonObject> nodeKv,
            PersistentCachingMap<Long, JsonObject> relationKv, PersistentCachingMap<Long, JsonObject> wayKv) {
        this.nodeKv = nodeKv;
        this.relationKv = relationKv;
        this.wayKv = wayKv;
    }

    @Override
    public String process(String input) {
        try {
            String blobType;
            if (input.startsWith("<node")) {
                processNode(nodeKv, input);
                blobType="node";
            } else if (input.startsWith("<way")) {
                processWay(nodeKv, wayKv, input);
                blobType="way";
            } else if (input.startsWith("<relation")) {
                processRelation(nodeKv, wayKv, relationKv, input);
                blobType="relation";
            } else {
                throw new IllegalStateException("unexpected node type " + input);
            }
            return blobType;
        } catch (SAXException e) {
            e.printStackTrace();
            return "error";
        } catch (XPathExpressionException e) {
            e.printStackTrace();
            return "error";
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
                // using a more compact notation for points here than the geojson point type. OSM has a billion+ nodes.
                JsonObject node = object().put("id", id).put("l", array(latitude, longitude)).get();
                while (kvm.find()) {
                    String name = kvm.group(1);
                    if (!OsmProcessor.BLACKLISTED_TAGS.contains(name)) {
                        node.put(name, kvm.group(2));
                    }
                }
                nodeKv.put(id, node);
            } else {
                LOG.warn("no lat/lon for " + id);
            }

        } else {
            LOG.warn("no id");
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
                if (!OsmProcessor.BLACKLISTED_TAGS.contains(name)) {
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
                if (!OsmProcessor.BLACKLISTED_TAGS.contains(name)) {
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
                        OsmProcessor.expandPoint(node);
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
}