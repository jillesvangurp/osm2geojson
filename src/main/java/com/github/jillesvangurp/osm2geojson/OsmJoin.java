package com.github.jillesvangurp.osm2geojson;

import static com.github.jsonj.tools.JsonBuilder.array;
import static com.github.jsonj.tools.JsonBuilder.object;
import static com.github.jsonj.tools.JsonBuilder.primitive;
import static com.jillesvangurp.iterables.Iterables.consume;
import static com.jillesvangurp.iterables.Iterables.map;
import static com.jillesvangurp.iterables.Iterables.processConcurrently;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.xpath.XPathExpressionException;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import com.github.jillesvangurp.common.ResourceUtil;
import com.github.jillesvangurp.mergesort.SortingWriter;
import com.github.jillesvangurp.metrics.StopWatch;
import com.github.jillesvangurp.osm2geojson.EntryJoiningIterable.JoinedEntries;
import com.github.jsonj.JsonArray;
import com.github.jsonj.JsonObject;
import com.github.jsonj.tools.JsonParser;
import com.jillesvangurp.iterables.ConcurrentProcessingIterable;
import com.jillesvangurp.iterables.LineIterable;
import com.jillesvangurp.iterables.PeekableIterator;
import com.jillesvangurp.iterables.Processor;

public class OsmJoin {
    private static final Logger LOG = LoggerFactory.getLogger(OsmJoin.class);

    public static final String NODE_ID_NODEJSON_MAP = "nodeid2rawnodejson.gz";
    private static final String REL_ID_RELJSON_MAP = "relid2rawreljson.gz";
    private static final String WAY_ID_WAYJSON_MAP = "wayid2rawwayjson.gz";
    private static final String NODE_ID_WAY_ID_MAP = "nodeid2wayid.gz";
    private static final String NODE_ID_REL_ID_MAP = "nodeid2relid.gz";
    private static final String WAY_ID_REL_ID_MAP = "wayid2relid.gz";

    private static final String WAY_ID_NODE_JSON_MAP = "wayid2nodejson.gz";

    public static final String WAY_ID_COMPLETE_JSON = "wayid2completejson.gz";

    private static final String REL_ID_NODE_JSON_MAP = "relid2nodejson.gz";
    private static final String REL_ID_JSON_WITH_NODES = "relid2jsonwithnodes.gz";

    private static final String REL_ID_WAY_JSON_MAP = "relid2wayjson.gz";
    public static final String REL_ID_COMPLETE_JSON = "relid2completejson.gz";


    // choose a bucket size that will fit in memory. Larger means less bucket files and more ram are used.
    private static final int BUCKET_SIZE = 500000;

    static final Pattern idPattern = Pattern.compile("id=\"([0-9]+)");
    static final Pattern latPattern = Pattern.compile("lat=\"(-?[0-9]+(\\.[0-9]+)?)");
    static final Pattern lonPattern = Pattern.compile("lon=\"(-?[0-9]+(\\.[0-9]+)?)");
    static final Pattern kvPattern = Pattern.compile("k=\"(.*?)\"\\s+v=\"(.*?)\"");
    static final Pattern ndPattern = Pattern.compile("nd ref=\"([0-9]+)");
    static final Pattern memberPattern = Pattern.compile("member type=\"(.*?)\" ref=\"([0-9]+)\" role=\"(.*?)\"");

//    private static final String OSM_XML = "/Users/jilles/data/brandenburg.osm.bz2";

    private final String workDirectory;

    private final JsonParser parser;

    public OsmJoin(String workDirectory, JsonParser parser) {
        this.workDirectory = workDirectory;
        this.parser = parser;
        try {
            FileUtils.forceMkdir(new File(workDirectory));
        } catch (IOException e) {
            throw new IllegalStateException("cannot create dir " + workDirectory);
        }
    }

    private String bucketDir(String file) {
        return workDirectory + File.separatorChar + file + ".buckets";
    }

    private SortingWriter sortingWriter(String file, int bucketSize) {
        try {
            return new SortingWriter(bucketDir(file), file, bucketSize);
        } catch (IOException e) {
            throw new IllegalStateException("cannot create sorting writer " + file);
        }
    }

    public void splitAndEmit(String osmFile) {

        // create various sorted maps that need to be joined in the next steps

        try (SortingWriter nodesWriter = sortingWriter(NODE_ID_NODEJSON_MAP, BUCKET_SIZE)) {
            try (SortingWriter nodeid2WayidWriter = sortingWriter(NODE_ID_WAY_ID_MAP, BUCKET_SIZE)) {
                try (SortingWriter waysWriter = sortingWriter(WAY_ID_WAYJSON_MAP, BUCKET_SIZE)) {
                    try (SortingWriter relationsWriter = sortingWriter(REL_ID_RELJSON_MAP, BUCKET_SIZE)) {
                        try (SortingWriter nodeId2RelIdWriter = sortingWriter(NODE_ID_REL_ID_MAP, BUCKET_SIZE)) {
                            try (SortingWriter wayId2RelIdWriter = sortingWriter(WAY_ID_REL_ID_MAP, BUCKET_SIZE)) {
                                try (LineIterable lineIterable = new LineIterable(ResourceUtil.bzip2Reader(osmFile))) {
                                    OsmBlobIterable osmIterable = new OsmBlobIterable(lineIterable);

                                    try (BufferedWriter problemNodes = ResourceUtil.gzipFileWriter("problemNodes.gz")) {
                                        try (BufferedWriter problemWays = ResourceUtil.gzipFileWriter("problemWays.gz")) {
                                            try (BufferedWriter problemRelations = ResourceUtil.gzipFileWriter("problemRelations.gz")) {
                                                Processor<String, Boolean> processor = new Processor<String, Boolean>() {

                                                    @Override
                                                    public Boolean process(String blob) {
                                                        try {
                                                            if (blob.trim().startsWith("<node")) {
                                                                parseNode(nodesWriter,problemNodes, blob);
                                                            } else if (blob.trim().startsWith("<way")) {
                                                                parseWay(waysWriter,problemWays, nodeid2WayidWriter, blob);
                                                            } else if (blob.trim().startsWith("<relation")) {
                                                                parseRelation(relationsWriter,problemRelations, nodeId2RelIdWriter, wayId2RelIdWriter, blob);
                                                            } else {
                                                                LOG.error("unexpected blob type\n" + blob);
                                                                throw new IllegalStateException("unexpected blob type");
                                                            }
                                                            return true;
                                                        } catch (Exception e) {
                                                            LOG.error("unexpected error " + e.getMessage(), e);
                                                            return false;
                                                        }

                                                    }
                                                };
                                                try (ConcurrentProcessingIterable<String, Boolean> it = processConcurrently(osmIterable, processor, 1000,    9,
                                                        10000)) {
                                                    consume(it);
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private void parseNode(SortingWriter nodeWriter, BufferedWriter problemNodes, String input) throws XPathExpressionException, SAXException, IOException {
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
                JsonObject node = object().put("id", id).put("l", array(longitude, latitude)).get();
                JsonObject tags=new JsonObject();
                while (kvm.find()) {
                    String name = kvm.group(1);
                    tags.put(name, kvm.group(2));
                }
                if(tags.size()>0) {
                    node.put("tags", tags);
                }
                nodeWriter.put("" + id, node.toString());
            } else {
                // ignore nodes without coordinates (apparently they exist), don't flood the logs
                problemNodes.write(input+'\n');
            }

        } else {
            problemNodes.write(input+'\n');
        }
    }

    private void parseWay(SortingWriter waysWriter, BufferedWriter problemWays, SortingWriter nodeid2WayidWriter, String input) throws XPathExpressionException, SAXException, IOException {

        Matcher idm = idPattern.matcher(input);
        Matcher kvm = kvPattern.matcher(input);
        Matcher ndm = ndPattern.matcher(input);

        if (idm.find()) {
            long wayId = Long.valueOf(idm.group(1));
            JsonObject way = object().put("id", wayId).get();
            JsonObject tags=new JsonObject();
            while (kvm.find()) {
                String name = kvm.group(1);
                tags.put(name, kvm.group(2));
            }
            if(tags.size()>0) {
                way.put("tags", tags);
            }
            JsonArray nodeRefs=array();
            while (ndm.find()) {
                Long nodeId = Long.valueOf(ndm.group(1));
                nodeid2WayidWriter.put("" + nodeId, "" + wayId);
                nodeRefs.add(primitive(nodeId));
            }
            way.put("ns", nodeRefs);
            waysWriter.put("" + wayId, way.toString());
        } else {
            problemWays.write(input+'\n');

        }
    }

    private void parseRelation(SortingWriter relationsWriter, BufferedWriter problemRelations, SortingWriter nodeId2RelIdWriter, SortingWriter wayId2RelIdWriter, String input) throws XPathExpressionException, SAXException, IOException {
        Matcher idm = idPattern.matcher(input);
        Matcher kvm = kvPattern.matcher(input);
        Matcher mm = memberPattern.matcher(input);

        if (idm.find()) {
            long relationId = Long.valueOf(idm.group(1));
            JsonObject relation = object().put("id", relationId).get();
            JsonObject tags=new JsonObject();
            while (kvm.find()) {
                String name = kvm.group(1);
                tags.put(name, kvm.group(2));
            }
            if(tags.size()>0) {
                relation.put("tags", tags);
            }

            JsonArray members = array();
            while (mm.find()) {
                String type = mm.group(1);
                Long ref = Long.valueOf(mm.group(2));
                String role = mm.group(3);
                if ("way".equalsIgnoreCase(type)) {
                    members.add(object().put("id",ref).put("type", type).put("role", role).get());
                    wayId2RelIdWriter.put(""+ref, ""+relationId);
                } else if ("node".equalsIgnoreCase(type)) {
                    members.add(object().put("id",ref).put("type", type).put("role", role).get());
                    nodeId2RelIdWriter.put(""+ref, ""+relationId);
                } else  if ("relation".equalsIgnoreCase(type)) {
                    // FIXME support relation members as well
                } else {
                    LOG.warn("unknown member type " + type);
                }
            }
            relation.put("members", members);
            relationsWriter.put(""+relationId, relation.toString());
        } else {
            problemRelations.write(input + '\n');
        }
    }

    public static <In, Out> void processIt(Iterable<In> iterable, Processor<In, Out> processor, int blockSize, int threads, int queueSize) {
        try (ConcurrentProcessingIterable<In, Out> concIt = processConcurrently(iterable, processor, blockSize, threads, queueSize)) {
            consume(concIt);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    static PeekableIterator<Entry<String,String>> peekableEntryIterable(Iterable<String> it) {
        return new PeekableIterator<Entry<String,String>>(map(it, new EntryParsingProcessor()));
    }

    void createWayId2NodeJsonMap(String nodeId2wayIdFile, String nodeId2nodeJsonFile, String outputFile) {
        try (SortingWriter out = sortingWriter(outputFile, BUCKET_SIZE)) {
            EntryJoiningIterable.join(nodeId2nodeJsonFile,nodeId2wayIdFile, new Processor<JoinedEntries, Boolean>() {

                @Override
                public Boolean process(JoinedEntries joined) {
                    String nodeJson = joined.left.get(0).getValue();
                    for(Entry<String, String> e: joined.right) {
                        String wayId = e.getValue();
                        out.put(wayId, nodeJson);
                    }

                    return true;
                }
            });
        } catch (IOException e) {
            throw new IllegalStateException("exception while closing sorted writer " + outputFile,e);
        }
    }

    private void createWayId2CompleteJsonMap(String wayIdWayjsonMap, String wayIdNodeJsonMap, String outputFile) {
        // json blobs are quite big, so reducing bucket size
        try (SortingWriter out = sortingWriter(outputFile, 50000)) {
            EntryJoiningIterable.join(wayIdWayjsonMap,wayIdNodeJsonMap, new Processor<JoinedEntries, Boolean>() {

                @Override
                public Boolean process(JoinedEntries joined) {
                    HashMap<String, JsonObject> nodes = new HashMap<String, JsonObject>();
                    for(Entry<String, String> e: joined.right) {
                        JsonObject node = parser.parse(e.getValue()).asObject();
                        nodes.put(node.getString("id"), node);
                    }
                    Entry<String, String> wayEntry = joined.left.get(0);
                    JsonObject way=parser.parse(wayEntry.getValue()).asObject();
                    JsonArray nodeObjects = array();
                    for(long nodeId: way.getArray("ns").longs()) {
                        JsonObject node=nodes.get(""+nodeId);
                        if(node != null) {
                            nodeObjects.add(node);
                        } else {
                            way.getOrCreateArray("missingNodeRefs").add(primitive(nodeId));
                        }
                    }
                    way.put("nodes", nodeObjects);
                    way.remove("ns");
                    out.put(wayEntry.getKey(), way.toString());
                    return true;
                }
            });

        } catch (IOException e) {
            throw new IllegalStateException("exception while closing sorted writer " + outputFile ,e);
        }
    }

    private void createRelid2NodeJsonMap(String nodeIdRelIdMap, String nodeIdNodejsonMap, String outputFile) {
        try (SortingWriter out = sortingWriter(outputFile, 10000)) {
            EntryJoiningIterable.join(nodeIdRelIdMap,nodeIdNodejsonMap, new Processor<JoinedEntries, Boolean>() {

                @Override
                public Boolean process(JoinedEntries joined) {
                    String nodeJson = joined.right.get(0).getValue();
                    for(Entry<String, String> e: joined.left) {
                        String relId = e.getValue();
                        out.put(relId, nodeJson);
                    }
                    return true;
                }

            });

        } catch (IOException e) {
            throw new IllegalStateException("exception while closing sorted writer " + outputFile ,e);
        }

    }

    private void createRelid2JsonWithNodes(String relIdReljsonMap, String relIdNodeJsonMap, String outputFile) {
        try (SortingWriter out = sortingWriter(outputFile, 100000)) {
            EntryJoiningIterable.join(relIdReljsonMap,relIdNodeJsonMap, new Processor<JoinedEntries, Boolean>() {

                @Override
                public Boolean process(JoinedEntries joined) {
                    JsonArray nodes = array();
                    for(Entry<String, String> e: joined.right) {
                        JsonObject nodeJson = parser.parse(e.getValue()).asObject();
                        nodes.add(nodeJson);
                    }
                    for(Entry<String, String> e: joined.left) {
                        JsonObject relJson = parser.parse(e.getValue()).asObject();
                        relJson.put("nodes", nodes);
                        out.put(e.getKey(), relJson.toString());
                    }

                    return true;
                }

            });

        } catch (IOException e) {
            throw new IllegalStateException("exception while closing sorted writer " + outputFile ,e);
        }
    }

    private void createRelId2WayJsonMap(String wayIdRelIdMap, String wayIdWayjsonMap, String outputFile) {
        try (SortingWriter out = sortingWriter(outputFile, 100000)) {
            EntryJoiningIterable.join(wayIdRelIdMap,wayIdWayjsonMap, new Processor<JoinedEntries, Boolean>() {

                @Override
                public Boolean process(JoinedEntries joined) {
                    String wayJson = joined.right.get(0).getValue();
                    for(Entry<String, String> e: joined.left) {
                        String relId = e.getValue();
                        out.put(relId, wayJson);
                    }
                    return true;
                }

            });

        } catch (IOException e) {
            throw new IllegalStateException("exception while closing sorted writer " + outputFile ,e);
        }
    }


    private void createRelId2CompleteJson(String relIdJsonWithNodes, String relIdWayJsonMap, String outputFile) {
        // relations can be extremely large, so reduce bucket size even further
        try (SortingWriter out = sortingWriter(outputFile, 10000)) {
            EntryJoiningIterable.join(relIdJsonWithNodes,relIdWayJsonMap, new Processor<JoinedEntries, Boolean>() {

                @Override
                public Boolean process(JoinedEntries joined) {
                    JsonArray ways = array();
                    for(Entry<String, String> e: joined.right) {
                        JsonObject wayJson = parser.parse(e.getValue()).asObject();
                        ways.add(wayJson);
                    }
                    for(Entry<String, String> e: joined.left) {
                        JsonObject relJson = parser.parse(e.getValue()).asObject();
                        relJson.put("ways", ways);
                        out.put(e.getKey(), relJson.toString());
                    }
                    return true;
                }

            });

        } catch (IOException e) {
            throw new IllegalStateException("exception while closing sorted writer " + outputFile ,e);
        }
    }

    public void processAll(String osmxml) {
        // the join process works by parsing the osm xml blob for blob and creating several sorted multi maps as files using SortingWriter
        // these map files are then joined to more complex files in several steps using the EntryJoiningIterable
        // the main idea behind this approach is to not try to fit everything in ram at once and process efficiently by working with sorted files
        // the output should be a big gzip file with all the nodes, ways, and relations as json blobs on each line. Each blob should have all the stuff it refers embedded.

        StopWatch processTimer = StopWatch.time(LOG, "process " + osmxml);

        StopWatch timer;
        timer = StopWatch.time(LOG, "splitting " +osmxml);
        splitAndEmit(osmxml);
        timer.stop();

        timer=StopWatch.time(LOG, "create "+WAY_ID_NODE_JSON_MAP);
        createWayId2NodeJsonMap(NODE_ID_WAY_ID_MAP, NODE_ID_NODEJSON_MAP, WAY_ID_NODE_JSON_MAP);
        timer.stop();

        timer=StopWatch.time(LOG, "create "+WAY_ID_NODE_JSON_MAP);
        createWayId2CompleteJsonMap(WAY_ID_WAYJSON_MAP, WAY_ID_NODE_JSON_MAP, WAY_ID_COMPLETE_JSON);
        timer.stop();

        timer = StopWatch.time(LOG, "create " + REL_ID_NODE_JSON_MAP);
        createRelid2NodeJsonMap(NODE_ID_REL_ID_MAP, NODE_ID_NODEJSON_MAP, REL_ID_NODE_JSON_MAP);
        timer.stop();

        timer = StopWatch.time(LOG, "create " + REL_ID_JSON_WITH_NODES);
        createRelid2JsonWithNodes(REL_ID_RELJSON_MAP, REL_ID_NODE_JSON_MAP, REL_ID_JSON_WITH_NODES);
        timer.stop();

        timer = StopWatch.time(LOG, "create " + REL_ID_WAY_JSON_MAP);
        createRelId2WayJsonMap(WAY_ID_REL_ID_MAP, WAY_ID_COMPLETE_JSON, REL_ID_WAY_JSON_MAP);
        timer.stop();

        timer = StopWatch.time(LOG, "create " + REL_ID_COMPLETE_JSON);
        createRelId2CompleteJson(REL_ID_JSON_WITH_NODES, REL_ID_WAY_JSON_MAP, REL_ID_COMPLETE_JSON);
        timer.stop();

        processTimer.stop();
    }

    public static void main(String[] args) {
        OsmJoin osmJoin = new OsmJoin("./temp", new JsonParser());
        String osmxml = args[0];
        osmJoin.processAll(osmxml);
    }
}
