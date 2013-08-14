package com.github.jillesvangurp.osm2geojson;

import static com.github.jsonj.tools.JsonBuilder.array;
import static com.github.jsonj.tools.JsonBuilder.object;
import static com.jillesvangurp.iterables.Iterables.consume;
import static com.jillesvangurp.iterables.Iterables.map;
import static com.jillesvangurp.iterables.Iterables.processConcurrently;

import java.io.File;
import java.io.IOException;
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
import com.jillesvangurp.iterables.ConcurrentProcessingIterable;
import com.jillesvangurp.iterables.LineIterable;
import com.jillesvangurp.iterables.PeekableIterator;
import com.jillesvangurp.iterables.Processor;

public class OsmJoin {
    private static final Logger LOG = LoggerFactory.getLogger(OsmJoin.class);

    private static final String NODE_ID_NODEJSON_MAP = "nodeid2rawnodejson.gz";
    private static final String REL_ID_RELJSON_MAP = "relid2rawreljson.gz";
    private static final String WAY_ID_WAYJSON_MAP = "wayid2rawwayjson.gz";
    private static final String NODE_ID_WAY_ID_MAP = "nodeid2wayid.gz";
    private static final String NODE_ID_REL_ID_MAP = "nodeid2relid.gz";
    private static final String WAY_ID_REL_ID_MAP = "wayid2relid.gz";

    private static final String WAY_ID_NODE_JSON_MAP = "wayid2nodejson.gz";

    // choose a bucket size that will fit in memory. Larger means less bucket files and more ram are used.
    private static final int BUCKET_SIZE = 100000;

    final Pattern idPattern = Pattern.compile("id=\"([0-9]+)");
    final Pattern latPattern = Pattern.compile("lat=\"([0-9]+(\\.[0-9]+)?)");
    final Pattern lonPattern = Pattern.compile("lon=\"([0-9]+(\\.[0-9]+)?)");
    final Pattern kvPattern = Pattern.compile("k=\"(.*?)\"\\s+v=\"(.*?)\"");
    final Pattern ndPattern = Pattern.compile("nd ref=\"([0-9]+)");
    final Pattern memberPattern = Pattern.compile("member type=\"(.*?)\" ref=\"([0-9]+)\" role=\"(.*?)\"");

    private static final String OSM_XML = "/Users/jilles/data/brandenburg.osm.bz2";

    private final String workDirectory;

    public OsmJoin(String workDirectory) {
        this.workDirectory = workDirectory;
        try {
            FileUtils.forceMkdir(new File(workDirectory));
        } catch (IOException e) {
            throw new IllegalStateException("cannot create dir " + workDirectory);
        }
    }

    private String bucketDir(String file) {
        return workDirectory + File.separatorChar + file + ".buckets";
    }

    private SortingWriter sortingWriter(String file) {
        try {
            return new SortingWriter(bucketDir(file), file, BUCKET_SIZE);
        } catch (IOException e) {
            throw new IllegalStateException("cannot create sorting writer " + file);
        }
    }
    public void splitAndEmit(String osmFile) {

        // create various sorted maps that need to be joined in the next steps

        try (SortingWriter nodesWriter = sortingWriter(NODE_ID_NODEJSON_MAP)) {
            try (SortingWriter nodeid2WayidWriter = sortingWriter(NODE_ID_WAY_ID_MAP)) {
                try (SortingWriter waysWriter = sortingWriter(WAY_ID_WAYJSON_MAP)) {
                    try (SortingWriter relationsWriter = sortingWriter(REL_ID_RELJSON_MAP)) {
                        try (SortingWriter nodeId2RelIdWriter = sortingWriter(NODE_ID_REL_ID_MAP)) {
                            try (SortingWriter wayId2RelIdWriter = sortingWriter(WAY_ID_REL_ID_MAP)) {
                                try (LineIterable lineIterable = new LineIterable(ResourceUtil.bzip2Reader(OSM_XML));) {
                                    OpenStreetMapBlobIterable osmIterable = new OpenStreetMapBlobIterable(lineIterable);

                                    Processor<String, Boolean> processor = new Processor<String, Boolean>() {

                                        @Override
                                        public Boolean process(String blob) {
                                            try {
                                                if (blob.trim().startsWith("<node")) {
                                                    parseNode(nodesWriter, blob);
                                                } else if (blob.trim().startsWith("<way")) {
                                                    parseWay(waysWriter, nodeid2WayidWriter, blob);
                                                } else {
                                                    parseRelation(relationsWriter, nodeId2RelIdWriter, wayId2RelIdWriter, blob);
                                                }
                                                return true;
                                            } catch (XPathExpressionException e) {
                                                throw new IllegalStateException("invalid xpath!",e);
                                            } catch (SAXException e) {
                                                LOG.warn("parse error for " + blob);
                                                return false;
                                            }

                                        }
                                    };
                                    try(ConcurrentProcessingIterable<String, Boolean> it = processConcurrently(osmIterable, processor, 1000, 9, 10000)) {
                                        consume(it);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void parseNode(SortingWriter nodeWriter, String input) throws XPathExpressionException, SAXException {
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
                while (kvm.find()) {
                    String name = kvm.group(1);
                    if (!OsmProcessor.BLACKLISTED_TAGS.contains(name)) {
                        node.put(name, kvm.group(2));
                    }
                }
                nodeWriter.put("" + id, node.toString());
            } else {
                LOG.warn("no lat/lon for " + id);
            }

        } else {
            LOG.warn("no id");
        }
    }

    private void parseWay(SortingWriter waysWriter, SortingWriter nodeid2WayidWriter, String input) throws XPathExpressionException, SAXException {

        Matcher idm = idPattern.matcher(input);
        Matcher kvm = kvPattern.matcher(input);
        Matcher ndm = ndPattern.matcher(input);

        if (idm.find()) {
            long wayId = Long.valueOf(idm.group(1));
            JsonObject way = object().put("id", wayId).get();
            while (kvm.find()) {
                String name = kvm.group(1);
                if (!OsmProcessor.BLACKLISTED_TAGS.contains(name)) {
                    way.put(name, kvm.group(2));
                }
            }
            while (ndm.find()) {
                Long nodeId = Long.valueOf(ndm.group(1));
                nodeid2WayidWriter.put("" + nodeId, "" + wayId);
            }
            waysWriter.put("" + wayId, way.toString());
        }
    }

    private void parseRelation(SortingWriter relationsWriter, SortingWriter nodeId2RelIdWriter, SortingWriter wayId2RelIdWriter, String input) throws XPathExpressionException, SAXException {
        Matcher idm = idPattern.matcher(input);
        Matcher kvm = kvPattern.matcher(input);
        Matcher mm = memberPattern.matcher(input);

        if (idm.find()) {
            long relationId = Long.valueOf(idm.group(1));
            JsonObject relation = object().put("id", relationId).get();
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
                    members.add(object().put("id",ref).put("type", type).put("role", role).get());
                    wayId2RelIdWriter.put(""+ref, ""+relationId);
                } else if ("node".equalsIgnoreCase(type)) {
                    members.add(object().put("id",ref).put("type", type).put("role", role).get());
                    nodeId2RelIdWriter.put(""+ref, ""+relationId);
                }
            }
            relation.put("members", members);
            relationsWriter.put(""+relationId, relation.toString());
        }
    }

//    public void mergeNodesAndWays(String tempDir, String waysNodesGz) {
//        // merge nodes.gz nodes-ways.gz -> ways-nodes.gz (wayid; nodejson)
//        try(LineIterable nodeJsonIt= LineIterable.openGzipFile(NODE_ID_NODEJSON_GZ)) {
//            try(LineIterable node2wayIt= LineIterable.openGzipFile(NODE_ID_WAY_ID_MAP)) {
//                try(SortingWriter sortingWriter = new SortingWriter(tempDir, waysNodesGz, BUCKET_SIZE)) {
//                    mergeNodeJsonWithWayIds(node2wayIt, nodeJsonIt, sortingWriter);
//                }
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        // merge ways-nodes.gz ways.gz -> ways-merged.gz (wayid; wayjson
//    }

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
        try (SortingWriter out = sortingWriter(WAY_ID_NODE_JSON_MAP)) {
            EntryJoiningIterable.join(nodeId2nodeJsonFile,nodeId2wayIdFile, new Processor<EntryJoiningIterable.JoinedEntries, Boolean>() {

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
            throw new IllegalStateException("exception while closing sorted writer",e);
        }
    }

    public void processAll() {
        // the join process works by parsing the osm xml blob for blob and creating several sorted multi maps as files using SortingWriter
        // these map files are then joined to more complex files in several steps using the EntryJoiningIterable
        // the main idea behind this approach is to not try to fit everything in ram at once and process efficiently by working with sorted files
        // the output should be a big gzip file with all the nodes, ways, and relations as json blobs on each line. Each blob should have all the stuff it refers embedded.

        StopWatch processTimer = StopWatch.time(LOG, "process " + OSM_XML);
        StopWatch timer = StopWatch.time(LOG, "splitting " +OSM_XML);
        splitAndEmit(OSM_XML);
        timer.stop();

        timer=StopWatch.time(LOG, "create "+WAY_ID_NODE_JSON_MAP);
        createWayId2NodeJsonMap(NODE_ID_WAY_ID_MAP, NODE_ID_NODEJSON_MAP, WAY_ID_NODE_JSON_MAP);
        timer.stop();

        processTimer.stop();

    }

    public static void main(String[] args) {
        OsmJoin osmJoin = new OsmJoin("./temp");
        osmJoin.processAll();
    }
}
