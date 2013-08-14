package com.github.jillesvangurp.osm2geojson;

import static com.github.jsonj.tools.JsonBuilder.array;
import static com.github.jsonj.tools.JsonBuilder.object;
import static com.jillesvangurp.iterables.Iterables.consume;
import static com.jillesvangurp.iterables.Iterables.map;
import static com.jillesvangurp.iterables.Iterables.processConcurrently;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.xpath.XPathExpressionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import com.github.jillesvangurp.common.ResourceUtil;
import com.github.jillesvangurp.mergesort.SortingWriter;
import com.github.jillesvangurp.metrics.StopWatch;
import com.github.jsonj.JsonArray;
import com.github.jsonj.JsonObject;
import com.jillesvangurp.iterables.ConcurrentProcessingIterable;
import com.jillesvangurp.iterables.LineIterable;
import com.jillesvangurp.iterables.PeekableIterator;
import com.jillesvangurp.iterables.Processor;

public class OsmJoin {
    private static final Logger LOG = LoggerFactory.getLogger(OsmJoin.class);

    private static final String NODE_ID_NODEJSON_GZ = "./temp/nodeid2rawnodejson.gz";
    private static final String REL_ID_RELJSON_GZ = "./temp/relid2rawreljson.gz";
    private static final String WAY_ID_WAYJSON_GZ = "./temp/wayid2rawwayjson.gz";
    private static final String NODE_ID_WAY_ID_MAP = "./temp/nodeid2wayid.gz";
    private static final String NODE_ID_REL_ID_MAP = "./temp/nodeid2relid.gz";
    private static final String WAY_ID_REL_ID_MAP = "./temp/wayid2relid.gz";

    private static final int BUCKET_SIZE = 100000;

    final Pattern idPattern = Pattern.compile("id=\"([0-9]+)");
    final Pattern latPattern = Pattern.compile("lat=\"([0-9]+(\\.[0-9]+)?)");
    final Pattern lonPattern = Pattern.compile("lon=\"([0-9]+(\\.[0-9]+)?)");
    final Pattern kvPattern = Pattern.compile("k=\"(.*?)\"\\s+v=\"(.*?)\"");
    final Pattern ndPattern = Pattern.compile("nd ref=\"([0-9]+)");
    final Pattern memberPattern = Pattern.compile("member type=\"(.*?)\" ref=\"([0-9]+)\" role=\"(.*?)\"");

    private static final String OSM_XML = "/Users/jilles/data/brandenburg.osm.bz2";

    public OsmJoin() {
    }

    public void splitAndEmit(String osmFile) {

        // create various sorted maps that need to be joined in the next steps

        try (SortingWriter nodesWriter = new SortingWriter("./temp/nodes", NODE_ID_NODEJSON_GZ, BUCKET_SIZE)) {
            try (SortingWriter nodeid2WayidWriter = new SortingWriter("./temp/nodes-ways", NODE_ID_WAY_ID_MAP, BUCKET_SIZE)) {
                try (SortingWriter waysWriter = new SortingWriter("./temp/ways", WAY_ID_WAYJSON_GZ, BUCKET_SIZE)) {
                    try (SortingWriter relationsWriter = new SortingWriter("./temp/relations", REL_ID_RELJSON_GZ, BUCKET_SIZE)) {
                        try (SortingWriter nodeId2RelIdWriter = new SortingWriter("./temp/nodes-relations", NODE_ID_REL_ID_MAP, BUCKET_SIZE)) {
                            try (SortingWriter wayId2RelIdWriter = new SortingWriter("./temp/ways-relations", WAY_ID_REL_ID_MAP, BUCKET_SIZE)) {
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

    public void mergeNodesAndWays(String tempDir, String waysNodesGz) {
        // merge nodes.gz nodes-ways.gz -> ways-nodes.gz (wayid; nodejson)
        try(LineIterable nodeJsonIt= LineIterable.openGzipFile(NODE_ID_NODEJSON_GZ)) {
            try(LineIterable node2wayIt= LineIterable.openGzipFile(NODE_ID_WAY_ID_MAP)) {
                try(SortingWriter sortingWriter = new SortingWriter(tempDir, waysNodesGz, BUCKET_SIZE)) {
                    mergeNodeJsonWithWayIds(node2wayIt, nodeJsonIt, sortingWriter);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        // merge ways-nodes.gz ways.gz -> ways-merged.gz (wayid; wayjson
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

    void createWayId2NodeJsonMap(Iterable<String> nodes2ways, Iterable<String> nodes2Json, SortingWriter out) {
        PeekableIterator<Entry<String, String>> node2WayIt = peekableEntryIterable(nodes2ways);
        PeekableIterator<Entry<String, String>> node2JsonIt = peekableEntryIterable(nodes2Json);
        List<String> nodeBuffer = new ArrayList<>();
        while(node2WayIt.hasNext() && node2JsonIt.hasNext()) {
            Entry<String, String> nodeJsonEntry = node2JsonIt.next();
            String nodeId=nodeJsonEntry.getKey();
            nodeBuffer.add(nodeJsonEntry.getValue());
            if(node2JsonIt.hasNext()) {
                Entry<String, String> nextNode2JsonEntry = node2JsonIt.peek();
                String nextNodeId=nextNode2JsonEntry.getValue();
                while(nextNodeId.equals(nodeId)) {
                    nextNode2JsonEntry = node2JsonIt.next();
                    nodeBuffer.add(nextNode2JsonEntry.getValue());
                    if(node2JsonIt.hasNext()) {
                        nextNode2JsonEntry = node2JsonIt.peek();
                        nextNodeId=nextNode2JsonEntry.getValue();
                    } else {
                        nextNodeId=null;
                    }
                }
            }

        }
    }

    void mergeNodeJsonWithWayIds(Iterable<String> node2wayIt, Iterable<String> nodeJsonIt, SortingWriter sortingWriter) {
        PeekableIterator<String> node2wayPeekingIt = new PeekableIterator<String>(node2wayIt.iterator());
        PeekableIterator<String> rightPeekingIt = new PeekableIterator<String>(nodeJsonIt.iterator());
        List<String> nodeBuffer = new ArrayList<>();
        while(node2wayPeekingIt.hasNext() && rightPeekingIt.hasNext()) {
            nodeBuffer.clear();
            String nwLine = rightPeekingIt.next();
            int idx = nwLine.indexOf(';');
            String nodeId = nwLine.substring(0, idx);
            nodeBuffer.add(nwLine.substring(idx+1));
            if(rightPeekingIt.hasNext()) {
                String nextNwLine = rightPeekingIt.peek();
                idx=nextNwLine.indexOf(';');
                String nextNodeId=nextNwLine.substring(0,idx);

                // there may be multiple ways with the same node; get all their ids
                while(nextNodeId.equals(nodeId)) {
                    nextNwLine=rightPeekingIt.next();
                    nodeBuffer.add(nextNwLine.substring(idx+1));
                    if(rightPeekingIt.hasNext()) {
                        nextNwLine = rightPeekingIt.peek();
                        idx=nextNwLine.indexOf(';');
                        nextNodeId=nextNwLine.substring(0,idx);
                    } else {
                        nextNodeId=null;
                    }
                }
            }
            if(node2wayPeekingIt.hasNext()) {
                String nodeLine = node2wayPeekingIt.peek();
                idx = nodeLine.indexOf(';');
                String nextNodeId=nodeLine.substring(0,idx);
                while (node2wayPeekingIt.hasNext() && nextNodeId.compareTo(nodeId) < 0) {
                    // fast forward until we find line with the nodeId or something larger
                    node2wayPeekingIt.next();
                    nodeLine = node2wayPeekingIt.peek();
                    idx = nodeLine.indexOf(';');
                    nextNodeId=nodeLine.substring(0,idx);
                }
                if (nextNodeId.compareTo(nodeId) == 0) {
                    // there should only be one line per nodeId
                    nodeLine = node2wayPeekingIt.next();
                    String wayId = nodeLine.substring(idx+1);
                    for(String nodeJson: nodeBuffer) {
                        sortingWriter.put(wayId,nodeJson);
                    }
                }
            }

        }
    }

    public static void main(String[] args) {
        OsmJoin osmJoin = new OsmJoin();
        StopWatch processTimer = StopWatch.time(LOG, "process " + OSM_XML);
        StopWatch timer = StopWatch.time(LOG, "creating id maps for " +OSM_XML);
        osmJoin.splitAndEmit(OSM_XML);
        timer.stop();

        timer=StopWatch.time(LOG, "merge nodes and ways");
        osmJoin.mergeNodesAndWays("./temp/ways-nodes", "./temp/ways-nodes.gz");
        timer.stop();
        processTimer.stop();
    }

    /*
     * nodes.gz nodeid; nodejson
     * ways.gz wayid; wayjson
     * nodes-ways.gz nodeid;wayid relations.gz
     * relationid;role;relationjson nodes-relations.gz nodeid;relationid ways-relations.gz wayid;relationid
     * node-ways-merged; wayid; nodejson ways-merged; wayid; wayjson nodes-relations-merged relationid; nodejson
     * ways-relations-merged relationid;wayjson relations-merged relationid; relationjson
     */
}
