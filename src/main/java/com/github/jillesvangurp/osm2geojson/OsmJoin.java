package com.github.jillesvangurp.osm2geojson;

import static com.github.jsonj.tools.JsonBuilder.array;
import static com.github.jsonj.tools.JsonBuilder.object;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.xpath.XPathExpressionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import com.github.jillesvangurp.common.ResourceUtil;
import com.github.jillesvangurp.mergesort.SortingWriter;
import com.github.jsonj.JsonArray;
import com.github.jsonj.JsonObject;
import com.jillesvangurp.iterables.LineIterable;
import com.jillesvangurp.iterables.PeekableIterator;

public class OsmJoin {
    private static final Logger LOG = LoggerFactory.getLogger(OsmJoin.class);

    private static final String NODES_GZ = "./temp/nodes.gz";
    private static final String RELATIONS_GZ = "./temp/relations.gz";
    private static final String WAYS_GZ = "./temp/ways.gz";
    private static final String WAYS_BY_NODE_ID_GZ = "./temp/nodes-ways.gz";
    private static final String RELATIONS_BY_NODE_ID_GZ = "./temp/nodes-relations.gz";
    private static final String RELATIONS_BY_WAY_ID_GZ = "./temp/ways-relations.gz";

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

        try (SortingWriter nodesWriter = new SortingWriter("./temp/nodes", NODES_GZ, BUCKET_SIZE)) {
            try (SortingWriter nodesWaysWriter = new SortingWriter("./temp/nodes-ways", WAYS_BY_NODE_ID_GZ, BUCKET_SIZE)) {
                try (SortingWriter waysWriter = new SortingWriter("./temp/ways", WAYS_GZ, BUCKET_SIZE)) {
                    try (SortingWriter relationsWriter = new SortingWriter("./temp/relations", RELATIONS_GZ, BUCKET_SIZE)) {
                        try (SortingWriter nodesRelationsWriter = new SortingWriter("./temp/nodes-relations", RELATIONS_BY_NODE_ID_GZ, BUCKET_SIZE)) {
                            try (SortingWriter waysRelationsWriter = new SortingWriter("./temp/ways-relations", RELATIONS_BY_WAY_ID_GZ, BUCKET_SIZE)) {
                                try (LineIterable lineIterable = new LineIterable(ResourceUtil.bzip2Reader(OSM_XML));) {
                                    OpenStreetMapBlobIterable osmIterable = new OpenStreetMapBlobIterable(lineIterable);
                                    for (String blob : osmIterable) {
                                        if (blob.trim().startsWith("<node")) {
                                            parseNode(nodesWriter, blob);
                                        } else if (blob.trim().startsWith("<way")) {
                                            parseWay(waysWriter, nodesWaysWriter, blob);
                                        } else {
                                            parseRelation(relationsWriter, nodesRelationsWriter, waysRelationsWriter, blob);
                                        }
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

    private void parseWay(SortingWriter waysWriter, SortingWriter nodesWaysWriter, String input) throws XPathExpressionException, SAXException {

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
            while (ndm.find()) {
                Long nodeId = Long.valueOf(ndm.group(1));
                nodesWaysWriter.put("" + nodeId, "" + id);
            }
            waysWriter.put("" + id, way.toString());
        }
    }

    private void parseRelation(SortingWriter relationsWriter, SortingWriter nodesRelationsWriter, SortingWriter waysRelationsWriter, String input) throws XPathExpressionException, SAXException {
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
                    members.add(object().put("id",ref).put("type", type).put("role", role).get());
                    nodesRelationsWriter.put(""+ref, ""+id);
                } else if ("node".equalsIgnoreCase(type)) {
                    members.add(object().put("id",ref).put("type", type).put("role", role).get());
                    waysRelationsWriter.put(""+ref, ""+id);
                }
            }
            relation.put("members", members);
            relationsWriter.put(""+id, relation.toString());
        }
    }

    public void mergeNodesAndWays(String tempDir, String waysNodesGz) {
        // merge nodes.gz nodes-ways.gz -> ways-nodes.gz (wayid; nodejson)
        try(LineIterable nodesIt= LineIterable.openGzipFile(NODES_GZ)) {
            try(LineIterable nodesWaysIt= LineIterable.openGzipFile(WAYS_BY_NODE_ID_GZ)) {
                try(SortingWriter sortingWriter = new SortingWriter(tempDir, waysNodesGz, BUCKET_SIZE)) {
                    mergeNodeJsonWithWayIds(nodesWaysIt, nodesIt, sortingWriter);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        // merge ways-nodes.gz ways.gz -> ways-merged.gz (wayid; wayjson
    }

    void mergeNodeJsonWithWayIds(Iterable<String> leftIt, Iterable<String> rightIt, SortingWriter sortingWriter) {
        PeekableIterator<String> leftPeekingIt = new PeekableIterator<String>(leftIt.iterator());
        PeekableIterator<String> rightPeekingIt = new PeekableIterator<String>(rightIt.iterator());
        List<String> idBuffer = new ArrayList<>();
        while(leftPeekingIt.hasNext() && rightPeekingIt.hasNext()) {
            idBuffer.clear();
            String nwLine = rightPeekingIt.next();
            int idx = nwLine.indexOf(';');
            String nodeId = nwLine.substring(0, idx);
            idBuffer.add(nwLine.substring(idx+1));
            if(rightPeekingIt.hasNext()) {
                String nextNwLine = rightPeekingIt.peek();
                idx=nextNwLine.indexOf(';');
                String nextNodeId=nextNwLine.substring(0,idx);

                // there may be multiple ways with the same node; get all their ids
                while(nextNodeId.equals(nodeId)) {
                    nextNwLine=rightPeekingIt.next();
                    idBuffer.add(nextNwLine.substring(idx+1));
                    if(rightPeekingIt.hasNext()) {
                    nextNwLine = rightPeekingIt.peek();
                    idx=nextNwLine.indexOf(';');
                    nextNodeId=nextNwLine.substring(0,idx);
                    } else {
                        nextNodeId=null;
                    }
                }
            }
            if(leftPeekingIt.hasNext()) {
                String nodeLine = leftPeekingIt.peek();
                idx = nodeLine.indexOf(';');
                String nextNodeId=nodeLine.substring(0,idx);
                while (leftPeekingIt.hasNext() && nextNodeId.compareTo(nodeId) < 0) {
                    // fast forward until we find line with the nodeId or something larger
                    leftPeekingIt.next();
                    nodeLine = leftPeekingIt.peek();
                    idx = nodeLine.indexOf(';');
                    nextNodeId=nodeLine.substring(0,idx);
                }
                if (nextNodeId.compareTo(nodeId) == 0) {
                    // there should only be one line per nodeId
                    nodeLine = leftPeekingIt.next();
                    String nodeJson = nodeLine.substring(idx+1);
                    for(String wayId: idBuffer) {
                        sortingWriter.put(wayId, nodeJson);
                    }
                }
            }

        }
    }

    public static void main(String[] args) {
        OsmJoin osmJoin = new OsmJoin();
        osmJoin.splitAndEmit(OSM_XML);
        osmJoin.mergeNodesAndWays("./temp/ways-nodes", "./temp/ways-nodes.gz");
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
