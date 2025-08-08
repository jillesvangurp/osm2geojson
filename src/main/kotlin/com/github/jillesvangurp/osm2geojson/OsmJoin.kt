package com.github.jillesvangurp.osm2geojson

object OsmJoin {
    const val NODE_ID_NODEJSON_MAP = "nodeid2rawnodejson.gz"
    const val REL_ID_RELJSON_MAP = "relid2rawreljson.gz"
    const val WAY_ID_WAYJSON_MAP = "wayid2rawwayjson.gz"
    const val NODE_ID_WAY_ID_MAP = "nodeid2wayid.gz"
    const val NODE_ID_REL_ID_MAP = "nodeid2relid.gz"
    const val WAY_ID_REL_ID_MAP = "wayid2relid.gz"
    const val WAY_ID_NODE_JSON_MAP = "wayid2nodejson.gz"
    const val WAY_ID_COMPLETE_JSON = "wayid2completejson.gz"
    const val REL_ID_NODE_JSON_MAP = "relid2nodejson.gz"
    const val REL_ID_JSON_WITH_NODES = "relid2jsonwithnodes.gz"
    const val REL_ID_WAY_JSON_MAP = "relid2wayjson.gz"
    const val REL_ID_COMPLETE_JSON = "relid2completejson.gz"

    val idPattern = Regex("id=\\\"([0-9]+)\\\"")
    val latPattern = Regex("lat=\\\"(-?[0-9]+(\\.[0-9]+)?)\\\"")
    val lonPattern = Regex("lon=\\\"(-?[0-9]+(\\.[0-9]+)?)\\\"")
    val kvPattern = Regex("k=\\\"(.*?)\\\"\\s+v=\\\"(.*?)\\\"")
    val ndPattern = Regex("nd ref=\\\"([0-9]+)\\\"")
    val memberPattern = Regex("member type=\\\"(.*?)\\\" ref=\\\"([0-9]+)\\\" role=\\\"(.*?)\\\"")
}
