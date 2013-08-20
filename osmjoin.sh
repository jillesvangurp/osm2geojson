# run mvn clean install first
# big heap needed to allow for large bucket size when sorting (less filehandles)
java -cp target/osm2geojson-1.0-SNAPSHOT.jar:target/lib/* -Xmx3000M com.github.jillesvangurp.osm2geojson.OsmJoin