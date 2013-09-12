# run mvn clean install first
java -cp target/osm2geojson-1.0-SNAPSHOT.jar:target/lib/* -Xmx1000M com.github.jillesvangurp.osm2geojson.OsmPostProcessor | tee osmpostprocess.log