# Introduction

Osm2geojson is a little project that utilizes several of my other github projects to convert open streetmap xml to a more
usable, geojson like format.

# Why and How?

The problem with the osm xml is that it is basically a database dump of the three tables they have for nodes, ways, and relations. Most interesting applications probably require these tables to be joined. 

This project merges the three into json blobs that have all the relevant information embedded. It's similar to loading everything in a database and then doing a gigantic join and then converting the output. The advantage of this approach is that it doesn't require a database, or an index and instead simply works by sorting and merging files. 

# OsmJoin

OsmJoin is the tool that joins the osm nodes, ways, and relations into more usable json equivalents. No attempt is made to filter the data and all tags are preserved.

A separate tool that takes the output of OsmJoin and procuces geojson is provided as well (see OsmPostProcess below). The latter tool requires interpreting the meaning of tags in OSM, which given inconsistencies and ambiguities is hardly an exact science.

## Installation and use

This project utilizes a few of my other github projects. Prebuilt binaries for those projects can be downloaded from my private maven repository here: http://www.jillesvangurp.com/2013/02/27/maven-and-my-github-projects/. Alternatively, you can check them out manually and mvn clean install them yourself.

After setting this up, you should be able to run mvn clean install on this project. It will compile and then put some libraries in target/lib. These are needed to run the osmjoin.sh script. Alternatively, you can run this from your IDE. 

Make sure to assign enough heap to fit the bucket size (constant in the source code, defaults to 1M). Make sure you have enough disk space. OSM is a big data set.

## performance, memory, file handles and disk usage

I've ran the OsmJoin tool on full world osm dumps. You'll want the planet osm xml dumps in bz2. These are about 30GB in size. DONOT expand it ;-). There is no reason to.

While running, the tool produces various .gz files with id, json pairs or id,id pairs on each line. These files are sorted and merged in several steps. Additionally, a temp directory is created where so-called bucket files are stored while the tool is running. You should ensure you have enough disk space for all of this. 

I've provided a list of the different files that are generated:

    -rw-r--r-- 1 localstream root 410M Sep 10 13:27 adrress_nodes.gz
    -rw-r--r-- 1 localstream root  26G Sep  5 21:58 nodeid2rawnodejson.gz
    -rw-r--r-- 1 localstream root  13M Sep  5 18:49 nodeid2relid.gz
    -rw-r--r-- 1 localstream root 7.9G Sep  5 20:20 nodeid2wayid.gz
    -rw-r--r-- 1 localstream root 1.3G Sep  6 05:07 relid2completejson.gz
    -rw-r--r-- 1 localstream root 141M Sep  6 04:17 relid2jsonwithnodes.gz
    -rw-r--r-- 1 localstream root  81M Sep  6 04:16 relid2nodejson.gz
    -rw-r--r-- 1 localstream root 161M Sep  5 18:50 relid2rawreljson.gz
    -rw-r--r-- 1 localstream root 5.8G Sep  6 04:57 relid2wayjson.gz
    -rw-r--r-- 1 localstream root  28G Sep  6 04:01 wayid2completejson.gz
    -rw-r--r-- 1 localstream root  25G Sep  6 01:01 wayid2nodejson.gz
    -rw-r--r-- 1 localstream root 9.3G Sep  5 19:23 wayid2rawwayjson.gz
    -rw-r--r-- 1 localstream root  78M Sep  5 18:49 wayid2relid.gz
    -rw-r--r-- 1 localstream root   20 Sep  5 12:06 wqyid2completejson.gz
    
You should expect to use a bit more than twice the total space of these files. So, somewhere around 100GB of free space should be sufficient for the planet osm file, temp directory with bucket files and the generated gz files.    

As you can see from the creation timestamps, the whole process takes some time to run. In this case it ran for approximately 12 hours on a quad core server with a heap size of 5GB and a raid1 disk. The first file is not created until several hours into the process since the first step (parsing the xml into several sorted files) is also the most expensive one. Your mileage may vary. The files of interest after running are

* nodeid2rawnodejson.gz the json for each node, this includes things like POIs. 
* wqyid2completejson.gz the json for each way with the node json for the referenced nodes merged. This includes streets.
* relid2completejson.gz the json for each relation with node and way json merged

The process uses a lot of memory. Especially the later steps are memory intensive. The configuration is hard coded in the OsmJoin class. The key parameter there is the bucketSize that is used for merge sorting the files. Each bucket is created in memory in a sorted datastructure, and then stored when filled to the specified limit. 

A smaller bucketSize means less memory is used. However, this also means more fileHandles are used during the merge and that the merge process has to do more work. With the billions of ways and nodes, you need to be careful to stay under any imposed Filehandle limits by the OS. You may need to increase this limit on e.g. ubuntu where it is by default configured very conservatively to only 1024. This is by no means enough unless you have tens of GB of heap to spare. To change this, modify /etc/security/limits.conf

    # this fixes ridiculously low file handle limit in Linux
    root soft nofile 64000
    root hard nofile 64000
    * soft nofile 64000
    * hard nofile 64000
    
# OsmPostProcess

The goal of this step is to take the output files of OsmJoin and filter, transform, and normalize into GeoJson for the purpose of indexing it in elastic search. 

The process involves interpreting what the OSM tags mean, categorizing, reconstructing polygons, linestrings, etc., and filtering out the stuff that cannot be easily categorized. 

Inevitably this step is lossy. Currently, relations are not processed for reasons of complexity and limited amount of data (only a few hundred thousand relations exist). A preliminary break down based on grepping through the file suggests that the following can be recovered from relations:

350K relations:
* admin_levels (60K) multi_polygons
* public transport routes (62K)
* associated street (30K)
* TMC ??? some traffic meta data (17K)
* restriction on traffic (153K)
* other 34K (mix of all kinds of uncategorized metadata)

Potentially admin_levels and routes may be of interest.

The good news is that the post processing step is easy to customise. All it does is iterate over the joined json from the OsmJoin step.

# Misc thoughts on OSM

One cannot help but wonder why the OSM data is so messy, inconsistent, and poorly structured. For a community effort to catalogue the world, the format is surprisingly sloppy. A project like this shows that it is possible to mine and recover a wealth of information. If only tagging was more consistent it could be exported in a much more usable format. 

The current format is a near unusable database dump that in its current form assumes a relational database. 

Should anybody involved with OSM care about my recommendations, I would recommend the following:

* Evolve internal storage towards a denormalized view of the world. My recent experience indicates that a document store combined with powerful indexing such as provided by Elasticsearch might be more appropriate than database lookups and joins. Also the raw compressed bzip xml is only a few GB smaller than the joined json equivalent. This makes you wonder what purpose storing the data like that serves. Any reasonable use of the data in a relational store involves doing lots of joins in any case and any processing of the data takes hours simply because of the way the data is stored. A pre joined data set is much more efficient to use.
* Introduce a standardized categorization that is validated. Apply this categorization in an automated fashion to all data and deprecate the use of free form tags in OSM applications.
* Crosslink the data with other data sets. For example embedding geoname ids, woe_ids, wikipedia links, etc. would be hugely valuable. Cross linking with e.g. Facebook's opengraph would be enormously valuable as well. Unlike embedding the meta data, linking the data should be safer from a legal point of view.
* Eliminate region specific variations of combinations of tags as much as possible
* Introduce a standardized address format. See also relevant work regarding this in W3C and other open data groups.
* Standardize naming of things and align with geonames and geoplanet on things like language codes, name translations, etc.

This is a massive undertaking but would make curating OSM more rewarding for contributors and open up applications of its data beyond rendering map tiles.
