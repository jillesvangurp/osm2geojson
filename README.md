# Introduction

Osm2geojson is a little project that utilizes several of my other github projects to convert open streetmap xml to a more
usable, geojson like format.

# Why and How?

The problem with the osm xml is that it is basically a database dump of the three tables they have for nodes, ways, and relations. Most interesting applications probably require these tables to be joined. 

This project merges the three into json blobs that have all the relevant information embedded. It's similar to loading everything in a database and then doing a gigantic join and then converting the output. The advantage of this approach is that it doesn't require a database, or an index and instead simply works by sorting and merging files. 

# OsmJoin

OsmJoin is the tool that joins the osm nodes, ways, and relations into more usable json equivalents. No attempt is made to filter the data and all tags are preserved.

This tool currently does not yet produce geojson. A separate tool that takes the output of OsmJoin and procuces geojson will be added later. The latter tool requires interpreting the meaning of tags in OSM, which given inconsistencies and ambiguities is hardly an exact science.

## Installation and use

This project utilizes a few of my other github projects. Prebuilt binaries for those projects can be downloaded from my private maven repository here: http://www.jillesvangurp.com/2013/02/27/maven-and-my-github-projects/. Alternatively, you can check them out manually and mvn clean install them yourself.

After setting this up, you should be able to run mvn clean install on this project. It will compile and then put some libraries in target/lib. These are needed to run the osmjoin.sh script. Alternatively, you can run this from your IDE. 

Make sure to assign enough heap to fit the bucket size (constant in the source code, defaults to 1M). Make sure you have enough disk space. OSM is a big data set.

## performance, memory, file handles and disk usage

I've ran the OsmJoin tool on full world osm dumps. You'll want the planet osm xml dumps in bz2. These are about 30GB in size. DONOT expand it ;-). There is no reason to.

While running, the tool produces various .gz files with id, json pairs or id,id pairs on each line. These files are sorted and merged in several steps. Additionally, a temp directory is created where so-called bucket files are stored while the tool is running. You should ensure you have enough disk space for all of this. 

I've provided a list of the different files that are generated:

    -rw-r--r-- 1 localstream root  14G Aug 23 06:43 nodeid2rawnodejson.gz
    -rw-r--r-- 1 localstream root  13M Aug 23 04:23 nodeid2relid.gz
    -rw-r--r-- 1 localstream root 7.9G Aug 23 05:50 nodeid2wayid.gz
    -rw-r--r-- 1 localstream root 1.1G Aug 23 11:09 relid2completejson.gz
    -rw-r--r-- 1 localstream root 103M Aug 23 09:55 relid2jsonwithnodes.gz
    -rw-r--r-- 1 localstream root  57M Aug 23 09:54 relid2nodejson.gz
    -rw-r--r-- 1 localstream root 159M Aug 23 04:24 relid2rawreljson.gz
    -rw-r--r-- 1 localstream root 3.4G Aug 23 10:17 relid2wayjson.gz
    -rw-r--r-- 1 localstream root  14G Aug 23 08:15 wayid2nodejson.gz
    -rw-r--r-- 1 localstream root 9.2G Aug 23 04:56 wayid2rawwayjson.gz
    -rw-r--r-- 1 localstream root  77M Aug 23 04:23 wayid2relid.gz
    -rw-r--r-- 1 localstream root  15G Aug 23 09:46 wqyid2completejson.gz 
    
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

