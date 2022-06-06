## Neo4j Enterprise Testing on Rivanna Large Local Storage Machines

**1. Hardware & Environment**
- Machines: 8 in total; *udc-aj36-[1-4]c[0-1]*
- Local storage info: e.g. on *udc-aj36-4c0*
  
    |NAME|MAJ:MIN|RM|SIZE|RO|FSTYPE|MOUNTPOINT|UUID|MODEL|ROTA|DISC-MAX|SERIAL|VENDOR|
    |:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|
    |sda|8:0|0|1.8T|0|ext4|/localscratch|d1294e1e-bfdf-4564-a13a-64a1c39ad844|MZ7KH1T9HAJR0D3|0|4G|S47JNA0M703738|ATA|

- The model number "MZ7KH1T9HAJR0D3" is very likely to be "Samsung Sm883 1.92 TB Internal SSD 2.5 inch". More details can be found in here: https://semiconductor.samsung.com/ssd/datacenter-ssd/sm883/mz7kh1t9hajr/
- Usage: add *--nodelist=udc-aj36-4c0* to the SBATCH script
- Note that the local storage does not have any backup yet for now. 
- Simple IO test results (using https://bench.sh and https://github.com/n-st/nench): 
    ```
    Sequential write speed (dd):
        I/O Speed(1st run) : 423 MB/s
        I/O Speed(2nd run) : 377 MB/s
        I/O Speed(3rd run) : 395 MB/s
        I/O Speed(average) : 398.3 MB/s
    ioping: seek rate
        min/avg/max/mdev = 33.1 us / 81.5 us / 299.1 us / 10.0 us
    ioping: sequential read speed
        generated 9.67 k requests in 5.00 s, 2.36 GiB, 1.93 k iops, 483.5 MiB/s
    ```
- Full machine specs:
  - CPU Model: Intel(R) Xeon(R) Gold 6248 CPU @ 2.50GHz
  - CPU Cores: 40 @ 2500.000 MHz (logic)
  - CPU Cache: 28160 KB
  - Total Disk: 1.7 TB (local)
  - Total Mem: 376.4 GB
  - OS: CentOS Linux release 7.9.2009 (Core)
  - Arch: x86_64 (64 Bit)
  - Kernel: 3.10.0-1160.59.1.el7.x86_64
- Usage of Computing Nodes on Rivanna
  - Start an interactive job on a specific computing node, e.g.
    ```
    ijob -p bii --nodelist=udc-aj36-4c0 --mem=350000 --time=2880 --exclusive -c 20 -S 40
    ```
  - Under the local mount point, i.e. */local/*, create the workspace folder for Neo4j DB. For testing, we created */local/mf3jh/neo4j_workspace_ca/* on *udc-aj36-4c0*.
- Create Singularity Image for Rivanna
  - Definition file: [*neo4j_singularity.def*](https://github.com/LeSaRDe/neo4j_test/blob/master/neo4j_singularity.def)
  - Use a default definition file (which may not be well tuned) to create a singularity image:
    ```
    sudo singularity build neo4j_enterprise.sif neo4j_singularity.def
    ```
  - Start an interactive job on a specified computing node.
  - Run the image on the computing node using [*launch_cypher_shell.sh*](https://github.com/LeSaRDe/neo4j_test/blob/master/launch_cypher_shell.sh). The script will start a shell inside the container running the image. 
  - In the containter's shell, use `neo4j-admin memrec` to obtain JVM parameters. Particularly, the following settings were suggested by the command:
    ```
    dbms.memory.heap.initial_size=31g
    dbms.memory.heap.max_size=31g
    dbms.memory.pagecache.size=331500m
    dbms.jvm.additional=-XX:+ExitOnOutOfMemoryError
    ```


**2. Testing for Loading**
- Testing data:
  - CA sized
  - Nodes: 35,516,052; 2.6GB
  - Initial Contact Network: 1,402,087,300; 49GB
  - Intermediate Contact Network: 1,205,361,116; 43GB
- Script for loading:
  - [*neo4j_load_data.sh*](https://github.com/LeSaRDe/neo4j_test/blob/master/neo4j_load_data.sh)
- Full log for loading the testing data into Neo4j Enterprise
    ```
    [NEO4J DATA LOADING] Starts.
    [NEO4J DATA LOADING] Work Folder: /local/mf3jh/neo4j_workspace_ca//
    [NEO4J DATA LOADING] Neo4j SIF: /local/mf3jh/neo4j_workspace_ca/img/neo4j_enterprise.sif
    [NEO4J DATA LOADING] Data folder: /var/lib/neo4j/data => /local/mf3jh/neo4j_workspace_ca//data
    [NEO4J DATA LOADING] Log folder: /var/lib/neo4j/logs => /local/mf3jh/neo4j_workspace_ca//logs
    [NEO4J DATA LOADING] Log folder: /var/lib/neo4j/import => /local/mf3jh/neo4j_workspace_ca//import
    [NEO4J DATA LOADING] Starting loading data...
    Selecting JVM - Version:11.0.15+10, Name:OpenJDK 64-Bit Server VM, Vendor:Oracle Corporation
    WARN: source file /var/lib/neo4j/import/edges_header.csv has been specified multiple times, this may result in unwanted duplicates
    Neo4j version: 4.4.6
    Importing the contents of these files into /var/lib/neo4j/data/databases/contacts:
    Nodes:
    [PERSON]:
    /var/lib/neo4j/import/nodes_header.csv
    /var/lib/neo4j/import/initial_contact_network_nodes_pure_data.csv

    Relationships:
    CONTACT_1:
    /var/lib/neo4j/import/edges_header.csv
    /var/lib/neo4j/import/intermediate_network_i_edges_pure_data.csv

    CONTACT_0:
    /var/lib/neo4j/import/edges_header.csv
    /var/lib/neo4j/import/initial_contact_network_edges_pure_data.csv


    Available resources:
    Total machine memory: 376.4GiB
    Free machine memory: 157.4GiB
    Max heap memory : 31.00GiB
    Processors: 20
    Configured max memory: 310.8GiB
    High-IO: true


    Import starting 2022-05-27 13:43:50.312+0000
    Estimated number of nodes: 38.42 M
    Estimated number of node properties: 307.36 M
    Estimated number of relationships: 3.03 G
    Estimated number of relationship properties: 9.08 G
    Estimated disk space usage: 215.0GiB
    Estimated required memory usage: 1.468GiB

    (1/4) Node import 2022-05-27 13:43:50.354+0000
    Estimated number of nodes: 38.42 M
    Estimated disk space usage: 3.522GiB
    Estimated required memory usage: 1.468GiB
    .......... .......... .......... .......... ..........   5% ∆2s 640ms
    .......... .......... .......... .......... ..........  10% ∆1s 815ms
    .......... .......... .......... .......... ..........  15% ∆1s 805ms
    .......... .......... .......... .......... ..........  20% ∆2s 9ms
    .......... .......... .......... .......... ..........  25% ∆1s 804ms
    .......... .......... .......... .......... ........-.  30% ∆802ms
    .......... .......... .......... .......... ..........  35% ∆1ms
    .......... .......... .......... .......... ..........  40% ∆1ms
    .......... .......... .......... .......... ..........  45% ∆1s 802ms
    .......... .......... .......... .......... ..........  50% ∆402ms
    .......... .......... .......... .......... ..........  55% ∆603ms
    .......... .......... .......... .......... ..........  60% ∆602ms
    .......... .......... .......... .......... ..........  65% ∆602ms
    .......... .......... .......... .......... ..........  70% ∆1ms
    .......... .......... .......... .......... ..........  75% ∆119ms
    .......... .......... .......... .......... ..........  80% ∆0ms
    .......... .......... .......... .......... ..........  85% ∆1ms
    .......... .......... .......... .......... ..........  90% ∆0ms
    .......... .......... .......... .......... ..........  95% ∆1ms
    .......... .......... .......... .......... .......... 100% ∆0ms

    Node import COMPLETED in 17s 462ms

    (2/4) Relationship import 2022-05-27 13:44:07.817+0000
    Estimated number of relationships: 3.03 G
    Estimated disk space usage: 211.5GiB
    Estimated required memory usage: 1.397GiB
    .......... .......... .......... .......... ..........   5% ∆47s 739ms
    .......... .......... .......... .......... ..........  10% ∆48s 475ms
    .......... .......... .......... .......... ..........  15% ∆48s 486ms
    .......... .......... .......... .......... ..........  20% ∆47s 233ms
    .......... .......... .......... .......... ..........  25% ∆47s 444ms
    .......... .......... .......... .......... ..........  30% ∆47s 640ms
    .......... .......... .......... .......... ..........  35% ∆47s 111ms
    .......... .......... .......... .......... ..........  40% ∆46s 675ms
    .......... .......... .......... .......... ..........  45% ∆46s 675ms
    .......... .......... .......... .......... ..........  50% ∆48s 44ms
    .......... .......... .......... .......... ..........  55% ∆50s 134ms
    .......... .......... .......... .......... ..........  60% ∆48s 570ms
    .......... .......... .......... .......... ..........  65% ∆49s 349ms
    .......... .......... .......... .......... ..........  70% ∆48s 677ms
    .......... .......... .......... .......... ..........  75% ∆49s 404ms
    .......... .......... .......... .......... ..........  80% ∆48s 153ms
    .......... .......... .......... .......... ..........  85% ∆46s 980ms
    .......... .......... .......... .......... ..........  90% ∆11s 9ms
    .......... .......... .......... .......... ..........  95% ∆1ms
    .......... .......... .......... .......... .......... 100% ∆0ms

    Relationship import COMPLETED in 13m 47s 799ms

    (3/4) Relationship linking 2022-05-27 13:57:55.617+0000
    Estimated required memory usage: 1.360GiB
    .......... .......... .......... .......... ..........   5% ∆27s 622ms
    .......... .......... .......... .......... ..........  10% ∆17s 222ms
    .......... .......... .......... .......... ..........  15% ∆16s 337ms
    .......... .......... .......... .......... .........-  20% ∆400ms
    .......... .......... .......... .......... ..........  25% ∆39s 639ms
    .......... .......... .......... .......... ..........  30% ∆42s 841ms
    .......... .......... .......... .......... ..........  35% ∆42s 832ms
    .......... .......... .......... .......... ..........  40% ∆35s 422ms
    .......... .......... .......... .......... ..........  45% ∆1m 12s 34ms
    .......... .......... .......... .......... ..........  50% ∆45s 231ms
    .......... .......... .......... .......... ..........  55% ∆47s 843ms
    .......... .......... .......... .......... .........-  60% ∆600ms
    .......... .......... .......... .......... ..........  65% ∆44s 835ms
    .......... .......... .......... .......... ..........  70% ∆45s 40ms
    .......... .......... .......... .......... ..........  75% ∆41s 828ms
    .......... .......... .......... .......... ..........  80% ∆47s 30ms
    .......... .......... .......... .......... ..........  85% ∆46s 233ms
    .......... .......... .......... .......... ..........  90% ∆42s 29ms
    .......... .......... .......... .......... ..........  95% ∆37s 421ms
    .......... .......... .......... .......... .......... 100% ∆38s 944ms

    Relationship linking COMPLETED in 13m 30s 257ms

    (4/4) Post processing 2022-05-27 14:11:25.874+0000
    Estimated required memory usage: 1020MiB
    .......... .......-.. .......... ......-... ......-...   5% ∆1s 200ms
    ........-. .......... .......... .......... ..........  10% ∆10s 10ms
    .......... .......... .......... .......... ..........  15% ∆11s 211ms
    .......... .......... .......... .......... ..........  20% ∆11s 14ms
    .......... .......... .......... .......... ..........  25% ∆11s 20ms
    .......... .......... .......... .......... ..........  30% ∆11s 213ms
    .......... .......... .......... .......... ..........  35% ∆11s 13ms
    .......... .......... .......... .......... ..........  40% ∆11s 220ms
    .......... .......... .......... .......... ..........  45% ∆11s 212ms
    .......... .......... .......... .......... ..........  50% ∆11s 814ms
    .......... .......... .......... .......... ..........  55% ∆12s 610ms
    .......... .......... .......... .......... ..........  60% ∆13s 15ms
    .......... .......... .......... .......... ..........  65% ∆13s 4ms
    .......... .......... .......... .......... ..........  70% ∆12s 825ms
    .......... .......... .......... .......... ..........  75% ∆12s 815ms
    .......... .......... .......... .......... ..........  80% ∆13s 10ms
    .......... .......... .......... .......... ..........  85% ∆13s 20ms
    .......... .......... .......... .......... ..........  90% ∆13s 12ms
    .......... .......... .......... .......... ..........  95% ∆12s 809ms
    .......... .......... .......... .......... .......... 100% ∆9s 739ms

    Post processing COMPLETED in 4m 18s 424ms


    IMPORT DONE in 31m 54s 544ms. 
    Imported:
    35516052 nodes
    2607448416 relationships
    8106473664 properties
    Peak memory usage: 3.361GiB
    [NEO4J DATA LOADING] Loading data done.
    ```
- *debug.log* after loading
    ```
    2022-05-27 13:43:49.203+0000 INFO  [o.n.k.i.s.f.RecordFormatSelector] Record format not configured, selected default: RecordFormat:PageAlignedV4_3[AF4.3.0]
    2022-05-27 13:43:49.754+0000 INFO  [o.n.i.b.ImportLogic] Import starting
    2022-05-27 14:15:44.311+0000 INFO  [o.n.i.b.ImportLogic] Import completed successfully, took 31m 54s 544ms. Imported:
    35516052 nodes
    2607448416 relationships
    8106473664 properties
    ```
- Space consumption after loading
  - *data* folder: 190GB (without indexes)
  - *data* folder: 340GB (with all indexes, i.e. 7 for nodes and 6 for edges on 2 time stamp labels)
  - raw data size: 2.6GB + 49GB + 43GB = 94.6GB
  - Expansion ratio after loading without indexes: 190GB / 94.6GB = 2.00
  - Expansion ratio with indexes: 340GB / 94.6GB = 3.59

- **Questions**
  - There are 35,516,052 nodes in the testing data. In the output log of loading, it states that "Estimated number of nodes: 38.42 M". These two numbers do not match. Why?
  - Similarly, the numbers of edges in the initial contact network and the intermediate contact network are 1,402,087,300 and 1,205,361,116 respectively. These numbers do not match with what is stated in the log (i.e. "Estimated number of relationships: 3.03 G") either. 


**3. Testing for Neo4j DB Running**
- Keep using the specified computing node for loading, i.e. *udc-aj36-4c0*. 
- Use [*launch_neo4j.sh*](https://github.com/LeSaRDe/neo4j_test/blob/master/launch_neo4j.sh) on this node to start the Neo4j server. 
- Start an interactive job on a regular computing node. 
- Use [*launch_cypher_shell.sh*](https://github.com/LeSaRDe/neo4j_test/blob/master/launch_cypher_shell.sh) to start a shell inside the container running the Neo4j image. 
- In the container's shell, use `cypher-shell -a udc-aj36-4c0` to connect the Neo4j server running on *udc-aj36-4c0*.
- After connecting to the DB server running on *udc-aj36-4c0*, use `create database contacts` to create the database named *contacts*. Without explicitly creating the database, it will not automatically be created. 
- Then we are able to see the database become online using `show databases` to list all existing databases.
- <s>**Questions**
  - The result of `show databases` was
  
    |name|aliases|access|address|role|requestedStatus|currentStatus|error|default|home|
    |:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|
    |"neo4j"|[]|"read-write"|"localhost:7687"|"standalone"|"online"|"online"|""|TRUE|TRUE|
    |"system"|[]|"read-write"|"localhost:7687"|"standalone"|"online"|"online"|""|FALSE|FALSE|

    There was no the loaded database named *contacts*. Why?</s>
- After the database creation, we can create constraints and indexes as normal. Creating indexes on node properties took less than 1 minute for each. However, creating indexes on edge properties took much longer. For example, typically, creating the index for the *duration* property for each time stamp label (e.g. *CONTACT_0* and *CONTACT_1*) took about 20 minutes. 
- **Question**
  - Can we make it faster when creating indexes for edge properties?
  - Is there any approach to create an *array-like* property for the time stamps with indexing instead of using multiple time stamp labels?
- A sample query
  - Cypher query string:
    ```
    match ()-[r]->(n)
    where (r.src_act="1:2") and 
          ((n.gender=2 and n.age>=21 and n.age<=55) 
          or 
          (n.gender=1 and n.age>=56 and n.age<=85))
    return distinct n.pid
    ```
  - Running time varies from a couple of to a few minutes when chaning the ranges of ages. 