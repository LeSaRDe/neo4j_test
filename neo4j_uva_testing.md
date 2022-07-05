## Neo4j Enterprise Testing on Rivanna Large Local Storage Machines

### Sync Testing with Neo4j on CA-Sized Data

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
  - Note on the above JVM memory settings: Neo4j suggests that we set 150GB for heap and 200GB for page cache. This is different from the above settings. **TODO**: We need to carefully investigate how these settings affect the performance of Neo4j.
- Memory Consumption on Idling
  - ~27GB
- Neo4j Plugins:
  - APOC
  - GDS
    - **Question**: How could we have the licence key file to unlock the Enterprise Edition of GDS? (https://neo4j.com/docs/graph-data-science/current/installation/installation-enterprise-edition/)

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
- Time needed to fully start Neo4j with data
  - After the loading and indexing, without further operations, it takes 1-2 minutes before *cypher-shell* is able to connect to the server. However, after some operations including using projections, shutting down and restarting, it would take much longer to start. And we observed many logs like this:
    ```
    2022-06-15 14:41:48.727+0000 INFO  [o.n.k.i.i.s.GenericNativeIndexProvider] [contacts/0b8ad418] Schema index cleanup job started: descriptor=Index( id=16, name='idx_rel_trg_act_1', type='GENERAL BTREE', schema=-[:Relationship Type[0] {PropertyKey[8]}]-, indexProvider='native-btree-1.0' ), indexFile=/data/databases/contacts/schema/index/native-btree-1.0/16/index-16
    ```
    - **Question**: Why did this happen? Did we do anything wrong?
      - Answer: According to some comments from Neo4j, it'd be better to stop the database server safely without crude shut-down. We may need to look into this matter more carefully to make sure our current server stopping is appropriate enough, and no recovery is needed when starting up.
  - About 5 or more minutes observing the memory consumed by the server increasing.
- Memory consumption after loading and indexing
  - After the server starts, without any action, the memory consumption keeps increasing. Typically it climbs higher than 300GB, and it can hit more than 360GB. And this amount almost hits the memory cap of our computing node. What is even worse is that in this situation we couldn't switch to our database *contacts* using `:use contacts`.
    - **Question**: Does this sound normal? How much memory should we install in the machine to have this server run without memory stalls? 
      - Answer: A rough answer is that we do need large memory to handle the entire database in memory.
- **Questions**
  - There are 35,516,052 nodes in the testing data. In the output log of loading, it states that "Estimated number of nodes: 38.42 M". These two numbers do not match. Why?
  - Similarly, the numbers of edges in the initial contact network and the intermediate contact network are 1,402,087,300 and 1,205,361,116 respectively. These numbers do not match with what is stated in the log (i.e. "Estimated number of relationships: 3.03 G") either. 


**3. Testing for Neo4j Data Loading and Indexing**
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
    - Answer: According to comments from Neo4j, this may depend on the performance of our disks. 
  - Is there any approach to create an *array-like* property for the time stamps with indexing instead of using multiple time stamp labels?
    - Answer: Up to this point, we need to keep using our current method instead of *array-like* data types, because using copies for time points is more friendly for indexing and executing queries. Note that, on the other hand, keeping using our current method would need large storage and memory. 
- Note for few-value attributes like `age`: According to comments from Neo4j, we should use labels instead of properties for those attributes to gain better efficacy and efficiency. And it'd be more efficient if this is done before the loading rather than after. Though, in case this has to be done after the loading, the following commands can be used:
  ```
  call apoc.periodic.iterate("MATCH (n:Person) WHERE n.gender = 1 RETURN id(n) as id", "MATCH (n) WHERE id(n)=id SET n:Male REMOVE n.gender", {batchSIze:50000, parallel:true});

  call apoc.periodic.iterate("MATCH (n:Person) WHERE n.gender = 0 RETURN id(n) as id", "MATCH (n) WHERE id(n)=id SET n:Female REMOVE n.gender", {batchSIze:50000, parallel:true});
  ```

**4. Testing for Queries**
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
  - Note: The above query string is a BAD example! Because it doesn't use the index for nodes and relationships. Instead, we should use the following:
    ```
    match (n:Person)
    where 
    ((n:Female and 21 <= n.age <=55) or
    (n:Male and 56 <= n.age <=85))
    and exists { ()-[:CONTACT_0|CONTACT_1 {src_act:"1:2"}]->(n) } // can be an index lookup if it selective enough
    with distinct n
    return n.pid
    ```
  - Running time varies from a couple of to a few minutes when chaning the ranges of ages. 


**5. Testing for GDS**
- Some background about Neo4j GDS
  - Two essential items in GDS are: 1) workflow of algorithms, and 2) graph projections.
  - Graph projections are in-memory data structures of graphs and relevant informantion. Projections are named by users, and managed by a so-called *graph catelog* object. Projections stay in memory, and can be retrieved by their names. 
  - Graph projections can be created by *native projections* or *Cypher projections*. The former one has better performance, while the latter one is more flexible. 
  - Using GDS can be both CPU and memory consuming, and thus doing memory estimation beforehand is necessary.
- Create Native Projections
  - Example: Create a projection for the initial contact network, named *init_cn*, on nodes with the label *PERSON* and edges with the label *CONTACT_0*.
  - Command:
    ```
    call gds.graph.project('init_cn', 'PERSON', 'CONTACT_0') yield graphName AS graph, nodeProjection, nodeCount AS nodes, relationshipCount AS rels
    ```
  - Running time: ~5 minutes
  - **Question**
    - Interestingly, after creating a projection, the memory consumed by the server doesn't increase, at least not significantly. According to the description of projections, they are in-memory data structures, then why did we not see memory increase?
      - Answer: According to comments from Neo4j, only the algorithm execution takes temporarily additional memory.
    - `gds.alpha.allShortestPaths.stream` doesn't support memory estimation?



### Testing on NY Data

**1. Machine**
  - *udc-aj36-3c0*

**2. Testing Data**
   - NY data
   - Nodes: 18,120,752; 1.3GB
   - Initial Edges: 782,828,604; 28GB
   - Intermediate #1 Edges: 656,138,932; 24GB

**3. Data Loading**
  - `neo4j-admin import` Log:
    ```
    [NEO4J DATA LOADING] Starts.
    [NEO4J DATA LOADING] Work Folder: /local/mf3jh/neo4j_workspace_ny//
    [NEO4J DATA LOADING] Neo4j SIF: /local/mf3jh/neo4j_workspace_ny/img/neo4j_enterprise.sig
    [NEO4J DATA LOADING] Data folder: /var/lib/neo4j/data => /local/mf3jh/neo4j_workspace_ny//data
    [NEO4J DATA LOADING] Log folder: /var/lib/neo4j/logs => /local/mf3jh/neo4j_workspace_ny//logs
    [NEO4J DATA LOADING] Log folder: /var/lib/neo4j/import => /local/mf3jh/neo4j_workspace_ny//import
    [NEO4J DATA LOADING] Starting loading data...
    Selecting JVM - Version:11.0.15+10, Name:OpenJDK 64-Bit Server VM, Vendor:Oracle Corporation
    WARN: source file /var/lib/neo4j/import/edges_header.csv has been specified multiple times, this may result in unwanted duplicates
    Neo4j version: 4.4.7
    Importing the contents of these files into /var/lib/neo4j/data/databases/contacts:
    Nodes:
      [PERSON]:
      /var/lib/neo4j/import/nodes_header.csv
      /var/lib/neo4j/import/ny_persontrait_epihiper_pure_data.csv

    Relationships:
      CONTACT_1:
      /var/lib/neo4j/import/edges_header.csv
      /var/lib/neo4j/import/network_30_pure_data

      CONTACT_0:
      /var/lib/neo4j/import/edges_header.csv
      /var/lib/neo4j/import/ny_contact_network_config_m_5_M_40_a_1000_m-contact_0_with_lid_pure_data.csv


    Available resources:
      Total machine memory: 376.4GiB
      Free machine memory: 47.07GiB
      Max heap memory : 31.00GiB
      Processors: 20
      Configured max memory: 310.8GiB
      High-IO: true


    Import starting 2022-06-16 16:00:19.419+0000
      Estimated number of nodes: 19.95 M
      Estimated number of node properties: 159.58 M
      Estimated number of relationships: 1.68 G
      Estimated number of relationship properties: 5.05 G
      Estimated disk space usage: 119.3GiB
      Estimated required memory usage: 1.241GiB

    (1/4) Node import 2022-06-16 16:00:19.462+0000
      Estimated number of nodes: 19.95 M
      Estimated disk space usage: 1.824GiB
      Estimated required memory usage: 1.241GiB
    .......... .......... .......... .......... ..........   5% ∆1s 828ms
    .......... .......... .......... .......... ..........  10% ∆808ms
    .......... .......... .......... .......... ..........  15% ∆1s 3ms
    .......... .......... .......... .......... ..........  20% ∆603ms
    .......... .......... .......... .......... ..........  25% ∆802ms
    .......... .......... .......... ...-...... ..........  30% ∆402ms
    .......... .......... .......... .......... ..........  35% ∆0ms
    .......... .......... .......... .......... ..........  40% ∆1ms
    .......... .......... .......... .......... ..........  45% ∆1s 402ms
    .......... .......... .......... .......... ..........  50% ∆201ms
    .......... .......... .......... .......... ..........  55% ∆202ms
    .......... .......... .......... .......... ..........  60% ∆201ms
    .......... .......... .......... .......... ..........  65% ∆337ms
    .......... .......... .......... .......... ..........  70% ∆1ms
    .......... .......... .......... .......... ..........  75% ∆0ms
    .......... .......... .......... .......... ..........  80% ∆0ms
    .......... .......... .......... .......... ..........  85% ∆1ms
    .......... .......... .......... .......... ..........  90% ∆0ms
    .......... .......... .......... .......... ..........  95% ∆1ms
    .......... .......... .......... .......... .......... 100% ∆0ms

    Node import COMPLETED in 8s 493ms

    (2/4) Relationship import 2022-06-16 16:00:27.956+0000
      Estimated number of relationships: 1.68 G
      Estimated disk space usage: 117.5GiB
      Estimated required memory usage: 1.205GiB
    .......... .......... .......... .......... ..........   5% ∆23s 487ms
    .......... .......... .......... .......... ..........  10% ∆24s 842ms
    .......... .......... .......... .......... ..........  15% ∆24s 818ms
    .......... .......... .......... .......... ..........  20% ∆24s 824ms
    .......... .......... .......... .......... ..........  25% ∆24s 420ms
    .......... .......... .......... .......... ..........  30% ∆24s 823ms
    .......... .......... .......... .......... ..........  35% ∆24s 17ms
    .......... .......... .......... .......... ..........  40% ∆24s 227ms
    .......... .......... .......... .......... ..........  45% ∆24s 229ms
    .......... .......... .......... .......... ..........  50% ∆23s 876ms
    .......... .......... .......... .......... ..........  55% ∆24s 419ms
    .......... .......... .......... .......... ..........  60% ∆24s 218ms
    .......... .......... .......... .......... ..........  65% ∆24s 915ms
    .......... .......... .......... .......... ..........  70% ∆24s 37ms
    .......... .......... .......... .......... ..........  75% ∆24s 418ms
    .......... .......... .......... .......... ..........  80% ∆24s 22ms
    .......... .......... .......... .......... ..........  85% ∆24s 445ms
    .......... .......... .......... .......... ..........  90% ∆2s 634ms
    .......... .......... .......... .......... ..........  95% ∆1ms
    .......... .......... .......... .......... .......... 100% ∆0ms

    Relationship import COMPLETED in 6m 56s 672ms

    (3/4) Relationship linking 2022-06-16 16:07:24.628+0000
      Estimated required memory usage: 1.182GiB
    .......... .......... .......... .......... ..........   5% ∆12s 414ms
    .......... .......... .......... .......... ..........  10% ∆7s 11ms
    .......... .......... .......... .......... ..........  15% ∆7s 410ms
    .......... .......... .......... .......... .........-  20% ∆400ms
    .......... .......... .......... .......... ..........  25% ∆20s 419ms
    .......... .......... .......... .......... ..........  30% ∆20s 621ms
    .......... .......... .......... .......... ..........  35% ∆22s 613ms
    .......... .......... .......... .......... ..........  40% ∆16s 415ms
    .......... .......... .......... .......... ..........  45% ∆22s 619ms
    .......... .......... .......... .......... ..........  50% ∆23s 815ms
    .......... .......... .......... .......... ..........  55% ∆20s 213ms
    .......... .......... .......... .......... .........-  60% ∆400ms
    .......... .......... .......... .......... ..........  65% ∆22s 620ms
    .......... .......... .......... .......... ..........  70% ∆32s 220ms
    .......... .......... .......... .......... ..........  75% ∆24s 224ms
    .......... .......... .......... .......... ..........  80% ∆16s 611ms
    .......... .......... .......... .......... ..........  85% ∆26s 219ms
    .......... .......... .......... .......... ..........  90% ∆16s 15ms
    .......... .......... .......... .......... ..........  95% ∆15s 413ms
    .......... .......... .......... .......... .......... 100% ∆29s 400ms

    Relationship linking COMPLETED in 6m 35s 547ms

    (4/4) Post processing 2022-06-16 16:14:00.176+0000
      Estimated required memory usage: 1020MiB
    .......... ......-... .......... ......-... .....-....   5% ∆1s 1ms
    .....-.... .......... .......... .......... ..........  10% ∆5s 410ms
    .......... .......... .......... .......... ..........  15% ∆5s 814ms
    .......... .......... .......... .......... ..........  20% ∆6s 205ms
    .......... .......... .......... .......... ..........  25% ∆6s 207ms
    .......... .......... .......... .......... ..........  30% ∆6s 207ms
    .......... .......... .......... .......... ..........  35% ∆6s 408ms
    .......... .......... .......... .......... ..........  40% ∆6s 5ms
    .......... .......... .......... .......... ..........  45% ∆5s 809ms
    .......... .......... .......... .......... ..........  50% ∆6s 207ms
    .......... .......... .......... .......... ..........  55% ∆6s 807ms
    .......... .......... .......... .......... ..........  60% ∆6s 810ms
    .......... .......... .......... .......... ..........  65% ∆6s 606ms
    .......... .......... .......... .......... ..........  70% ∆6s 805ms
    .......... .......... .......... .......... ..........  75% ∆6s 406ms
    .......... .......... .......... .......... ..........  80% ∆6s 807ms
    .......... .......... .......... .......... ..........  85% ∆6s 407ms
    .......... .......... .......... .......... ..........  90% ∆6s 609ms
    .......... .......... .......... .......... ..........  95% ∆6s 605ms
    .......... .......... .......... .......... .......... 100% ∆4s 825ms

    Post processing COMPLETED in 2m 19s 265ms


    IMPORT DONE in 16m 591ms. 
    Imported:
      18120752 nodes
      1438967534 relationships
      4461868618 properties
    Peak memory usage: 2.327GiB
    ```
  - *debug.log*
    ```
    2022-06-16 16:00:18.292+0000 INFO  [o.n.k.i.s.f.RecordFormatSelector] Record format not configured, selected default: RecordFormat:PageAlignedV4_3[AF4.3.0]
    2022-06-16 16:00:18.850+0000 INFO  [o.n.i.b.ImportLogic] Import starting
    2022-06-16 16:16:19.451+0000 INFO  [o.n.i.b.ImportLogic] Import completed successfully, took 16m 591ms. Imported:
      18120752 nodes
      1438967534 relationships
      4461868618 properties
    ```
  - Space consumption
    - After loading and DB creation: 
      - *data* folder: 106GB
      - Expansion ratio: $\frac{106}{1.3 + 28 + 24} = 1.98$ (same as that in the sync testing)
  - Memory consumption
    - After loading and DB creation
      - Idle memory: ~28GB
      - After fully started: ~62GB

**4. Indexing and Constraints**
  - Creating indexes on node properties is fast, while creating indexes on edge properties are slow. This is similar to what we observed in the sync testing. 
  - Memory consumption
    - ~240GB after the indexing (13 indexes) and restarting the server
  - Space consumption
    - After indexing
      - *data* folder: 187GB
      - Expansion ratio: $\frac{187}{1.3 + 28 + 24} = 3.5$

**5. Testing for GDS**
  - Create Native Projections
    - Command:
      ```
      call gds.graph.project('init_cn', 'PERSON', 'CONTACT_0')
           yield graphName AS graph,
           relationshipProjection AS knowsProjection,
           nodeCount AS nodes,
           relationshipCount AS rels;
      ```
    - Running time: can take several minutes, but not very long so far.
  - Test single-source shortest path with Dijkstra
    - Command:
      ```
      match (src:PERSON {hid: 0, county_fips: "36001", gender: 2, age_group: "a", pid: 0, age: 28})
           CALL gds.allShortestPaths.dijkstra.stream('init_cn', {
               sourceNode: src
           })
           YIELD index, sourceNode, targetNode, totalCost, nodeIds, costs, path
           RETURN
               index,
               gds.util.asNode(sourceNode).name AS sourceNodeName,
               gds.util.asNode(targetNode).name AS targetNodeName,
               totalCost,
               [nodeId IN nodeIds | gds.util.asNode(nodeId).name] AS nodeNames,
               costs,
               nodes(path) as path
           ORDER BY index
      ```
    - **Question**: W.r.t. the parallelism, I noticed that there are 15 working threads in the running state. I didn't set up the `concurrency` parameter for `gds.allShortestPaths.dijkstra.stream`, and by default its value is supposed to be $4$ (according to the document: https://neo4j.com/docs/graph-data-science/current/algorithms/dijkstra-single-source/). However, it seems more than $4$ threads working for this algorithm and less than the number of logic cores offered by the machine (i.e. $40$). Does this look normal?
      - Answer: This may be caused by the overwhelmingly large amount of returned results. Consider the simpler test in the below. 
        - **Follow-up Question**: However, as shown in the results in the below, the number of returned paths is `17,525,513` which honestly could hardly be considered as an unbearable number on Rivanna machines. If running throught the single-source Dijkstra needs merely a couple of minutes, then what was the Neo4j server doing for the rest of time? Preparing the output? This doesn't sound reasonable. Further, we observed that during the runtime many threads kept running constantly while there was nearly no change in memory consumption. This looks like infinite loop. So what happened? 
    - Running time: > 17 hours (hasn't run through once)
    - A simpler test
      - Command:
        ```
        MATCH (src:PERSON {hid: 0, county_fips: "36001", gender: 2, age_group: "a", pid: 0, age: 28})
           CALL gds.allShortestPaths.dijkstra.stream('init_cn', {sourceNode: src })
           YIELD targetNode, totalCost 
           RETURN avg(totalCost) as avgCost, count(*) as pathCount;
        ```
      - Returned results:
        - avgCost: 6.681244537606326
        - pathCount: 17,525,513
      - Running time: a couple of minutes
