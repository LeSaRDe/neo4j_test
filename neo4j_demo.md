# Neo4j Demo

## 0. Motivation
EpiHiper works on contact networks and output modified contact networks as well as some addditional simulation results. Can we maintain these data into data structures and make analyses on the networks and results over time? A series of questions need to be answered such as how to store the contact networks (both in memory and disc), how to make queries over the networks, how to handle the time series, can we do these efficiently, and etc. 


## 1. Environment Preparation
- **Mandatory**: 
  - Jave JDK 11 (Oracle or OpenJDK

- **Optional**:
  - Docker/Singularity (only when Neo4j is deployed on VMs) 


## 2. Installation
https://neo4j.com/docs/operations-manual/current/installation/


## 3. Singularity Image Preparation
Converted from official Docker image.
- **Official Docker images**
  - Download from here: https://hub.docker.com/_/neo4j/?tab=tags
  - Mind the Docker tag you want to use. `neo4j:latest` and `neo4j:community` are fine for the free version. 
  - If you want to try Docker images first, read this: https://neo4j.com/developer/docker/
- **Singularity Documents**
  - Find your Singularity version from here: https://sylabs.io/
  - Need a *container definition file*. What is it? See here: https://sylabs.io/guides/3.7/user-guide/definition_files.html
  - Our definition file saved in name `neo4j_singularity.def`:
    ```
    Bootstrap: docker
    From: neo4j:latest

    %post
    chmod -R 777 /var/lib/neo4j
    echo "dbms.connector.bolt.listen_address=0.0.0.0" >> /var/lib/neo4j/conf/neo4j.conf
    echo "dbms.connector.https.listen_address=0.0.0.0" >> /var/lib/neo4j/conf/neo4j.conf
    echo "dbms.connector.http.listen_address=0.0.0.0" >> /var/lib/neo4j/conf/neo4j.conf
    echo "dbms.security.auth_enabled=false" >> /var/lib/neo4j/conf/neo4j.conf
    echo "dbms.security.procedures.unrestricted=my.extensions.example,my.procedures.*,apoc.*" >> /var/lib/neo4j/conf/neo4j.conf
    echo "dbms.security.procedures.allowlist=apoc.coll.*,apoc.load.*,gds.*,apoc.*" >> /var/lib/neo4j/conf/neo4j.conf
    echo "apoc.import.file.enabled=true" >> /var/lib/neo4j/conf/neo4j.conf
    echo "dbms.memory.heap.initial_size=31g" >> /var/lib/neo4j/conf/neo4j.conf
    echo "dbms.memory.heap.max_size=31g" >> /var/lib/neo4j/conf/neo4j.conf
    echo "dbms.memory.pagecache.size=80g" >> /var/lib/neo4j/conf/neo4j.conf

    %startscript
    tini -s -g -- /docker-entrypoint.sh neo4j
    ```
    `Bootstrap: docker` tells the builder to search for Docker images. `From: neo4j:latest` gives the exact tag of the desired Docker image. In `%post` section, some networking and plugin settings are configured. `%startscript` section gives the essential command to start the Neo4j server. `docker-entrypoint.sh` is used to check and set up some runtime configurations for Neo4j, and finally starts the server. The file can be found here: https://github.com/LeSaRDe/neo4j_test/blob/master/docker-entrypoint.sh
    `dbms.security.auth_enabled=false` disables the authentication of Neo4j. And the last four settings are suggested by the Neo4j team.
  - The definition file can also be found here: https://github.com/LeSaRDe/neo4j_test/blob/master/neo4j_singularity.def
  - Build Singularity image:
    ```
    sudo singularity build neo4j_community.sif neo4j_singularity.def
    ``` 
    May have to run this command locally if you don't have the root on Rivanna. More about `singularity build` can be fonud here: https://sylabs.io/guides/3.7/user-guide/build_a_container.html
  - Upload to Rivanna:
    Typically, it is put in here: `/project/biocomplexity/mf3jh/neo4j_workspace/img/neo4j_community.sif`


## 4. Run Neo4j SIF As a Server on Rivanna Computing Node
For convenience, the runtime folders are exposed to outside (e.g. config files and logs).
- Bash Script for Running Neo4j: https://github.com/LeSaRDe/neo4j_test/blob/master/launch_neo4j.sh
- Slurm Script for Running Neo4j on Rivanna: https://github.com/LeSaRDe/neo4j_test/blob/master/launch_neo4j.slurm


## 5. Access Neo4j Server From Another Machine
Typically, `cypher-shell` is used for accessing Neo4j servers. There are two steps.
- (1) Run Singularity Shell. Bash script for this:  https://github.com/LeSaRDe/neo4j_test/blob/master/launch_cypher_shell.sh
- (2) Run `cypher-shell` inside the Singuilarity shell using the command:
  ```bash
  cypher-shell -a [NEO4j HOST NAME]
  ```
  `[NEO4j HOST NAME]` is the host name of the machine running the Neo4j server. 


## 6. Driver for Python Interface
- All communications between Python and Neo4j need a driver for bridging (as most DB drivers do). 
- TICSMTC: https://github.com/LeSaRDe/neo4j_test/blob/master/neo4j_ops.py
- More details about driver construction can be found here: https://neo4j.com/docs/api/python-driver/current/api.html#driver-construction


## 7. Create Graph DB, Indexes and Constraints
- The Community edition does not support creating databases (though by some trick techniques this can be done anyway).
- For the Community edition, the only database can be used is the built-in one named `neo4j`. 
- The Community edition does not support creating constraints either. 
- Creating indexes is supported by both editions. 
- Details about database management can be found here: https://neo4j.com/docs/cypher-manual/current/databases/
- Details about constraints can be found here: https://neo4j.com/docs/cypher-manual/current/constraints/
- Details about indexes can be found here: https://neo4j.com/docs/cypher-manual/current/indexes-for-search-performance/
- Example of creating database:
  ```sql
  create database DBNAME if not exists
  ```
- Example of creating constraint:
  
  The uniqueness of PID for each person (as a node in DB).
  ```sql
  create constraint pid_unique if not exists on (n:PERSON) assert n.pid is unique
  ```
- Example of creating index:
  
  Create a BTree index for the *age* attribute of person. 
  ```sql
  create btree index idx_age if not exists for (n:PERSON) on (n.age)
  ```
- Take a look at the data structures and indexes for EpiHiper contact networks. TICSMTC: https://github.com/LeSaRDe/neo4j_test/blob/master/neo4j_ops.py
  

## 8. Create Nodes
Two different methods were tried. 
- (A) First, we sort the input node/edge file; and second, use `CREATE` with/without multithreading. Commit batch by batch. 
- (B) First, we sort the input node/edge file; and second, use `apoc.periodic.iterate()` and `apoc.load.csv()` with parallelization. 
- (C) First, we sort the input node/edge file; second, we split the sorted file into a specific number of partitions; and third, we use multiple physical machines in parallel, and on each machine we apply Method (B) to a partition.
- (D) Same as Method (C) except that we do not define indexed before loading.
- Experimental Settings:
  - WY data: /project/bii_nssac/COVID-19_USA_EpiHiper/rivanna/20211020-network_query/
    - Data description: https://github.com/NSSAC/EpiHiper-network_analytics 
    - Number of nodes: $548,603$
    - Number of edges in the initial contact network: $21,685,569$
    - $32$ Threads (Rivanna computing node, exclusive mode).
    - Batch size: $100,000$
  - VA data: 
    - Number of nodes: $7,688,060$
    - Number of edges in the initial contact network: $371,888,621$
  - NY data:
    - Number of nodes: $18,120,753$
    - Number of edges in the initial contact network: $782,828,605$
  - CA data:
    - Number of nodes: $35,516,054$
    - Number of edges in the initial contact network: $1,402,087,302$
- Using WY Data:
  - With/without predefined indexes, running times are similar. 
  - Method (A): $< 1$ min (no significant difference between `cypher-shell` and Python), $30 \sim 60$ seconds for each thread. 
  - Method (A) in a single thread: $> 70$ seconds. 
  - Method (B): several seconds. 
  - Creating indexes after creating nodes is very fast in this case, though the running time may vary for larger data. 
- `apoc`, though also uses parallelization, processes data in some different ways. 
- TICSMTC: https://github.com/LeSaRDe/neo4j_test/blob/master/neo4j_ops.py
- Using VA Data:
  - Method (B): $\sim 5$ minutes.
- Using NY Data:
  - Method (B): $\sim 35$ minutes.

## 9. Create Edges for Initial Contact Network
Similar to creating nodes. 
- Some Results:
  - Method (B): Using WY data, $\sim 15$ minutes.
  - Method (A): Using VA data, more than $30$ hours using a single thread. 
  - Method (B) (reported by Neo4j using Enterprise): Using VA data, $\sim 4.5$ hours. 
  - Method (B) (using Community): Using VA data, $\sim 11$ hours.
  - Method (C): Using WY data,
  
    | CN Partitioned Edge File | Lines  | Time |
    |---|---|---|
    |wy_init_cn_split_0|2168577|10 min|
    |wy_init_cn_split_1|2168578|11 min|
    |wy_init_cn_split_2|2168578|10 min|
    |wy_init_cn_split_3|2168578|9 min|
    |wy_init_cn_split_4|2168578|9 min|
    |wy_init_cn_split_5|2168578|4 min|
    |wy_init_cn_split_6|2168578|8 min|
    |wy_init_cn_split_7|2168578|4 min|
    |wy_init_cn_split_8|2168578|4 min|
    |wy_init_cn_split_9|2168369|4 min|
  - Method (C): Using VA data,

    | CN Partitioned Edge File | Lines  | Time |
    |---|---|---|
    |va_init_cn_split_0|37188862|9.2 hours|
    |va_init_cn_split_1|37188863|8.4 hours|
    |va_init_cn_split_2|37188863|7.3 hours|
    |va_init_cn_split_3|37188863|5 hours|
    |va_init_cn_split_4|37188863|5 hours|
    |va_init_cn_split_5|37188863|8 hours|
    |va_init_cn_split_6|37188863|5.7 hours|
    |va_init_cn_split_7|37188863|3.2 hours|
    |va_init_cn_split_8|37188863|5.6 hours|
    |va_init_cn_split_9|37188854|3.6 hours|
  - Method (D): Using WY data,
    - **CAUTION!!!**
      When using Method (D), it is very likely to run into dead lock. Though, so far it is unknown to us why this dead lock happens.
    - **CAUTION!!!**
      If we set the number of partitions to be 1 (i.e. no spliting), then Method (D) is very likely to overwhelm the JVM with our current settings. A lot of "stop-the-world" pauses occurred. 


## 10. Create Edges for Intermediate Contact Networks
Similar as before. 
- A key concept capturing the time series is the `occur` attribute at edges. `occur = -1` for the initial contact network, and `occur > 0` for intermediate networks.
- Some Results:
  - Method (B): Using WY data, $\sim 1.7$ hours for loading in the first 5 intermediate contact networks in sequence, and $\sim 5.8$ hours for loading in from $5$ to $15$. 
  
    | CN Edge File | Lines  | Time |
    |---|---|---|
    |Initial|21685569|15 min|
    |network[0]|21685569|23 min|
    |network[1]|21685569|18 min|
    |network[2]|20111489|17 min|
    |network[3]|20111489|17 min|
    |network[4]|20111489|30 min|
    |network[5]|20111489|246 min|
    |network[6]|20111489|24 min|
    |network[7]|20111489|16 min|
    |network[8]|20079945|33 min|
    |network[9]|20040281|23 min|
    |network[10]|19991167|48 min|
    |network[11]|19941589|92 min|
    |network[12]|19886993|18 min|
    |network[13]|19841011|38 min|
    |network[14]|19782673|16 min|
    |network[15]|19729413|22 min|
  - The size of initial contact networks and the first $5$ intermediate contact networks is about $7$GB. After loading, the database size is about $62$GB. The size of initial contact network and the first $16$ intermediate contact network is about $20.7$GB. After loading, the database size is about $130$GB. The expansion ratio is about $6 \sim 9$.


## 11. Create SQLite DB for EpiHiper Output
When using Neo4j to process EpiHiper's data, my methodology is *keep networks inside Neo4j and the rest outside*. Sounds trivial, but it may become much less trivial when things turn complex. 
- Create DB, Load output data, Create indexes.
- Running time is trivail so far. 
- Take a look at the data structure and indexes. TICSMTC: https://github.com/LeSaRDe/neo4j_test/blob/master/neo4j_ops.py


## 12. A Concrete Scenario
Pull out the distribution of durations w.r.t. an exit state. 
- Given an exit state: `Isymp_s`
- Fetch PIDs over time for this exit state (SQLite).
- Find contacts incoming to these PIDS for each time point. Translated: query incoming 1-nearest-neighbors of a given set of nodes for each time point. (Neo4j)
- Get the distribution of durations.
- TICSMTC: https://github.com/LeSaRDe/neo4j_test/blob/master/neo4j_ops.py


# 13. Link with SNAP/CINES
Execute a query on Neo4j, retrieve the subgraphs, convert subgraphs to SNAP TTables batch by batch. This is useful for some graph algorithms. Note that constructing TTables can be tricky, and the documents of SNAP is not perfect. 
- More about TTables: 
  - https://snap.stanford.edu/snappy/doc/reference/conv.html
  - https://snap.stanford.edu/snappy/doc/reference/table.html
- TICSMTC: https://github.com/LeSaRDe/neo4j_test/blob/master/neo4j_ops.py


# 14. Another Example (fast)
- Query population distribution grouped BY `age_group`
- Cypher code:
  ```cypher
  match (n)
  with distinct n.age_group as age_group
  unwind age_group as each_age_group
  match (m)
  where m.age_group = each_age_group
  return each_age_group, count(m)
  ```

- Results:
  
  | each_age_group | count(m) |
  |---|---|
  | "a"| 213047|
  | "p"| 35874|
  | "s"| 97906|
  | "g"| 82645|
  | "o"| 119131
  Running time: 2498 ms


## 15. One More Example (use apoc graph properties)
Query incoming degrees of infected people at a given time point. 
- Get infected PIDs
- Cypher code:
  ```cypher
  unwind $infect_pid as infect_pid
  match (n:PERSON {pid: infect_pid})
  return infect_pid, apoc.node.degree(n, "<CONTACT")
  ```
- Run it out. TICSMTC: https://github.com/LeSaRDe/neo4j_test/blob/master/neo4j_ops.py


# 16. The Last Example (Slow)
- Query source activity distribution
- Cypher code:
  ```cypher
  match ()-[r]->()
  with distinct r.src_act as src_act
  unwind src_act as each_src_act
  match ()-[q]->()
  where q.src_act = each_src_act
  return each_src_act, count(q)
  ```
   
- Results:
  
  | each_src_act | count(q) |
  |---|---|
  | "1:1"        | 22796760 |
  | "1:5"        | 15745284 |
  | "1:3"        | 4761003  |
  | "1:2"        | 38399970 |
  | "1:4"        | 39981876 |
  | "1:7"        | 157923   |
  | "1:6"        | 3548364  |

  Running time: 1154917ms = ~19min


# 17. Example Queries 
- (1) How many males between 18 and 24 years of age are newly infected (i.e., are just transitioned to state I or its variants [this could be a set of states]) between times 5 and 15 inclusive? What are the PIDs of these males?
  - Running Time: 0.4s
  - /project/biocomplexity/mf3jh/example_queries/query_1
- (2) What are the states that are transitioned to by males or females that have the activity of shopping on their incoming edges?
  - Running Time: 18s
  - /project/biocomplexity/mf3jh/example_queries/query_2
- (3) What are the states that are transitioned to by males between 75 and 90 or by females 32 to 39 where the other node of an edge, if there is an edge, is activity work?
  - Running Time: 47s
  - /project/biocomplexity/mf3jh/example_queries/query_3
- (5) What are the household IDs of households where the number of people in the household is at least 6 and there is at least one child between 8 and 14?
  - Running Time: 0.7s
  - /project/biocomplexity/mf3jh/example_queries/query_5


# Using "neo4j-admin import" CA node data 2.6 GB
## On Rivanna
- Exclusive mode
- Neo4j Enterprise
- Singularity image
- "neo4j-admin memrec" for JVM tuning
- Log as follows:
```
Selecting JVM - Version:11.0.15+10, Name:OpenJDK 64-Bit Server VM, Vendor:Oracle Corporation
WARN: source file /var/lib/neo4j/import/edges_header.csv has been specified multiple times, this may result in unwanted duplicates
WARN: source file /var/lib/neo4j/import/ca_contact_network_config_m_5_M_40_a_1000_m-contact_0_with_lid_pure_data.txt has been specified multiple times, this may result in unwanted duplicates
Neo4j version: 4.4.6
Importing the contents of these files into /var/lib/neo4j/data/databases/contacts:
Nodes:
  [PERSON]:
  /var/lib/neo4j/import/nodes_header.csv
  /var/lib/neo4j/import/ca_persontrait_epihiper_pure_date.csv

Relationships:
  CONTACT_1:
  /var/lib/neo4j/import/edges_header.csv
  /var/lib/neo4j/import/ca_contact_network_config_m_5_M_40_a_1000_m-contact_0_with_lid_pure_data.txt

  CONTACT_0:
  /var/lib/neo4j/import/edges_header.csv
  /var/lib/neo4j/import/ca_contact_network_config_m_5_M_40_a_1000_m-contact_0_with_lid_pure_data.txt


Available resources:
  Total machine memory: 376.6GiB
  Free machine memory: 350.3GiB
  Max heap memory : 31.00GiB
  Processors: 20
  Configured max memory: 311.0GiB
  High-IO: true


Import starting 2022-05-19 20:49:28.867+0000
  Estimated number of nodes: 38.42 M
  Estimated number of node properties: 307.36 M
  Estimated number of relationships: 3.26 G
  Estimated number of relationship properties: 9.77 G
  Estimated disk space usage: 231.0GiB
  Estimated required memory usage: 1.468GiB

(1/4) Node import 2022-05-19 20:49:28.963+0000
  Estimated number of nodes: 38.42 M
  Estimated disk space usage: 3.517GiB
  Estimated required memory usage: 1.468GiB
.......... .......... .......... .......... ..........   5% ∆6m 33s 517ms
.......... .......... .......... .......... ..........  10% ∆11m 15s 712ms
.......... .......... .......... .......... ..........  15% ∆13m 28s 85ms
.......... .......... .......... .......... ..........  20% ∆9m 50s 790ms
.......... .......... .......... .......... ..........  25% ∆13m 46s 480ms
.......... .......... .......... .......... ..........  30% ∆16m 20s 247ms
..-....... .......... .......... .......... ..........  35% ∆803ms
.......... .......... .......... .......... ..........  40% ∆0ms
.......... .......... .......... .......... ..........  45% ∆1s 1ms
.......... .......... .......... .......... ..........  50% ∆400ms
.......... .......... .......... .......... ..........  55% ∆200ms
.......... .......... .......... .......... ..........  60% ∆400ms
.......... .......... .......... .......... ..........  65% ∆200ms
.......... .......... .......... .......... ..........  70% ∆201ms
.......... .......... .......... .......... ..........  75% ∆0ms
.......... .......... .......... .......... ..........  80% ∆0ms
.......... .......... .......... .......... ..........  85% ∆3ms
.......... .......... .......... .......... ..........  90% ∆0ms
.......... .......... .......... .......... ..........  95% ∆1ms
.......... .......... .......... .......... .......... 100% ∆0ms

Node import COMPLETED in 1h 11m 18s 917ms

```

## On Fanchao's laptop
-Similar singularity image as that used on Rivanna, except the JVM settings.
- Log as follows:
```
Selecting JVM - Version:11.0.15+10, Name:OpenJDK 64-Bit Server VM, Vendor:Oracle Corporation
Neo4j version: 4.4.7
Importing the contents of these files into /var/lib/neo4j/data/databases/contacts:
Nodes:
  [PERSON]:
  /var/lib/neo4j/import/nodes_header.csv
  /var/lib/neo4j/import/ca_persontrait_epihiper_pure_date.csv


Available resources:
  Total machine memory: 30.98GiB
  Free machine memory: 263.2MiB
  Max heap memory : 11.52GiB
  Processors: 12
  Configured max memory: 17.51GiB
  High-IO: true

WARNING: heap size 11.52GiB is unnecessarily large for completing this import.
The abundant heap memory will leave less memory for off-heap importer caches. Suggested heap size is 1.003GiB
Import starting 2022-05-20 02:52:55.572+0000
  Estimated number of nodes: 38.42 M
  Estimated number of node properties: 307.36 M
  Estimated number of relationships: 0.00 
  Estimated number of relationship properties: 0.00 
  Estimated disk space usage: 3.510GiB
  Estimated required memory usage: 747.1MiB

(1/4) Node import 2022-05-20 02:52:55.635+0000
  Estimated number of nodes: 38.42 M
  Estimated disk space usage: 3.508GiB
  Estimated required memory usage: 747.1MiB
.......... .......... .......... .......... ..........   5% ∆6s 470ms
.......... .......... .......... .......... ..........  10% ∆5s 214ms
.......... .......... .......... .......... ..........  15% ∆3s 806ms
.......... .......... .......... .......... ..........  20% ∆3s 807ms
.......... .......... .......... .......... ..........  25% ∆3s 808ms
.......... .......... .......... .......... ..........  30% ∆3s 412ms
.......-.. .......... .......... .......... ..........  35% ∆749ms
.......... .......... .......... .......... ..........  40% ∆0ms
.......... .......... .......... .......... ..........  45% ∆1s 602ms
.......... .......... .......... .......... ..........  50% ∆1s 5ms
.......... .......... .......... .......... ..........  55% ∆1s 201ms
.......... .......... .......... .......... ..........  60% ∆1s 203ms
.......... .......... .......... .......... ..........  65% ∆804ms
.......... .......... .......... .......... ..........  70% ∆201ms
.......... .......... .......... .......... ..........  75% ∆201ms
.......... .......... .......... .......... ..........  80% ∆0ms
.......... .......... .......... .......... ..........  85% ∆14ms
.......... .......... .......... .......... ..........  90% ∆1ms
.......... .......... .......... .......... ..........  95% ∆0ms
.......... .......... .......... .......... .......... 100% ∆0ms

Node import COMPLETED in 33s 951ms
```

## Michael from Neo4j on laptop
- Log as follows:
```
Selecting JVM - Version:11.0.13+8-LTS, Name:OpenJDK 64-Bit Server VM, Vendor:Azul Systems, Inc.
WARN: source file /Users/mh/data/uni-va/neo4j-enterprise-4.4.6/../edges_header.csv has been specified multiple times, this may result in unwanted duplicates
Neo4j version: 4.4.6
Importing the contents of these files into /Users/mh/data/uni-va/neo4j-enterprise-4.4.6/data/databases/contacts:
Nodes:
  [Person]:
  /Users/mh/data/uni-va/neo4j-enterprise-4.4.6/../nodes_header.csv
  /Users/mh/data/uni-va/neo4j-enterprise-4.4.6/../initial_contact_network_nodes.csv.gz

Relationships:
  CONTACT_1:
  /Users/mh/data/uni-va/neo4j-enterprise-4.4.6/../edges_header.csv
  /Users/mh/data/uni-va/neo4j-enterprise-4.4.6/../intermediate_network_i_edges.csv.gz

  CONTACT_0:
  /Users/mh/data/uni-va/neo4j-enterprise-4.4.6/../edges_header.csv
  /Users/mh/data/uni-va/neo4j-enterprise-4.4.6/../initial_contact_network_edges.csv.gz


Available resources:
  Total machine memory: 64.00GiB
  Free machine memory: 7.863GiB
  Max heap memory : 16.00GiB
  Processors: 10
  Configured max memory: 43.20GiB
  High-IO: true


Import starting 2022-05-13 01:02:18.344+0200
  Estimated number of nodes: 34.78 M
  Estimated number of node properties: 278.22 M
  Estimated number of relationships: 2.69 G
  Estimated number of relationship properties: 8.07 G
  Estimated disk space usage: 191.1GiB
  Estimated required memory usage: 1.424GiB

(1/4) Node import 2022-05-13 01:02:18.350+0200
  Estimated number of nodes: 34.78 M
  Estimated disk space usage: 3.174GiB
  Estimated required memory usage: 1.424GiB
.......... .......... .......... .......... ..........   5% ∆1s 642ms
.......... .......... .......... .......... ..........  10% ∆2s 175ms
.......... .......... .......... .......... ..........  15% ∆2s 54ms
.......... .......... .......... .......... ..........  20% ∆2s 463ms
.......... .......... .......... .......... ..........  25% ∆2s 51ms
.......... .......... .......... .......... ..........  30% ∆2s 49ms
.......... .......... .......... .........- ..........  35% ∆152ms
.......... .......... .......... .......... ..........  40% ∆203ms
.......... .......... .......... .......... ..........  45% ∆0ms
.......... .......... .......... .......... ..........  50% ∆403ms
.......... .......... .......... .......... ..........  55% ∆201ms
.......... .......... .......... .......... ..........  60% ∆200ms
.......... .......... .......... .......... ..........  65% ∆204ms
.......... .......... .......... .......... ..........  70% ∆202ms
.......... .......... .......... .......... ..........  75% ∆0ms
.......... .......... .......... .......... ..........  80% ∆0ms
.......... .......... .......... .......... ..........  85% ∆0ms
.......... .......... .......... .......... ..........  90% ∆3ms
.......... .......... .......... .......... ..........  95% ∆0ms
.......... .......... .......... .......... .......... 100% ∆0ms

Node import COMPLETED in 15s 686ms
```



```
(2/4) Relationship import 2022-05-19 22:00:47.881+0000
  Estimated number of relationships: 3.26 G
  Estimated disk space usage: 227.4GiB
  Estimated required memory usage: 1.397GiB
.......... .......... .......... .......... ..........   5% ∆3h 36m 10s 195ms
.......... .......... .......... .......... ..........  10% ∆4h 17m 26s 157ms
.......... .......... .......... .......... ..........  15% ∆7h 46m 59s 190ms

```


## Rivanna Eight Special Large Local Storage Machines
- Machines: *udc-aj36-[1-4]c[0-1]*
- Local storage info: e.g. on *udc-aj36-4c0*
  
|NAME|MAJ:MIN|RM|SIZE|RO|FSTYPE|MOUNTPOINT|UUID|MODEL|ROTA|DISC-MAX|SERIAL|VENDOR|
|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|
|sda|8:0|0|1.8T|0|ext4|/localscratch|d1294e1e-bfdf-4564-a13a-64a1c39ad844|MZ7KH1T9HAJR0D3|0|4G|S47JNA0M703738|ATA|

- The model number "MZ7KH1T9HAJR0D3" is very likely to be "Samsung Sm883 1.92 TB Internal SSD 2.5 inch". More details can be found in here: https://semiconductor.samsung.com/ssd/datacenter-ssd/sm883/mz7kh1t9hajr/
- Usage: add *--nodelist=udc-aj36-4c0* to the SBATCH script
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
- Testing data:
  - CA sized
  - Nodes: 35,516,052
  - Initial Contact Network: 1,402,087,300
  - Intermediate Contact Network: 1,205,361,116
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
  - *data* folder: 190GB
  - raw data size: 2.6GB + 49GB + 43GB = 94.6GB
  - Expansion ratio after loading: 190GB / 94.6GB = 2.00