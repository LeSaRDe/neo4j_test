# HOW TO RUN NEO4J ON RIVANNA

## Run Neo4j Docker Image Using Singularity On Rivanna

Neo4j, both the Community and Enterprise versions, is provided Docker images. The details about how to run the Docker images can be found in here: https://neo4j.com/developer/docker-run-neo4j/ However, Rivanna does not support Docker, and we have to convert Docker images to Singularity images. Some details can be found in here: https://www.rc.virginia.edu/userinfo/howtos/rivanna/docker-images-on-rivanna/ 

### Build Singularity Image For Neo4j

Here is a Singularity definition file for building a Neo4j image (thanks to Dustin), and this file can be found in here: https://github.com/NSSAC/EpiHiper-network_analytics/blob/master/neo4j.build.def

```
Bootstrap: docker
From: neo4j:enterprise

%setup
mkdir -p $SINGULARITY_ROOTFS/input
mkdir -p $SINGULARITY_ROOTFS/output
mkdir -p $SINGULARITY_ROOTFS/job
mkdir -p $SINGULARITY_ROOTFS/data

%post
chmod -R 777 /var/lib/neo4j
echo "dbms.directories.data=/data" >> /var/lib/neo4j/conf/neo4j.conf
echo "dbms.directories.transaction.logs.root=/data/transactions" >> /var/lib/neo4j/conf/neo4j.conf
echo "dbms.connector.bolt.listen_address=0.0.0.0" >> /var/lib/neo4j/conf/neo4j.conf
echo "dbms.connector.https.listen_address=0.0.0.0" >> /var/lib/neo4j/conf/neo4j.conf
echo "dbms.connector.http.listen_address=0.0.0.0" >> /var/lib/neo4j/conf/neo4j.conf

%startscript
tini -s -g -- /docker-entrypoint.sh neo4j
```

Note that the version string **neo4j:enterprise** in the **From** line is critical, which specifies the exact version of Neo4j to be used. This version string is the tag of a Neo4j Docker image published officially, and the whole list of such tags can be found in here: https://hub.docker.com/_/neo4j/?tab=tags Also, the last line defines how exactly the Neo4j server will be started. Although it may be valid to start it in some other ways (e.g. `neo4j start`), it is more official and robust to keep the startup following the way shown above. 

Then with this definition file we can build a singularity image (locally, i.e. with sudo) as follows:

```bash
sudo singularity build neo4j.sif neo4j.build.def
```

Note that by using this command we do not have to download the Docker image of Neo4j beforehand. The command will do this for us. And the version to be downloaded has been specified in the definition file as described above. If this command succeeds, an image file named **neo4j.sif** (as shown in the command and can be changed to any proper name) will be created. The building procedure will be logged and printed on screen as follows:

```
INFO:    Starting build...
Getting image source signatures
Copying blob e8b689711f21 done  
Copying blob 2bf2b8c78141 done  
Copying blob 62763247decf done  
Copying blob 007baf3afc22 done  
Copying blob 4016ac4fbf0d done  
Copying blob 092d8ace2e3e done  
Copying blob e985dc8badef done  
Copying blob 423e1f0b0344 done  
Copying config 7339775a0a done  
Writing manifest to image destination
Storing signatures
2021/11/15 23:28:39  info unpack layer: sha256:e48a28e58eeee480b5ffb771ad8baef9f36fa5a69bbdda8628eea5ea1e8cf757
2021/11/15 23:28:40  info unpack layer: sha256:b1c7b763aa5113a2afc5aec91324832359ef6a88c0b4ab0080e75cd7420a0a8d
2021/11/15 23:28:40  info unpack layer: sha256:bd4653ee8de080503fc78969b4c365572fbef52a156c827d5951928716e7a2a1
2021/11/15 23:28:40  info unpack layer: sha256:db0d435d99e92d2577365abf07791b6982fcf83c3577ef2b4b5c66c98d7fd737
2021/11/15 23:28:42  info unpack layer: sha256:56edb77d7402541a4c7dfdebac70d8fac579d2051fbf780d0a1df68c20500d82
2021/11/15 23:28:42  info unpack layer: sha256:8507a3a6f8d9e8a1dabe836f50edf7d634a9ed7d7bff78a97c933a9fccc8c10e
2021/11/15 23:28:42  info unpack layer: sha256:64d6fe1724fa2a06acb297920d0e7e454698b60b01869ca5803cd3df7d7fcb73
2021/11/15 23:28:43  info unpack layer: sha256:06206c01ced0080194368d8668369e642108c9d08b3b814f129192b94978ffb2
INFO:    Creating SIF file...
INFO:    Build complete: neo4j_enterprise_4.3.7.sif
```

When the Singularity image is ready, we upload it to Rivanna. And it can be found in here: `/project/biocomplexity/singularity_images/neo4j_enterprise_4.3.7.sif`

### Run Neo4j Singularity Image As A Service On A Rivanna Computing Node

To run the Neo4j Singularity image as a service, we need some configurations. First, as we may need to access and modify files used by Neo4j (e.g. the Neo4j config file), we would like to have them exposed to us. Second, as we will send the service to a computing node, we need to know how to access Neo4j remotely. The following bash script helps with these (again, thanks to Dustin):

```bash
#!/bin/sh

LDB=/project/biocomplexity/mf3jh/neo4j_workspace/data
CONF=/project/biocomplexity/mf3jh/neo4j_workspace/conf
PLUGINS=/project/biocomplexity/mf3jh/neo4j_workspace/plugins
LOGS=/project/biocomplexity/mf3jh/neo4j_workspace/logs
IMAGE=/project/biocomplexity/singularity_images/neo4j_enterprise_4.3.7.sif
echo "Local DB Folder ${LDB}"

mkdir -p ${LDB}
mkdir -p ${CONF}
mkdir -p ${PLUGINS}
mkdir -p ${LOGS}

echo "Neo4j instance name: $SLURM_JOB_ID"
singularity instance start --env=NEO4J_ACCEPT_LICENSE_AGREEMENT=yes --writable-tmpfs  --bind ${LDB}:/data,${CONF}:/var/lib/neo4j/conf,${PLUGINS}:/var/lib/neo4j/plugins,${LOGS}:/logs ${IMAGE} $SLURM_JOB_ID
#singularity instance list
#singularity exec instance://$SLURM_JOB_ID neo4j start

handler()
{
  echo "Closing Database..."
  #shutdown the db cleanly
  echo "Stop DB"
  # we need to add a stop script in here for neo4j
  #singularity run --app stop instance://$SLURM_JOB_ID
  singularity exec instance://$SLURM_JOB_ID neo4j stop

  # stop the singularity instance
  echo "Stop Singularity Instance"
  singularity instance stop $SLURM_JOB_ID

  touch ${LDB}/SHUTDOWN
}

echo "Waiting for DB Completion..."
while [ ! -f "${LDB}/SHUTDOWN" ] ;do
  sleep 10
done;

trap handler SIGINT
```

This script can be found in here: https://github.com/NSSAC/EpiHiper-network_analytics/blob/master/launch_neo4j.sh We map the **config**, **logs**, **data** and **plugins** folders used by Neo4j (i.e. inside the image) to local real folders. We are able to access and modify the files in these folders. Nevertheless, in some cases, for example, after modifying the config file, we have to `scancel` the job of this bash script and `sbatch` it again so that the changes will be effective. When the job of this script is running on a computing node, we can retrieve the **hostname** (e.g. `udc-aj34-36`) of the computing node by using `squeue`. And this **hostname** will be used when we connect to the Neo4j server running on the computing node from a local *cypher-shell* or APIs. 

To run this script in a computing node, we use a slurm script:

```
#!/bin/bash

#SBATCH --time=3-00:00:00
#SBATCH --nodes=1
#SBATCH --mem=350000
#SBATCH --partition=bii-gpu
#SBATCH --exclusive
#SBATCH --cpus-per-task=20
#SBTACH --core-spec=40
#SBATCH --gres=gpu:v100:4

module load anaconda
source activate my_venv
conda deactivate
source activate my_venv

./launch_neo4j.sh
```

The script can be found in here: https://github.com/NSSAC/EpiHiper-network_analytics/blob/master/launch_neo4j.slurm Note that this script needs to be modified for different users. In addition, as our application scenarios can be easily intense on memory consumption, it can be a good idea to keep the `--exclusive` option to `sbatch` without sharing the resource on the computing node with other jobs. 

Also note that the bash script `launch_neo4j.sh` can be run locally on a computing node if one uses `ijob` to interact with the computing node. In this case, the slurm script is not necessary anymore. 

If the Neo4j server is successfully started on the computing node, usually we can read some `ps` log from the slurm output file like this:

```bash
Local DB Folder /project/biocomplexity/mf3jh/neo4j_workspace/data
Neo4j instance name: 28731971
INFO:    instance started successfully
mf3jh     12830  0.0  0.0 113304  1492 ?        S    13:07   0:00      \_ /bin/sh ./launch_neo4j.sh
mf3jh     13571  0.0  0.0 112824   960 ?        S    13:07   0:00          \_ grep neo4j
mf3jh     12881  0.0  0.0   2300   372 ?        S    13:07   0:00          \_ tini -s -g -- /docker-entrypoint.sh neo4j
mf3jh     12882  256  0.3 42826896 1305536 ?    Sl   13:07   0:25              \_ /usr/local/openjdk-11/bin/java -cp /var/lib/neo4j/plugins/*:/var/lib/neo4j/conf/*:/var/lib/neo4j/lib/* -Dfile.encoding=UTF-8 com.neo4j.server.enterprise.EnterpriseEntryPoint --home-dir=/var/lib/neo4j --config-dir=/var/lib/neo4j/conf
Waiting for DB Completion...
```

Particularly, notice that the `java` process should be running with no trouble is the Neo4j server is in the normal running status. Though, this is not a sufficient condition to determine if the Neo4j server is healthy. Checking `debug.log` file in the log folder (i.e. the environment variable named `LOGS`) specified in `launch_neo4j.sh` is also helpful. In addition, running the following command (substituting `[hostname of Neo4j server]` with the real **hostname** of the computing node) should be returned with some information of the Neo4j server if it is running. 

```bash
curl [hostname of Neo4j server]:7474
```

And the return should look like this:

```bash
(my_venv) -bash-4.2$curl udc-aj34-36:7474
{
  "bolt_routing" : "neo4j://udc-aj34-36:7687",
  "transaction" : "http://udc-aj34-36:7474/db/{databaseName}/tx",
  "bolt_direct" : "bolt://udc-aj34-36:7687",
  "neo4j_version" : "4.3.7",
  "neo4j_edition" : "enterprise"
}
```

## Access Neo4j Server Running On A Rivanna Computing Node





## Data Structures in Neo4j

```
Note Properties:
    pid: (int) Person ID
    hid: (int) Household ID
    age: (int) Age
    age_group: (str) One of ['p' (0-4), 's' (5-17), 'a' (18-49), 'o' (50-64), 'g' (65+)]
    gender: (int) 1 or 2
    fips: (str)
    home_lat: (float) Home latitude
    home_lon: (float) Home longitude
    admin1: (str)
    admin2: (str)
    admin3: (str)
    admin4: (str)
Edge Properties:
	occur: (int) Time point the edge occurs
    duration: (int) Contact by second
    src_act: (str) Source Activity
    trg_act: (str) Target Activity
```

