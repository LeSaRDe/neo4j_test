#!/bin/sh

LDB=/project/biocomplexity/mf3jh/neo4j_workspace/data
CONF=/project/biocomplexity/mf3jh/neo4j_workspace/conf
PLUGINS=/project/biocomplexity/mf3jh/neo4j_workspace/plugins
LOGS=/project/biocomplexity/mf3jh/neo4j_workspace/logs
#IMAGE=/project/biocomplexity/singularity_images/neo4j-enterprise.sif
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

sleep 10
ps -aufx | grep neo4j

handler()
{
  echo "Closing Database..."
  #shutdown the db cleanly
  echo "Stop DB"
  # we need to add a stop script in here for neo4j
  #singularity run --app stop instance://$SLURM_JOB_ID

  # stop the singularity instance
  echo "Stop Singularity Instance"
  singularity exec instance://$SLURM_JOB_ID neo4j stop
  singularity instance stop $SLURM_JOB_ID

  touch ${LDB}/SHUTDOWN
}

echo "Waiting for DB Completion..."
while [ ! -f "${LDB}/SHUTDOWN" ] ;do
  sleep 10
done;

trap handler SIGINT
