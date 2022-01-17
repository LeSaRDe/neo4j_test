#!/bin/sh

echo "[RUN CYPHER-SHELL] Starts."

# ARGUMENT CHECKING
if [ "$#" -lt 1 ]
  then
    echo "[RUN CYPHER-SHELL] USAGE:"
    echo "[RUN CYPHER-SHELL] launch_cypher_shell.sh [NEO4J SIF IMAGE PATH]"
    echo "[RUN CYPHER-SHELL] Exits."
    exit -1
fi

# SET NEO4J SIF IMAGE PATH
NEO4J_SIF=$2
if [ ! -f ${NEO4J_SIF} ]
  then
    echo "[RUN CYPHER-SHELL] NEO4J SIG IMAGE: ${NEO4J_SIF} dose not exist!"
    echo "[RUN CYPHER-SHELL] Exits."
    exit -1
fi
echo "[RUN CYPHER-SHELL] Neo4j SIF: ${NEO4J_SIF}"

# RUN CYPHER-SHELL
echo "[RUN CYPHER-SHELL] Run cypher-shell."
singularity shell --writable-tmpfs ${NEO4J_SIF}
