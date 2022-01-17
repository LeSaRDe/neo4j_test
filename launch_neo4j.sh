#!/bin/sh

# CAUTION
# MAKE SURE YOU HAVE CHECKED ALL "TODO" IN THIS FILE BEFORE RUNNING!!!

echo "[RUN NEO4J] Starts."

# ARGUMENT CHECKING
if [ "$#" -lt 2 ]
  then
    echo "[RUN NEO4J] USAGE:"
    echo "[RUN NEO4J] launch_neo4j.sh [WORK FOLDER] [NEO4J SIF IMAGE PATH] [NEO4J INSTANCE NAME]"
    echo "[RUN NEO4J] Exits."
    exit -1
fi

# SET WORK FOLDER
if [ $1 != "*/" ]
  then
    WORK_FOLDER=$1/
  else
    WORK_FOLDER=$1
fi

if [ ! -d ${WORK_FOLDER} ]
  then
    echo "[RUN NEO4J] WORK FOLDER: ${WORK_FOLDER} dose not exist!"
    echo "[RUN NEO4J] Exits."
    exit -1
fi
echo "[RUN NEO4J] Work Folder: ${WORK_FOLDER}"

# SET NEO4J SIF IMAGE PATH
NEO4J_SIF=$2
if [ ! -f ${NEO4J_SIF} ]
  then
    echo "[RUN NEO4J] NEO4J SIG IMAGE: ${NEO4J_SIF} dose not exist!"
    echo "[RUN NEO4J] Exits."
    exit -1
fi
echo "[RUN NEO4J] Neo4j SIF: ${NEO4J_SIF}"

# SET NEO4J INSTANCE NAME
if [ "$#" -gt 2 ]
  then
    NEO4J_INS_NAME=$3
else
  NEO4J_INS_NAME=${SLURM_JOB_ID}
fi
echo "[RUN NEO4J] Neo4j Singuilarity Instance Name: ${NEO4J_INS_NAME}"

# REMOVE "SHUTDOWN" FROM PREVIOUS RUNS IF ANY
if [ -f ${WORK_FOLDER}SHUTDOWN ]; then
  rm ${WORK_FOLDER}SHUTDOWN
  echo "[RUN NEO4J] Removed SHUTDOWN"
fi

# SET UP FOLDER EXPOSURE
# NOTE
# The "metrics" folder is only available to the Enterprise version.

# CAUTION
# When exposing the "conf" folder, the default "neo4j.conf" will NOT be
# forwarded to the outside folder. And thus everything built in the container
# definition file will NOT be available in this case. So exposing this folder
# should only be for testing purposes rather than official runs.

# TODO
# Set the following two lines to "true" to expose "conf" and "metrics"
# respectively when needed.
EN_CONF_EXP=false
EN_METRICS_EXP=false

EXP_DATA=${WORK_FOLDER}data
EXP_PLUGINS=${WORK_FOLDER}plugins
EXP_LOGS=${WORK_FOLDER}logs
EXP_IMPORT=${WORK_FOLDER}import
EXP_CONF=${WORK_FOLDER}conf
EXP_METRICS=${WORK_FOLDER}metrics

mkdir -p ${EXP_DATA}
mkdir -p ${EXP_PLUGINS}
mkdir -p ${EXP_LOGS}
mkdir -p ${EXP_IMPORT}
if [ ${EN_CONF_EXP} = true ]
  then
    mkdir -p ${EXP_CONF}
fi
if [ ${EN_METRICS_EXP} = true ]
  then
    mkdir -p ${EXP_METRICS}
fi

chmod -R 777 ${EXP_DATA}
chmod -R 777 ${EXP_PLUGINS}
chmod -R 777 ${EXP_LOGS}
chmod -R 777 ${EXP_IMPORT}
if [ ${EN_CONF_EXP} = true ]
  then
    chmod -R 777 ${EXP_CONF}
fi
if [ ${EN_METRICS_EXP} = true ]
  then
    chmod -R 777 ${EXP_METRICS}
fi

# TODO
# These default folders need to be double checked for different versions of Neo4j.
NEO4J_DEF_FOLDER=/var/lib/neo4j/
NEO4J_DEF_DATA=${NEO4J_DEF_FOLDER}data
NEO4J_DEF_LOGS=${NEO4J_DEF_FOLDER}logs
NEO4J_DEF_PLUGINS=${NEO4J_DEF_FOLDER}plugins
NEO4J_DEF_IMPORT=${NEO4J_DEF_FOLDER}import
NEO4J_DEF_CONF=${NEO4J_DEF_FOLDER}conf
NEO4J_DEF_METRICS=${NEO4J_DEF_FOLDER}metrics

# Folder Exposure Arguments
FOLDER_EXP_ARG="${EXP_DATA}:${NEO4J_DEF_DATA},${EXP_PLUGINS}:${NEO4J_DEF_PLUGINS},${EXP_LOGS}:${NEO4J_DEF_LOGS},${EXP_IMPORT}:${NEO4J_DEF_IMPORT}"
if [ ${EN_CONF_EXP} = true ]
  then
    FOLDER_EXP_ARG+="${EXP_CONF}:${NEO4J_DEF_CONF}"
fi
if [ ${EN_METRICS_EXP} = true ]
  then
    FOLDER_EXP_ARG+="${EXP_METRICS}:${NEO4J_DEF_METRICS}"
fi

echo "[RUN NEO4J] Data folder: ${NEO4J_DEF_DATA} => ${EXP_DATA}"
echo "[RUN NEO4J] Plugin folder: ${NEO4J_DEF_PLUGINS} => ${EXP_PLUGINS}"
echo "[RUN NEO4J] Log folder: ${NEO4J_DEF_LOGS} => ${EXP_LOGS}"
echo "[RUN NEO4J] Log folder: ${NEO4J_DEF_IMPORT} => ${EXP_IMPORT}"
if [ ${EN_CONF_EXP} = true ]
  then
    echo "[RUN NEO4J] Config folder: ${NEO4J_DEF_CONF} => ${EXP_CONF}"
fi
if [ ${EN_METRICS_EXP} = true ]
  then
    echo "[RUN NEO4J] Metrics folder: ${NEO4J_DEF_METRICS} => ${EXP_METRICS}"
fi

# RUN NEO4J INSTANCE
echo "[RUN NEO4J] Starting Neo4j Instance..."
singularity instance start --env=NEO4J_ACCEPT_LICENSE_AGREEMENT=yes --writable-tmpfs --bind ${FOLDER_EXP_ARG} ${NEO4J_SIF} ${NEO4J_INS_NAME}
echo "[RUN NEO4J] Neo4j Instance Started."

sigint_handler()
{
  echo "[RUN NEO4J] Stopping Neo4j Instance..."
  singularity instance stop ${NEO4J_INS_NAME}
  echo "[RUN NEO4J] Neo4j Instance Stopped."
}

echo "[RUN NEO4J] Waiting for Neo4j Instance Completion..."
while true
do
  if [ -f "${WORK_FOLDER}/SHUTDOWN" ]
    then
      sigint_handler
      exit 0
    else
      sleep 5
  fi
done

trap sigint_handler INT
