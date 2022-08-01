#!/bin/sh

# CAUTION
# MAKE SURE YOU HAVE CHECKED ALL "TODO" IN THIS FILE BEFORE RUNNING!!!

# CONFIGURATIONS FOR "neo4j-admin import"
# TODO
#   Modify the following parameters when necessary
NODE_LABEL="PERSON"
NODE_HEADER="nodes_header.csv"
NODE_FILE="va_persontrait_epihiper_pure_date.csv"
#NODE_FILE="wy_persontrait_epihiper_pure_data.csv"
#NODE_FILE="ny_persontrait_epihiper_pure_data.csv"
#NODE_FILE="initial_contact_network_nodes_pure_data.csv"
#NODE_FILE="ca_persontrait_epihiper_pure_date.csv"
DB_NAME="contacts"
EDGE_0_LABEL="CONTACT_0"
EDGE_1_LABEL="CONTACT_1"
EDGE_HEADER="edges_header.csv"
#EDGE_0_FILE="initial_contact_network_edges_pure_data.csv"
EDGE_0_FILE="va_contact_network_config_m_5_M_40_a_1000_m-contact_0_with_lid_pure_data.csv"
#EDGE_0_FILE="wy_contact_network_config_m_5_M_40_a_1000_m-contact_0_with_lid_pure_data.csv"
#EDGE_0_FILE="ny_contact_network_config_m_5_M_40_a_1000_m-contact_0_with_lid_pure_data.csv"
#EDGE_1_FILE="intermediate_network_i_edges_pure_data.csv"
#EDGE_1_FILE="network_30_pure_data"
#EDGE_1_FILE="network_0_pure_data.csv"
EDGE_1_FILE="va_contact_network_config_m_5_M_40_a_1000_m-contact_0_with_lid_pure_data.csv"
#EDGE_0_FILE="ca_contact_network_config_m_5_M_40_a_1000_m-contact_0_with_lid_pure_data.txt"
#EDGE_1_FILE="ca_contact_network_config_m_5_M_40_a_1000_m-contact_0_with_lid_pure_data.txt"


LOG_TITLE="NEO4J DATA LOADING"

echo "[${LOG_TITLE}] Starts."

# ARGUMENT CHECKING
if [ "$#" -lt 2 ]
  then
    echo "[${LOG_TITLE}] USAGE:"
    echo "[${LOG_TITLE}] neo4j_load_data.sh [WORK FOLDER] [NEO4J SIF IMAGE PATH]"
    echo "[${LOG_TITLE}] Exits."
    exit 255
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
    echo "[${LOG_TITLE}] WORK FOLDER: ${WORK_FOLDER} dose not exist!"
    echo "[${LOG_TITLE}] Exits."
    exit 255
fi
echo "[${LOG_TITLE}] Work Folder: ${WORK_FOLDER}"

# SET NEO4J SIF IMAGE PATH
NEO4J_SIF=$2
if [ ! -f ${NEO4J_SIF} ]
  then
    echo "[${LOG_TITLE}] NEO4J SIG IMAGE: ${NEO4J_SIF} dose not exist!"
    echo "[${LOG_TITLE}] Exits."
    exit 255
fi
echo "[${LOG_TITLE}] Neo4j SIF: ${NEO4J_SIF}"

EXP_DATA=${WORK_FOLDER}data
EXP_LOGS=${WORK_FOLDER}logs
EXP_IMPORT=${WORK_FOLDER}import

mkdir -p ${EXP_DATA}
mkdir -p ${EXP_LOGS}
mkdir -p ${EXP_IMPORT}

chmod -R 777 ${EXP_DATA}
chmod -R 777 ${EXP_LOGS}
chmod -R 777 ${EXP_IMPORT}

# TODO
# These default folders need to be double checked for different versions of Neo4j.
NEO4J_DEF_FOLDER=/var/lib/neo4j/
NEO4J_DEF_DATA=${NEO4J_DEF_FOLDER}data
NEO4J_DEF_LOGS=${NEO4J_DEF_FOLDER}logs
NEO4J_DEF_IMPORT=${NEO4J_DEF_FOLDER}import

# Folder Exposure Arguments
FOLDER_EXP_ARG="${EXP_DATA}:${NEO4J_DEF_DATA},${EXP_LOGS}:${NEO4J_DEF_LOGS},${EXP_IMPORT}:${NEO4J_DEF_IMPORT}"

echo "[${LOG_TITLE}] Data folder: ${NEO4J_DEF_DATA} => ${EXP_DATA}"
echo "[${LOG_TITLE}] Log folder: ${NEO4J_DEF_LOGS} => ${EXP_LOGS}"
echo "[${LOG_TITLE}] Log folder: ${NEO4J_DEF_IMPORT} => ${EXP_IMPORT}"

# LOAD DATA INTO NEO4J
# NOTE
#   'exec' instead of 'run' needs to be used so that 'tini' will be run with PID 1.
echo "[${LOG_TITLE}] Starting loading data..."
singularity exec --env=NEO4J_ACCEPT_LICENSE_AGREEMENT=yes --writable-tmpfs --bind ${FOLDER_EXP_ARG} ${NEO4J_SIF} \
neo4j-admin import \
--nodes=${NODE_LABEL}=${NEO4J_DEF_IMPORT}/${NODE_HEADER},${NEO4J_DEF_IMPORT}/${NODE_FILE} \
--database ${DB_NAME} --normalize-types=false --skip-bad-relationships=true \
--skip-bad-entries-logging=true --id-type=INTEGER --bad-tolerance=35000000 \
--high-io=true \
--relationships=${EDGE_0_LABEL}=${NEO4J_DEF_IMPORT}/${EDGE_HEADER},${NEO4J_DEF_IMPORT}/${EDGE_0_FILE} \
--relationships=${EDGE_1_LABEL}=${NEO4J_DEF_IMPORT}/${EDGE_HEADER},${NEO4J_DEF_IMPORT}/${EDGE_1_FILE}

# Loading nodes only
#singularity exec --env=NEO4J_ACCEPT_LICENSE_AGREEMENT=yes --writable-tmpfs --bind ${FOLDER_EXP_ARG} ${NEO4J_SIF} \
#neo4j-admin import \
#--nodes=${NODE_LABEL}=${NEO4J_DEF_IMPORT}/${NODE_HEADER},${NEO4J_DEF_IMPORT}/${NODE_FILE} \
#--database ${DB_NAME} --normalize-types=false --skip-bad-relationships=true \
#--skip-bad-entries-logging=true --id-type=INTEGER --bad-tolerance=35000000
#echo "[${LOG_TITLE}] Loading data done."

