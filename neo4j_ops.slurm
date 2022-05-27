#!/bin/bash

#SBATCH --time=3-00:00:00
#SBATCH --nodes=1
#SBATCH --mem=350000
#SBATCH --partition=bii
#SBATCH --exclusive
#SBATCH --cpus-per-task=20
#SBTACH --core-spec=40

module load anaconda
module load singularity
#module load java/1.12.0
#conda install -n my_venv pip
#conda install -n my_venv python
source activate my_venv
conda deactivate
source activate my_venv

# SET NEO4J_HOSTNAME
export NEO4J_HOSTNAME="udc-aj37-16c1"

# CREATE DB
#python neo4j_ops.py "neo4j_driver->create_db->create_constraints->create_indexes"
#python neo4j_ops.py "neo4j_driver->create_indexes"

# CREATE INITIAL CONTACT NETWORK
#python neo4j_ops.py "neo4j_driver->create_nodes"
python neo4j_ops.py "neo4j_driver->create_init_cn_edges"
#python neo4j_ops.py "neo4j_driver->parallel_apoc" wy_init_cn_split_${SLURM_ARRAY_TASK_ID}
#python neo4j_ops.py "neo4j_driver->serial_apoc"

# CREATE INTERMEDIATE CONTACT NETWORKS



