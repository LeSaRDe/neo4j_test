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

./launch_neo4j.sh /project/biocomplexity/mf3jh/neo4j_workspace_wy_test /project/biocomplexity/mf3jh/neo4j_img/neo4j_community_large_heap.sif
