#!/bin/sh

WORD_FOLDER="/project/biocomplexity/mf3jh/neo4j_workspace/import/wy_replicate_0/"

for t in 5 6 7 8 9 10 11 12 13 14 15
do
  echo "Working on network_${t}..."
  # wc -l ${WORD_FOLDER}network_${t}
  sed -e '1, 2d' ${WORD_FOLDER}network_${t} > ${WORD_FOLDER}network_no_head_${t}
  sort --field-separator=',' --key=1,5 ${WORD_FOLDER}network_no_head_${t} > ${WORD_FOLDER}network_no_head_sorted_${t}
  sed -i '1s/^/targetPID,targetActivity,sourcePID,sourceActivity,duration,LID\n/' ${WORD_FOLDER}network_no_head_sorted_${t}
  rm ${WORD_FOLDER}network_no_head_${t}
  echo "Done with network_${t}."
  echo ""
done
