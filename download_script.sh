if [[ $# -lt 1 ]]
then
    echo "Enter scaling factor 1/10"
    exit 1
fi
if [[ $1 -eq 1 ]]
then
    DATASET_URL="https://pub-383410a98aef4cb686f0c7601eddd25f.r2.dev/bi-pre-audit/bi-sf1-composite-merged-fk.tar.zst"
    DIR="bi-sf1-composite-merged-fk"
elif [[ $1 -eq 10 ]] 
then 
    DATASET_URL="https://pub-383410a98aef4cb686f0c7601eddd25f.r2.dev/bi-pre-audit/bi-sf10-composite-merged-fk.tar.zst"
    DIR="bi-sf10-composite-merged-fk"
else
    echo "Invalid scaling factor"
    exit 1
fi

TARGET_DIR="bi-sf-composite-merged-fk"

curl ${DATASET_URL} | tar -x --use-compress-program=unzstd

rm -rf ${DIR}/graphs/csv/bi/composite-merged-fk/deletes
rm -rf ${DIR}/graphs/csv/bi/composite-merged-fk/inserts
mv ${DIR}/graphs/csv/bi/composite-merged-fk/initial_snapshot/ ${DIR}/
rm -rf ${DIR}/graphs
mv ${DIR}/initial_snapshot/dynamic/* ${DIR}/
mv ${DIR}/initial_snapshot/static/* ${DIR}/
rm -rf ${DIR}/initial_snapshot
rm -rf ${TARGET_DIR}
mv ${DIR} ${TARGET_DIR}

find . -name '*.csv.gz' -print0 | xargs -0 -n1 gzip -d

rm -rf adjacency/
rm -rf mapping/

go run cmd/generic/main.go 
