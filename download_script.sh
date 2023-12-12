export DATASET_URL=https://pub-383410a98aef4cb686f0c7601eddd25f.r2.dev/interactive/snb-out-sf1-merged-fk.tar.zst
curl --silent --fail ${DATASET_URL} | tar -xv --use-compress-program=unzstd

# Delete unnecessary data
rm -rf out-sf*/graphs/csv/bi/*projected-fk
rm -rf out-sf*/graphs/csv/bi/composite-merged-fk/deletes
rm -rf out-sf*/graphs/csv/bi/composite-merged-fk/inserts

# Move the initial snapshot up to root directory
mv out-sf*/graphs/csv/bi/composite-merged-fk/initial_snapshot/ out-sf*/
rm -rf out-sf*/graphs

# Merge the dynamic and static folders
mv out-sf*/initial_snapshot/dynamic/* out-sf*/
mv out-sf*/initial_snapshot/static/* out-sf*/
rm -rf out-sf*/initial_snapshot

