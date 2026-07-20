#!/bin/bash
source ../../bash/utils.sh
set_error_traps

echo
echo "#########################################"
echo "# create admin bounds relational tables #"
echo "#########################################"
echo

# attributes_maprojid_bin
echo 'Creating maprojid --> bin relational table'
run_sql_file sql/attributes_maprojid_bin.sql &

# attributes_maprojid_bbl
echo 'Creating maprojid --> bin relational table'
run_sql_file sql/attributes_maprojid_bbl.sql

wait
