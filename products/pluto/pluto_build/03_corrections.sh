#!/bin/bash
source ./bash/config.sh
set_error_traps

run_sql_file sql/corr_create.sql

echo "Applying corrections to PLUTO"
run_sql_file sql/corr_lotarea.sql
run_sql_file sql/corr_template.sql -v FIELD=yearbuilt
run_sql_file sql/corr_template.sql -v FIELD=ownername
run_sql_file sql/corr_ownername_punctuation.sql
run_sql_file sql/corr_template.sql -v FIELD=cd
run_sql_file sql/corr_template.sql -v FIELD=numfloors
run_sql_file sql/corr_template.sql -v FIELD=unitsres
run_sql_file sql/corr_template.sql -v FIELD=unitstotal
run_sql_file sql/corr_inwoodrezoning.sql
run_sql_file sql/corr_template.sql -v FIELD=bct2020
run_sql_file sql/corr_template.sql -v FIELD=address
run_sql_file sql/remove_unitlots.sql
