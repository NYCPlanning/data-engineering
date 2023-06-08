#!/bin/bash
source ../../bash_utils/config.sh
set_env ../../.env

run_sql_file sql/corr_create.sql

echo "Applying corrections to PLUTO"
run_sql_file sql/corr_lotarea.sql
run_sql_file sql/corr_yearbuilt_lpc.sql
run_sql_file sql/corr_ownername_city.sql
run_sql_file sql/corr_ownername_punctuation.sql
run_sql_file sql/corr_communitydistrict.sql
run_sql_file sql/corr_numfloors.sql
run_sql_file sql/corr_units.sql
run_sql_file sql/corr_inwoodrezoning.sql
run_sql_file sql/corr_dropoldrecords.sql
run_sql_file sql/remove_unitlots.sql
