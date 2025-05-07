#!/bin/bash
source config.sh
set_error_traps

echo "CBBR Version ${VERSION} : 02 CBBR"

# TODO delete tables that are created by this stage
echo "Create build tables to modify ..."
run_sql_file sql/preprocessing.sql
run_sql_file sql/cbbr_submissions.sql

echo "Normalize agency values ..."
run_sql_file sql/normalize_agency.sql

echo "Normalize commdist values ..."
run_sql_file sql/normalize_commdist.sql

echo "Normalize denominator values ..."
run_sql_file sql/normalize_denominator.sql

echo "Done!"
