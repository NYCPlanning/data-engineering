#!/bin/bash
source config.sh

echo "CBBR Version $VERSION : 02 CBBR"
# TODO delete tables that are created by this stage
echo "Create build tables to modify ..."
run_sql sql/preprocessing.sql

## Skipping for dev of initial FY2024 build
# psql $BUILD_ENGINE -f sql/apply_agency_updates.sql

echo "Normalize agency values ..."
run_sql sql/normalize_agency.sql

echo "Normalize commdist values ..."
run_sql sql/normalize_commdist.sql

echo "Normalize denominator values ..."
run_sql sql/normalize_denominator.sql

echo "Done!"
