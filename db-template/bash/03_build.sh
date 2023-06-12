#!/bin/bash
#
# Build dataset.
source bash/build_config.sh

echo "Dataset Version $VERSION : 03 Build"

echo "Build dataset ..."
run_sql_file sql/build.sql

echo "Done!"
