#!/bin/bash
#
# Preprocesses data for build.
source bash/build_config.sh

echo "Dataset Version $VERSION : 02 Preprocessing"

echo "Preprocess source data ..."
run_sql_file sql/preprocess.sql

echo "Done!"
