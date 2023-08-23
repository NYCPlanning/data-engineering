#!/bin/bash
#
# Loads source data files into a local SQL database.

source bash/build_config.sh

echo "Dataset Version $DATASET_VERSION : 01 Data Loading"

# Create a table with the versions of source data
echo "Create source data versions table ..."
run_sql_command "
  DROP TABLE IF EXISTS source_versions;
  CREATE TABLE source_versions (
    datasource text,
    version text
  );
"

# Import data
echo "Import source data ..."
import_public dcp_zoningdistricts
import_public dcp_cdboundaries_wi $DCP_CDBOUNDARIES_WI_VERSION

python3 -m python.01_dataloading

# # Delete data cache (optional)
# echo "Deleting source data cache ..."
# rm -rf .library

echo "Done!"
