#!/bin/bash
source ../../bash/utils.sh
set_error_traps

run_sql_file "sql/_create.sql"

echo "fixing dot_bridges"
run_sql_command "ALTER TABLE dot_projects_bridges RENAME COLUMN fmsid TO fms_id;"
python3 python/dot_bridges.py
