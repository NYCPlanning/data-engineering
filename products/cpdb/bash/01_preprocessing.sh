#!/bin/bash
source bash/config.sh

run_sql_file "sql/_create.sql"

echo "fixing dot_bridges"
run_sql_command "ALTER TABLE dot_projects_bridges RENAME COLUMN fmsid TO fms_id;"
python3 python/dot_bridges.py

echo "fixing doitt_buildingfootprints_source and saving result into doitt_buildingfootprints"
run_sql_command "
    DROP TABLE IF EXISTS doitt_buildingfootprints;
    CREATE TABLE doitt_buildingfootprints AS 
    SELECT * 
    FROM doitt_buildingfootprints_source 
    WHERE bin is NOT NULL
"
