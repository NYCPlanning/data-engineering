#!/bin/bash
function clean_database {
    connection_string=${1:-$BUILD_ENGINE}
    source ../../../bash/utils.sh

    run_sql_file sql/clean_database.sql
    run_sql_command "VACUUM FULL"
}

register db clean "Clean and vacuum database" clean_database
