#!/bin/bash
source ../../../bash/utils.sh
set_error_traps

run_sql_file sql/clean_database.sql
run_sql_command "VACUUM FULL"
