#!/bin/bash
source ./bash/config.sh
set_error_traps

run_sql_file sql/_create.sql
