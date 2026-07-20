#!/bin/bash
source ../../bash/utils.sh
set_error_traps

run_sql_command "CREATE EXTENSION IF NOT EXISTS tablefunc;"

echo "fixing dot_bridges"
python3 python/dot_bridges.py
