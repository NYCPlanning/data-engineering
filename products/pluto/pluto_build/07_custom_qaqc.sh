#!/bin/bash
source ./pluto_build/bash/config.sh   # assuming this script is run from pluto/ dir
set_error_traps

echo "Test source tables"
dbt test --select "source:*" --exclude tag:de_check

echo "Build QAQC models (intermediate and reports)"
dbt build --select qaqc.intermediate qaqc.reports --exclude tag:de_check

echo "🔥 Run DE aka important tests 🔥"
dbt test --select tag:de_check,tag:$VERSION_TYPE    # this will only run tests that have both tags, not just one of them
