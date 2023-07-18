#!/bin/bash
#
# Builds db-checkbook

# import bash utils
FILE_DIR=$(dirname "$(readlink -f "$0")")
source $FILE_DIR/../bash/utils.sh
# ensure script exits whent there's an error
set_error_traps

echo "started building db-checkbook ..."

python3 -m db-checkbook.build_scripts.01_dataloading
python3 -m db-checkbook.build_scripts.02_build
python3 -m db-checkbook.build_scripts.03_export

echo "done building db-checkbook!"
