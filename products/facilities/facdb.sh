#!/bin/bash

FILE_DIR=$(dirname "$(readlink -f "$0")")

source $FILE_DIR/../../bash/utils.sh
set_error_traps
max_bg_procs 5


case $1 in
    init) init ;;
    upload) dcpy lifecycle builds artifacts builds upload -p db-facilities -a public-read ;;
    export) ./facdb/bash/export.sh ;;
    *) facdb_execute $@ ;;
esac
