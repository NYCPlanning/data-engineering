#!/bin/bash

FILE_DIR=$(dirname "$(readlink -f "$0")")

source $FILE_DIR/../../bash/utils.sh
set_error_traps
max_bg_procs 5


case $1 in
    export) ./facdb/bash/export.sh ;;
esac
