#!/bin/bash

case $1 in 
    dataloading ) ./bash/01_dataloading.sh ;;
    build ) 
        ./bash/02a_load_data.sh
        ./bash/02b_python.sh
        ./bash/02c_sql_build.sh
        ;;
    load_data ) ./bash/02a_load_data.sh ;;
    python ) ./bash/02b_python.sh ;;
    sql_build ) ./bash/02c_sql_build.sh ;;
    qaqc ) ./bash/03_qaqc.sh ;;
    export ) ./bash/04_export.sh ;;
    upload ) python3 -m dcpy.connectors.edm.publishing upload -p db-colp -a public-read ;;
    sql) sql $@ ;;
    * ) echo "COMMAND \"$1\" is not found. (valid commands: dataloading|build|load_data|python|sql_build|qaqc|export|upload)" ;;
esac
