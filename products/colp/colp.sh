#!/bin/bash

case $1 in 
    dataloading ) ./bash/01_dataloading.sh ;;
    build ) ./bash/02_build.sh ;;
    qaqc ) ./bash/03_qaqc.sh ;;
    export ) ./bash/04_export.sh ;;
    upload ) dcpy lifecycle builds artifacts builds upload -p db-colp -a public-read ;;
    sql) sql $@ ;;
    * ) echo "COMMAND \"$1\" is not found. (valid commands: dataloading|build|export|upload)" ;;
esac
