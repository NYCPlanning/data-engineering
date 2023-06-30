#!/bin/bash

case $1 in 
    dataloading ) ./bash/01_dataloading.sh ;;
    build ) ./bash/02_build.sh ;;
    qaqc ) ./bash/03_qaqc.sh ;;
    export ) ./bash/04_export.sh ;;
    upload ) ./bash/05_upload.sh ;;
    sql) sql $@ ;;
    * ) echo "COMMAND \"$1\" is not found. (valid commands: dataloading|build|export|upload)" ;;
esac
