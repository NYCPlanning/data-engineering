#!/bin/bash
source bash/config.sh

case $1 in 
    dataloading ) ./bash/01_dataloading.sh;;
    build ) ./bash/02_build.sh;;
    sca ) ./bash/03_sca_aggregate.sh;;
    export ) shift && ./bash/04_export.sh $@;;
    * ) echo "$@ command not found";
esac
