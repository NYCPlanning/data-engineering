#!/bin/bash

function dataloading { 
    ./bash/01_dataloading.sh
}

function build { 
    ./bash/02_build.sh 
}

function qaqc { 
    ./bash/03_qaqc.sh 
}

case $1 in
    dataloading | build | qaqc ) $1 ;;
    *) echo "$1 not found!" ;;
esac