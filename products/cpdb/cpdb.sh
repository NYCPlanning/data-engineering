#!/bin/bash
source ../../bash/utils.sh
set_error_traps

case $1 in 
    build)
        ./bash/01_preprocessing.sh
        ./bash/02_build.sh
        ./bash/03_adminbounds.sh
        ./bash/04_analysis.sh
        ./bash/05_export.sh
esac
