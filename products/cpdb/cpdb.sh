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
        python3 -m dcpy.connectors.edm.publishing upload -p db-cpdb -a private --max_files 200
esac
