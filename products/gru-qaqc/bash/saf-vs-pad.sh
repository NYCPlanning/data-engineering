#!/bin/bash
set -e
source bash/config.sh
parse_flags $@


if [[ $import_data -eq 1 ]]; then
    mkdir -p .library/datasets/dcp_saf && 
    (
    cd .library/datasets/dcp_saf
    mc cp spaces/edm-publishing/gru/dcp_saf/latest/dcp_saf.zip .
    unzip dcp_saf.zip
    VERSION=$(mc stat spaces/edm-publishing/gru/dcp_saf/latest/dcp_saf.zip --json | jq -r '.lastModified')
    record_version "SAF" "$VERSION"
    )
fi

if [[ $python_script -eq 1 ]]; then 
    echo "no python script to execute here ..."
fi 

function run {
    poetry run python -m python.saf-vs-pad $1 $2
    mkdir -p output/saf-vs-pad && (
        cd output/saf-vs-pad
        psql $BUILD_ENGINE -c "\copy (
        SELECT DISTINCT
            hnum, 
            sname, 
            borough, 
            b7sc, 
            geo_grc, 
            geo_reason_code, 
            geo_message
        from saf_$1_$2_pad
        ) TO stdout DELIMITER ',' CSV HEADER;" > saf_$1_$2_pad.csv
    )
}

if [[ $export_data -eq 1 ]]; then 
    # Execution
    run gen 1A &
    run gen 1R &
    run gen 1 &
    run rb 1A &
    run rb 1R &
    run rb 1 &
    wait
    echo "complete"
    (
        cd output/saf-vs-pad
        CSV_export versions
    )
fi

# Upload File
if [[ $upload_data -eq 1 ]]; then 
    Upload saf-vs-pad latest
    Upload saf-vs-pad $DATE
fi