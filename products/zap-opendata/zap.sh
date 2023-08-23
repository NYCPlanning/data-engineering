#!/bin/bash
VERSION=$(date +%Y%m%d)
location=US

function upload_to_big_query {
    dataset=${2}
    version=${1}
    output_suffix=${3}

    output_filepath=.output/$dataset/$dataset_${output_suffix}.csv
    google_storage_filepath=gs://zap-crm-export/datasets/$dataset/$version/${dataset}_${output_suffix}.csv

    version_specific="${version}_${output_suffix}"
    tablename=$dataset.$version_specific
    echo "Archving output version ${dataset} ${version_specific} to ${tablename}..."

    gsutil cp $output_filepath $google_storage_filepath

    bq show $dataset || bq mk --location=$location --dataset $dataset
    bq show $tablename || bq mk $tablename
    bq load \
        --location=$location \
        --source_format=CSV \
        --autodetect \
        --quote '"' \
        --replace \
        --allow_quoted_newlines \
        $tablename \
        $google_storage_filepath
}

case $1 in
    download ) 
        python3 -m src.runner $2
    ;;
    upload_crm_bq )
        # crm version
        dataset=$2
        version=${3:-$VERSION}
        upload_to_big_query ${dataset} ${version} "crm"
    ;;
    upload_internal_bq )
        # internal version
        dataset=$2
        VERSION=${3:-$VERSION}
        upload_to_big_query ${dataset} ${version} "internal"
    ;;
    upload_visible_bq )
        # visible version
        dataset=$2
        VERSION=${3:-$VERSION}
        # internal version
        upload_to_big_query ${dataset} ${version} "visible"

    ;;
    upload_do )
        dataset=$2
        VERSION=$3
        SPACES="spaces/edm-publishing/db-zap"
        output_filepath=".output/$dataset/${dataset}_visible.csv"
        mc cp ${output_filepath} $SPACES/$VERSION/$dataset/$dataset.csv
        mc cp ${output_filepath} $SPACES/latest/$dataset/$dataset.csv


esac