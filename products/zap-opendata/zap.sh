#!/bin/bash
VERSION=$(date +%Y%m%d)
LOCATION=US

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

    bq show $dataset || bq mk --location=$LOCATION --dataset $dataset
    bq show $tablename || bq mk $tablename
    bq load \
        --location=$LOCATION \
        --source_format=CSV \
        --autodetect \
        --quote '"' \
        --replace \
        --allow_quoted_newlines \
        $tablename \
        $google_storage_filepath
}

function upload_to_digital_ocean {
    dataset=${1}
    version=${2}
    output_suffix=${3}
    do_bucket=${4}

    filename=${dataset}_${output_suffix}.csv
    output_filepath=.output/${dataset}/${filename}
    do_directory="spaces/${do_bucket}/db-zap"

    mc cp --attr x-amz-acl=public-read ${output_filepath} ${do_directory}/${version}/${dataset}/${filename}
    mc cp --attr x-amz-acl=public-read ${output_filepath} ${do_directory}/latest/${dataset}/${filename}
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
        version=${3:-$VERSION}
        upload_to_big_query ${dataset} ${version} "internal"
    ;;
    upload_visible_bq )
        # visible version
        dataset=$2
        version=${3:-$VERSION}
        upload_to_big_query ${dataset} ${version} "visible"
    ;;

    upload_crm_do )
        dataset=$2
        version=${3:-$VERSION}
        upload_to_digital_ocean ${dataset} ${version} "crm" "edm-private"
    ;;
    upload_internal_do )
        dataset=$2
        version=${3:-$VERSION}
        upload_to_digital_ocean ${dataset} ${version} "internal" "edm-private"
    ;;
    upload_visible_do )
        dataset=$2
        version=${3:-$VERSION}
        upload_to_digital_ocean ${dataset} ${version} "visible" "edm-publishing"

esac
