#!/bin/bash
source bash/config.sh
set -e

# Import fisa_capitalcommitments to database
version=$(get_version fisa_capitalcommitments latest private)
location=US

function import_data {
    echo "version: $version"
    create_source_data_table
    import fisa_capitalcommitments $version
}


# Loading the latest fisa_capitalcommitments to bigquery
function fisa {
    mkdir -p .output && (
        cd .output
        local dataset=fisa_capitalcommitments
        local tablename=$dataset.$version
        echo "$dataset"
        CSV_export $dataset
        bq show \
            --location=$location\
            --dataset $dataset ||
        bq mk \
            --location=$location\
            --dataset $dataset
        bq mk --force $tablename
        bq load \
            --location=$location\
            --autodetect\
            --skip_leading_rows 1\
            --replace\
            $tablename ./$dataset.csv
    )
    echo 
    echo "Creating table $tablename complete"
    echo 
}
# Get Capital Spending based the latest fisa_capitalcommitments
function calculate {
    local dataset=cpdb_capital_spending
    local tablename=$dataset.$version
    mkdir -p .output && (
        cd .output
        bq show \
            --location=$location\
            --dataset $dataset ||
        bq mk \
            --location=$location\
            --dataset $dataset
        bq query\
            --location=$location\
            --replace\
            --destination_table $tablename \
            --use_legacy_sql=false "
            SELECT DISTINCT * FROM \`checkbooknyc_capital_spending.*\` 
            WHERE CAST(TRIM(LEFT(capital_project,12)) AS STRING) IN (
                SELECT DISTINCT LPAD(CAST(managing_agcy_cd AS STRING), 3, '0')||REPLACE(project_id,' ','')
                FROM \`fisa_capitalcommitments.$version\`);"
    )
    echo 
    echo "Creating table $tablename complete"
    echo 
}

function export_data {
    local dataset=cpdb_capital_spending
    local tablename=$dataset.$version
    mkdir -p .output && (
        cd .output
        bq extract \
            --location=$location\
            --destination_format CSV \
            --field_delimiter ',' \
            --print_header \
            $tablename gs://edm-temporary/$dataset/$version/$dataset.csv
        gsutil cp gs://edm-temporary/$dataset/$version/$dataset.csv .
    )
    echo 
    echo "export complete"
    echo 
}

function archive {
    local format=$1
    docker run --rm \
        -e AWS_S3_ENDPOINT=$AWS_S3_ENDPOINT\
        -e AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID\
        -e AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY\
        -e AWS_S3_BUCKET=$AWS_S3_BUCKET\
        -v $(pwd)/.library:/library/.library\
        -v $(pwd)/templates:/templates\
        -v $(pwd)/.output:/.output\
        nycplanning/library:ubuntu-01d9e5eddf65dbdea3eb0c500314963b5ec6246a library archive -f /templates/cpdb_capital_spending.yml -v $version -s -l -o $format --compress
}

function all {
    import_data
    fisa
    calculate
    export_data
}

# Execution of All commands:
case $1 in 
    import_data | fisa | calculate | export_data | archive) $@;;
    *) all;;
esac
