#!/bin/bash
source ../../bash/utils.sh
set_error_traps

# Export tables to DAT files
# fixed-width text files with no header or delimeter are specific to LION
function dat_export {
    local table=${1}
    local output_file=${2:-${table}}
    run_sql_command \
        "\COPY (\
            SELECT * FROM ${table}\
        ) TO STDOUT;">${output_file}.dat
}

echo "Export LION outputs"
rm -rf output
mkdir -p output 
(
    cd output

    echo "Export LION dat files"
    dat_export manhattan_lion_dat ManhattanLION &
    wait
    
    echo "Export source data versions and build metadata"
    cp ../source_data_versions.csv ./
    cp ../build_metadata.json ./
)

wait
zip -r output/output.zip output

echo "Export Complete"
