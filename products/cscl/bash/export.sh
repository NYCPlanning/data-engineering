#!/bin/bash
source ../../bash/utils.sh
set_error_traps

# Export tables to DAT files
# fixed-width text files with no header or delimiter are specific to LION
function dat_export {
    local table=${1}
    local output_file=${2:-${table}}
    run_sql_command \
        "\COPY (\
            SELECT * FROM ${table}\
        ) TO STDOUT;" | sed $'s/$/\r/' > $output_file.dat
}

echo "Export LION outputs"
rm -rf output
mkdir -p output 
(
    cd output

    echo "Export LION dat files"
    dat_export manhattan_lion_dat ManhattanLION &
    dat_export bronx_lion_dat BronxLION &
    dat_export brooklyn_lion_dat BrooklynLION &
    dat_export queens_lion_dat QueensLION &
    dat_export staten_island_lion_dat StatenIslandLION &
    wait

    echo "Export SEDAT and SpecialSEDAT"
    dat_export export_sedat SEDAT &
    dat_export export_sedat_special SpecialSEDAT &
    
    echo "Export source data versions and build metadata"
    cp ../source_data_versions.csv ./
    cp ../build_metadata.json ./
)

wait
zip -r output/output.zip output

echo "Export Complete"
