#!/bin/bash
source ../../bash/utils.sh
set_error_traps

fgdb_filename=green_fast_track

rm -rf output

function export_source {
    format=${2:-"MULTIPOLYGON"}
    fgdb_export_partial ${fgdb_filename} ${format} $1 $1 ${default_srs} -update
}

echo "Export product tables"
mkdir -p output && (
    cd output

    echo "Copy metadata files"
    cp ../source_data_versions.csv .
    cp ../build_metadata.json .
    
    echo "Generate FileGeodatabase ${fgdb_filename}"
    fgdb_export_partial ${fgdb_filename} MULTIPOLYGON green_fast_track_bbls green_fast_track_bbls ${default_srs}

    export sources__airports 

    export sources__arterial_highways MULTILINESTRING
    export sources__arterial_highways_buffers

    export sources__exposed_railways MULTILINESTRING
    export sources__exposed_railways_buffers
    
    export sources__vent_towers
    export sources__vent_towers_buffers

    export sources__industrial_sources_lots
    export sources__industrial_sources_buffers

    export sources__cats_permits_points POINT
    export sources__cats_permits_lots
    export sources__cats_permits_buffers

    export sources__state_facility_permits_points POINT
    export sources__state_facility_permits_lots
    export sources__state_facility_permits_buffers

    export sources__title_v_permits_points POINT
    export sources__title_v_permits_lots
    export sources__title_v_permits_buffers

    export sources__nyc_parks_properties
    export sources__nyc_parks_properties_buffers

    export sources__nys_parks_properties
    export sources__nys_parks_properties_buffers

    export sources__pops_points POINT
    export sources__pops_lots
    export sources__pops_buffers

    export sources__us_parks_properties
    export sources__us_parks_properties_buffers

    export sources__waterfront_access_pow
    export sources__waterfront_access_pow_buffers

    export sources__waterfront_access_wpaa
    export sources__waterfront_access_wpaa_buffers

    export variables NONE
    export source_data_versions NONE

    fgdb_export_cleanup ${fgdb_filename}
)

zip -r output/output.zip output
