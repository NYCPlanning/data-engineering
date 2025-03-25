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

    echo "Generate int_flags__all csv"
    csv_export int_flags__all all_flags
    
    echo "Generate FileGeodatabase ${fgdb_filename}"
    echo "FGDB export green_fast_track_bbls ..."
    fgdb_export_partial ${fgdb_filename} MULTIPOLYGON green_fast_track_bbls green_fast_track_bbls ${default_srs}
    
    echo "FGDB export Air Quality ..."
    export_source source__cats_permit_points POINT
    export_source source__cats_permit_lots
    export_source source__cats_permit_buffer

    export_source source__industrial_lots
    export_source source__industrial_lots_buffer

    export_source source__state_facility_points POINT
    export_source source__state_facility_lots
    export_source source__state_facility_buffer

    export_source source__title_v_permit_points POINT
    export_source source__title_v_permit_lots
    export_source source__title_v_permit_buffer

    export_source source__vent_tower_polys
    export_source source__vent_tower_buffer
    
    export_source source__arterial_highway_lines MULTILINESTRING
    export_source source__arterial_highway_buffer

    echo "FGDB export Noise ..."
    export_source source__exposed_railway_lines MULTILINESTRING
    export_source source__exposed_railway_polys
    export_source source__exposed_railway_buffer

    export_source source__airport_noise

    echo "FGDB export Natural Resources ..."
    export_source source__natural_resources_points POINT
    export_source source__natural_resources_lines MULTILINESTRING
    export_source source__natural_resources_polys

    export_source source__wetland_checkzone

    echo "FGDB export Historic Resources ..."
    export_source source__archaeological_area_polys
    export_source source__historic_districts_polys
    export_source source__historic_resources_points POINT
    export_source source__historic_resources_lots

    export_source source__historic_resources_adj_points POINT
    export_source source__historic_resources_adj_lots
    export_source source__historic_resources_adj_buffer

    echo "FGDB export Shadows ..."
    export_source source__shadow_open_spaces_points POINT
    export_source source__shadow_open_spaces_polys
    export_source source__shadow_open_spaces_buffer

    export_source source__shadow_nat_resources_lines MULTILINESTRING
    export_source source__shadow_nat_resources_polys
    export_source source__shadow_nat_resources_buffer

    export_source source__shadow_hist_resources_points POINT
    export_source source__shadow_hist_resources_lots
    export_source source__shadow_hist_resources_buffer

    echo "FGDB export non-spatial tables ..."
    export_source variables NONE
    export_source source_data_versions NONE

    fgdb_export_cleanup ${fgdb_filename}
)

zip -r output/output.zip output
