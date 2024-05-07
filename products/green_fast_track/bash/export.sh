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

    export_source sources__airports

    export_source sources__arterial_highways MULTILINESTRING
    export_source sources__arterial_highways_buffers

    export_source sources__exposed_railways MULTILINESTRING
    export_source sources__exposed_railways_buffers
    
    export_source sources__vent_towers
    export_source sources__vent_towers_buffers

    export_source sources__industrial_sources_lots
    export_source sources__industrial_sources_buffers

    export_source sources__cats_permits_points POINT
    export_source sources__cats_permits_lots
    export_source sources__cats_permits_buffers

    export_source sources__state_facility_permits_points POINT
    export_source sources__state_facility_permits_lots
    export_source sources__state_facility_permits_buffers

    export_source sources__title_v_permits_points POINT
    export_source sources__title_v_permits_lots
    export_source sources__title_v_permits_buffers

    export_source sources__nyc_parks_properties
    export_source sources__nyc_parks_properties_buffers

    export_source sources__dpr_schoolyard_to_playgrounds_lots
    export_source sources__dpr_schoolyard_to_playgrounds_buffers

    export_source sources__nys_parks_properties
    export_source sources__nys_parks_properties_buffers

    export_source sources__pops_points POINT
    export_source sources__pops_lots
    export_source sources__pops_buffers

    export_source sources__us_parks_properties
    export_source sources__us_parks_properties_buffers

    export_source sources__waterfront_access_pow
    export_source sources__waterfront_access_pow_buffers

    export_source sources__waterfront_access_wpaa
    export_source sources__waterfront_access_wpaa_buffers

    # Natural Resources
    export_source sources__beaches
    export_source sources__forever_wild_reserves
    export_source sources__national_wetlands
    export_source sources__natural_heritage_communities
    export_source sources__priority_estuaries
    export_source sources__priority_lakes
    export_source sources__priority_streams MULTILINESTRING
    export_source sources__recognized_ecological_complexes POINT
    export_source sources__special_natural_waterfront_areas
    export_source sources__state_freshwater_wetlands_checkzones
    export_source sources__state_freshwater_wetlands
    export_source sources__state_tidal_wetlands

    export_source sources__natural_resources_buffer

    # Historic
    export_source sources__nyc_historic_buildings_points MULTIPOINT
    export_source sources__nyc_historic_buildings_lots
    export_source sources__nyc_historic_buildings_buffers
    export_source sources__nyc_historic_buildings_buffers_200
    export_source sources__nyc_historic_districts
    export_source sources__scenic_landmarks
    export_source sources__nys_historic_buildings_points POINT
    export_source sources__nys_historic_buildings_lots
    export_source sources__nys_historic_buildings_buffers
    export_source sources__nys_historic_buildings_buffers_200
    export_source sources__nys_historic_districts
    export_source sources__us_historic_places
    export_source sources__us_historic_places_buffers
    export_source sources__archaeological_areas
    export_source int_buffers__historic_resource_shadows

    export_source variables NONE
    export_source source_data_versions NONE

    fgdb_export_cleanup ${fgdb_filename}
)

zip -r output/output.zip output
