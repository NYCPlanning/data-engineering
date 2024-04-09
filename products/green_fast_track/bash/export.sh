#!/bin/bash
source ../../bash/utils.sh
set_error_traps

fgdb_filename=green_fast_track

rm -rf output

echo "Export product tables"
mkdir -p output && (
    cd output

    echo "Copy metadata files"
    cp ../source_data_versions.csv .
    cp ../build_metadata.json .
    
    echo "Generate FileGeodatabase ${fgdb_filename}"
    fgdb_export_partial ${fgdb_filename} MULTIPOLYGON green_fast_track_bbls green_fast_track_bbls ${default_srs}

    fgdb_export_partial ${fgdb_filename} MULTIPOLYGON sources__airports sources__airports ${default_srs} -update

    fgdb_export_partial ${fgdb_filename} MULTILINESTRING sources__arterial_highways sources__arterial_highways ${default_srs} -update
    fgdb_export_partial ${fgdb_filename} MULTIPOLYGON sources__arterial_highways_buffers sources__arterial_highways_buffers ${default_srs} -update

    fgdb_export_partial ${fgdb_filename} MULTILINESTRING sources__exposed_railways sources__exposed_railways ${default_srs} -update
    fgdb_export_partial ${fgdb_filename} MULTIPOLYGON sources__exposed_railways_buffers sources__exposed_railways_buffers ${default_srs} -update
    
    fgdb_export_partial ${fgdb_filename} MULTIPOLYGON sources__vent_towers sources__vent_towers ${default_srs} -update
    fgdb_export_partial ${fgdb_filename} MULTIPOLYGON sources__vent_towers_buffers sources__vent_towers_buffers ${default_srs} -update

    fgdb_export_partial ${fgdb_filename} MULTIPOLYGON sources__industrial_sources_lots sources__industrial_sources_lots ${default_srs} -update
    fgdb_export_partial ${fgdb_filename} MULTIPOLYGON sources__industrial_sources_buffers sources__industrial_sources_buffers ${default_srs} -update

    fgdb_export_partial ${fgdb_filename} POINT sources__cats_permits_points sources__cats_permits_points ${default_srs} -update
    fgdb_export_partial ${fgdb_filename} MULTIPOLYGON sources__cats_permits_lots sources__cats_permits_lots ${default_srs} -update
    fgdb_export_partial ${fgdb_filename} MULTIPOLYGON sources__cats_permits_buffers sources__cats_permits_buffers ${default_srs} -update

    fgdb_export_partial ${fgdb_filename} POINT sources__state_facility_permits_points sources__state_facility_permits_points ${default_srs} -update
    fgdb_export_partial ${fgdb_filename} MULTIPOLYGON sources__state_facility_permits_lots sources__state_facility_permits_lots ${default_srs} -update
    fgdb_export_partial ${fgdb_filename} MULTIPOLYGON sources__state_facility_permits_buffers sources__state_facility_permits_buffers ${default_srs} -update

    fgdb_export_partial ${fgdb_filename} POINT sources__title_v_permits_points sources__title_v_permits_points ${default_srs} -update
    fgdb_export_partial ${fgdb_filename} MULTIPOLYGON sources__title_v_permits_lots sources__title_v_permits_lots ${default_srs} -update
    fgdb_export_partial ${fgdb_filename} MULTIPOLYGON sources__title_v_permits_buffers sources__title_v_permits_buffers ${default_srs} -update

    fgdb_export_partial ${fgdb_filename} MULTIPOLYGON sources__nyc_parks_properties sources__nyc_parks_properties ${default_srs} -update
    fgdb_export_partial ${fgdb_filename} MULTIPOLYGON sources__nyc_parks_properties_buffers sources__nyc_parks_properties_buffers ${default_srs} -update

    fgdb_export_partial ${fgdb_filename} MULTIPOLYGON sources__nys_parks_properties sources__nys_parks_properties ${default_srs} -update
    fgdb_export_partial ${fgdb_filename} MULTIPOLYGON sources__nys_parks_properties_buffers sources__nys_parks_properties_buffers ${default_srs} -update

    fgdb_export_partial ${fgdb_filename} POINT sources__pops_points sources__pops_points ${default_srs} -update
    fgdb_export_partial ${fgdb_filename} MULTIPOLYGON sources__pops_lots sources__pops_lots ${default_srs} -update
    fgdb_export_partial ${fgdb_filename} MULTIPOLYGON sources__pops_buffers sources__pops_buffers ${default_srs} -update

    fgdb_export_partial ${fgdb_filename} MULTIPOLYGON sources__us_parks_properties sources__us_parks_properties ${default_srs} -update
    fgdb_export_partial ${fgdb_filename} MULTIPOLYGON sources__us_parks_properties_buffers sources__us_parks_properties_buffers ${default_srs} -update

    fgdb_export_partial ${fgdb_filename} MULTIPOLYGON sources__waterfront_access_pow sources__waterfront_access_pow ${default_srs} -update
    fgdb_export_partial ${fgdb_filename} MULTIPOLYGON sources__waterfront_access_pow_buffers sources__waterfront_access_pow_buffers ${default_srs} -update

    fgdb_export_partial ${fgdb_filename} MULTIPOINT sources__waterfront_access_wpaa sources__waterfront_access_wpaa ${default_srs} -update
    fgdb_export_partial ${fgdb_filename} MULTIPOLYGON sources__waterfront_access_wpaa_buffers sources__waterfront_access_wpaa_buffers ${default_srs} -update

    fgdb_export_partial ${fgdb_filename} NONE variables variables ${default_srs} -update
    fgdb_export_partial ${fgdb_filename} NONE source_data_versions source_data_versions ${default_srs} -update

    fgdb_export_cleanup ${fgdb_filename}
)

zip -r output/output.zip output
