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
    
    # TODO export all relevant source data (e.g. buffered and original geometries of Title V permits)
    echo "Generate FileGeodatabase ${fgdb_filename}"
    fgdb_export_partial ${fgdb_filename} MULTIPOLYGON green_fast_track_bbls green_fast_track_bbls ${default_srs}

    fgdb_export_partial ${fgdb_filename} POINT cats_permits_points cats_permits_points ${default_srs} -update
    fgdb_export_partial ${fgdb_filename} MULTIPOLYGON cats_permits_lots cats_permits_lots ${default_srs} -update
    fgdb_export_partial ${fgdb_filename} MULTIPOLYGON cats_permits_buffered cats_permits_buffered ${default_srs} -update

    fgdb_export_partial ${fgdb_filename} POINT state_facility_permits_points state_facility_permits_points ${default_srs} -update
    fgdb_export_partial ${fgdb_filename} MULTIPOLYGON state_facility_permits_lots state_facility_permits_lots ${default_srs} -update
    fgdb_export_partial ${fgdb_filename} MULTIPOLYGON state_facility_permits_buffered state_facility_permits_buffered ${default_srs} -update

    fgdb_export_partial ${fgdb_filename} POINT title_v_permits_points title_v_permits_points ${default_srs} -update
    fgdb_export_partial ${fgdb_filename} MULTIPOLYGON title_v_permits_lots title_v_permits_lots ${default_srs} -update
    fgdb_export_partial ${fgdb_filename} MULTIPOLYGON title_v_permits_buffered title_v_permits_buffered ${default_srs} -update

    fgdb_export_partial ${fgdb_filename} MULTIPOLYGON airports airports ${default_srs} -update

    fgdb_export_partial ${fgdb_filename} MULTIPOLYGON arterial_highways_buffered arterial_highways_buffered ${default_srs} -update

    fgdb_export_partial ${fgdb_filename} MULTIPOLYGON elevated_railways_buffered elevated_railways_buffered ${default_srs} -update
    
    fgdb_export_partial ${fgdb_filename} MULTIPOLYGON vent_towers_buffered vent_towers_buffered ${default_srs} -update
    
    fgdb_export_partial ${fgdb_filename} NONE variables variables ${default_srs} -update
    fgdb_export_partial ${fgdb_filename} NONE source_data_versions source_data_versions ${default_srs} -update

    fgdb_export_cleanup ${fgdb_filename}
)

zip -r output/output.zip output
