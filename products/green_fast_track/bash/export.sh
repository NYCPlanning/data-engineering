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

    fgdb_export_partial ${fgdb_filename} MULTIPOLYGON sources__airports sources__airports ${default_srs} -update

    fgdb_export_partial ${fgdb_filename} MULTILINESTRING sources__arterial_highways sources__arterial_highways ${default_srs} -update
    fgdb_export_partial ${fgdb_filename} MULTIPOLYGON sources__arterial_highways_buffers sources__arterial_highways_buffers ${default_srs} -update

    fgdb_export_partial ${fgdb_filename} MULTILINESTRING sources__elevated_railways sources__elevated_railways ${default_srs} -update
    fgdb_export_partial ${fgdb_filename} MULTIPOLYGON sources__elevated_railways_buffers sources__elevated_railways_buffers ${default_srs} -update
    
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
    
    fgdb_export_partial ${fgdb_filename} NONE variables variables ${default_srs} -update
    fgdb_export_partial ${fgdb_filename} NONE source_data_versions source_data_versions ${default_srs} -update

    fgdb_export_cleanup ${fgdb_filename}
)

zip -r output/output.zip output
