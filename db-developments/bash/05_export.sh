#!/bin/bash
source bash/config.sh
set_error_traps

display "Generate output tables"
run_sql_file sql/_export.sql\
    -v VERSION=$VERSION\
    -v CAPTURE_DATE=$CAPTURE_DATE

mkdir -p output 
(
    cd output

    display "Export Devdb and HousingDB"
    csv_export EXPORT_housing housing &
    shp_export SHP_housing POINT -f housing &

    csv_export EXPORT_devdb devdb &
    shp_export SHP_devdb POINT -f devdb &
    
    display "Export HNY_devdb_lookup"
    csv_export HNY_devdb_lookup &

    display "Export QAQC Tables"
    csv_export FINAL_qaqc &
    csv_export HNY_no_match & 
    csv_export qaqc_app &
    csv_export qaqc_historic
    pg_dump -d $BUILD_ENGINE -t qaqc_historic -f qaqc_historic.sql
    csv_export qaqc_field_distribution &
    csv_export qaqc_quarter_check &

    wait
    
    display "Export Corrections"
    csv_export CORR_hny_matches &
    csv_export corrections_applied &
    csv_export corrections_not_applied &
    csv_export corrections_reference &
    csv_export _manual_corrections manual_corrections &

    display "Export A2 units for review by housing"
    csv_export EXPORT_A2_devdb &

    wait

    display "Export aggregate tables"
    columns_to_drop="'Shape_Area', 'Shape_Leng', 'wkb_geometry'"
    mkdir -p aggregate
    (
        cd aggregate
        csv_export_drop_columns aggregate_block "${columns_to_drop}" &
        csv_export_drop_columns aggregate_tract "${columns_to_drop}" &
        csv_export_drop_columns aggregate_nta "${columns_to_drop}" HousingDB_by_2020_NTA &
        csv_export_drop_columns aggregate_councildst "${columns_to_drop}" &
        csv_export_drop_columns aggregate_commntydst "${columns_to_drop}" &
        csv_export_drop_columns aggregate_cdta "${columns_to_drop}" &
        wait
    )
    mkdir -p bytes_unit_change_summary
    (
        cd bytes_unit_change_summary
        csv_export_drop_columns aggregate_block_external "${columns_to_drop}" HousingDB_by_2020_CensusBlock &
        csv_export_drop_columns aggregate_tract_external "${columns_to_drop}" HousingDB_by_2020_CensusTract &
        csv_export_drop_columns aggregate_nta_external "${columns_to_drop}" HousingDB_by_2020_NTA &
        csv_export_drop_columns aggregate_councildst_external "${columns_to_drop}" HousingDB_by_2013_CityCouncilDistrict &
        csv_export_drop_columns aggregate_commntydst_external "${columns_to_drop}" HousingDB_by_CommunityDistrict &
        csv_export_drop_columns aggregate_cdta_external "${columns_to_drop}" HousingDB_by_2020_CDTA &
        shp_export aggregate_block_external MULTIPOLYGON -f HousingDB_by_2020_CensusBlock -t_srs "EPSG:2263" &
        shp_export aggregate_tract_external MULTIPOLYGON -f HousingDB_by_2020_CensusTract -t_srs "EPSG:2263" &
        shp_export aggregate_nta_external MULTIPOLYGON -f HousingDB_by_2020_NTA -t_srs "EPSG:2263" &
        shp_export aggregate_councildst_external MULTIPOLYGON -f HousingDB_by_2013_CityCouncilDistrict -t_srs "EPSG:2263" &
        shp_export aggregate_commntydst_external MULTIPOLYGON -f HousingDB_by_CommunityDistrict -t_srs "EPSG:2263" &
        shp_export aggregate_cdta_external MULTIPOLYGON -f HousingDB_by_2020_CDTA -t_srs "EPSG:2263" &
        wait
    )

    display "Export bytes project-level files"
    mkdir -p bytes_project_level
    (
        cd bytes_project_level
        csv_export_drop_columns HousingDB_post2010 "'geom'" &
        csv_export_drop_columns HousingDB_post2010_completed_jobs "'geom'" &
        csv_export_drop_columns HousingDB_post2010_incomplete_jobs "'geom'" &
        csv_export_drop_columns HousingDB_post2010_inactive_jobs "'geom'" &
        csv_export_drop_columns HousingDB_post2010_inactive_included "'geom'" &
        shp_export HousingDB_post2010 POINT -t_srs "EPSG:2263" &
        shp_export HousingDB_post2010_completed_jobs POINT -t_srs "EPSG:2263" &
        shp_export HousingDB_post2010_incomplete_jobs POINT -t_srs "EPSG:2263" &
        shp_export HousingDB_post2010_inactive_jobs POINT -t_srs "EPSG:2263" &
        shp_export HousingDB_post2010_inactive_included POINT -t_srs "EPSG:2263" &
        wait
    )

    display "Export source data versions"
    csv_export source_data_versions
    echo "[$(date)] ${VERSION}" > version.txt

)

wait
zip -r output/output.zip output

display "Export Complete"
