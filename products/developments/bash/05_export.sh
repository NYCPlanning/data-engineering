#!/bin/bash
source bash/config.sh
source sql/constants/export_schemas.sh
set_error_traps

display "Generate output tables"
run_sql_file sql/_export.sql\
    -v VERSION=$VERSION\
    -v CAPTURE_DATE=$CAPTURE_DATE\
    -v internal_columns="$internal_columns"\
    -v external_columns="$external_columns"

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
    csv_export hny_corrections &
    csv_export corrections_applied &
    csv_export corrections_not_applied &
    csv_export corrections_reference &
    csv_export _manual_corrections manual_corrections &

    display "Export A2 units for review by housing"
    csv_export EXPORT_A2_devdb &

    wait

    display "Export unit change summary tables"
    columns_to_drop="'Shape_Area', 'Shape_Leng', 'wkb_geometry'"
    mkdir -p unit_change_summary
    (
        cd unit_change_summary
        csv_export_drop_columns aggregate_block "${columns_to_drop}" HousingDB_by_2020_CensusBlock &
        csv_export_drop_columns aggregate_tract "${columns_to_drop}" HousingDB_by_2020_CensusTract &
        csv_export_drop_columns aggregate_nta "${columns_to_drop}" HousingDB_by_2020_NTA &
        csv_export_drop_columns aggregate_councildst "${columns_to_drop}" HousingDB_by_2024_CityCouncilDistrict &
        csv_export_drop_columns aggregate_commntydst "${columns_to_drop}" HousingDB_by_CommunityDistrict &
        csv_export_drop_columns aggregate_cdta "${columns_to_drop}" HousingDB_by_2020_CDTA &
        shp_export aggregate_block MULTIPOLYGON -f HousingDB_by_2020_CensusBlock -t_srs "EPSG:2263" &
        shp_export aggregate_tract MULTIPOLYGON -f HousingDB_by_2020_CensusTract -t_srs "EPSG:2263" &
        shp_export aggregate_nta MULTIPOLYGON -f HousingDB_by_2020_NTA -t_srs "EPSG:2263" &
        shp_export aggregate_councildst MULTIPOLYGON -f HousingDB_by_2024_CityCouncilDistrict -t_srs "EPSG:2263" &
        shp_export aggregate_commntydst MULTIPOLYGON -f HousingDB_by_CommunityDistrict -t_srs "EPSG:2263" &
        shp_export aggregate_cdta MULTIPOLYGON -f HousingDB_by_2020_CDTA -t_srs "EPSG:2263" &
        wait
    )

    display "Export project-level files"
    mkdir -p project_level_internal
    (
        cd project_level_internal
        for table in HousingDB_post2010 HousingDB_post2010_inactive_included
        do
            csv_export_drop_columns ${table}_internal "'geom'" $table &
            shp_export ${table}_internal POINT -f $table -t_srs "EPSG:2263" &
        done
        wait
    )

    mkdir -p project_level_external
    (
        cd project_level_external
        for table in HousingDB_post2010 HousingDB_post2010_inactive_included
        do
            csv_export_drop_columns ${table}_external "'geom'" $table &
            shp_export ${table}_external POINT -f $table -t_srs "EPSG:2263" &
        done
        wait
    )

    display "Export source data versions and build metadata"
    csv_export source_data_versions
    cp ../build_metadata.json ./
    echo "${VERSION}" > version.txt

)

wait
zip -r output/output.zip output

display "Export Complete"
