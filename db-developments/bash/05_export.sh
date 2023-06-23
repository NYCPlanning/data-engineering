#!/bin/bash
source bash/config.sh

display "Generate output tables"
run_sql_file sql/_export.sql\
    -v VERSION=$VERSION\
    -v CAPTURE_DATE=$CAPTURE_DATE

mkdir -p output 
(
    cd output

    display "Export Devdb and HousingDB"
    csv_export EXPORT_housing housing &
    shp_export SHP_housing POINT housing &

    csv_export EXPORT_devdb devdb &
    shp_export SHP_devdb POINT devdb &
    
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
    
    display "Export Corrections"
    csv_export CORR_hny_matches &
    csv_export corrections_applied &
    csv_export corrections_not_applied &
    csv_export corrections_reference &
    csv_export _manual_corrections manual_corrections &

    display "Export A2 units for review by housing"
    csv_export EXPORT_A2_devdb

    display "Export aggregate tables"
    mkdir -p aggregate 
    (
        cd aggregate
        csv_export aggregate_block_2020_external HousingDB_by_2020_CensusBlock &
        csv_export aggregate_tract_2020_external HousingDB_by_2020_CensusTract &
        csv_export aggregate_nta_2020_external HousingDB_by_2020_NTA &
        csv_export aggregate_councildst_2010_external HousingDB_by_2013_CityCouncilDistrict &
        csv_export aggregate_commntydst_2010_external HousingDB_by_CommunityDistrict &
        csv_export aggregate_cdta_2020_external HousingDB_by_2020_CDTA &
        shp_export aggregate_block_2020_shp MULTIPOLYGON HousingDB_by_2020_CensusBlock &
        shp_export aggregate_tract_2020_shp MULTIPOLYGON HousingDB_by_2020_CensusTract &
        shp_export aggregate_nta_2020_shp MULTIPOLYGON HousingDB_by_2020_NTA &
        shp_export aggregate_councildst_2010_shp MULTIPOLYGON HousingDB_by_2013_CityCouncilDistrict &
        shp_export aggregate_commntydst_2010_shp MULTIPOLYGON HousingDB_by_CommunityDistrict &
        shp_export aggregate_cdta_2020 MULTIPOLYGON_shp HousingDB_by_2020_CDTA 
        wait
    )

    csv_export aggregate_block_2020_internal aggregate_block_2020 &
    csv_export aggregate_tract_2020_internal aggregate_tract_2020 &
    csv_export aggregate_nta_2020_internal aggregate_nta_2020 &
    csv_export aggregate_councildst_2010_internal aggregate_councildst_2010 &
    csv_export aggregate_commntydst_2010_internal aggregate_commntydst_2010 &
    csv_export aggregate_cdta_2020_internal aggregate_cdta_2020

    wait

    display "Export source data versions"
    csv_export source_data_versions
    wait

    display "CSV Export Complete"
    echo "[$(date)] ${VERSION}" > version.txt
)

zip -r output/output.zip output

wait 
display "Upload Complete"