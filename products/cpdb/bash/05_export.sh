#!/bin/bash
source bash/config.sh

run_sql_file sql/_create_export.sql -v ccp_v=$ccp_v
run_sql_file sql/opendata.sql -v ccp_v=$ccp_v
python3 python/projects_spending_byyear.py

mkdir -p output && (
    cd output
    shp_export cpdb_dcpattributes_pts MULTIPOINT &
    shp_export cpdb_dcpattributes_poly MULTIPOLYGON &
    shp_export cpdb_opendata_projects_pts MULTIPOINT &
    shp_export cpdb_opendata_projects_poly MULTIPOLYGON &
    csv_export cpdb_adminbounds &
    csv_export cpdb_projects_combined &
    csv_export cpdb_commitments &
    csv_export cpdb_projects &
    csv_export cpdb_budgets &
    csv_export cpdb_projects_spending_byyear &
    csv_export cpdb_opendata_projects &
    csv_export cpdb_opendata_commitments &
    csv_export geospatial_check &
    csv_export source_data_versions &
    echo $DATE > version.txt
    wait 
    echo 
    echo "export complete"
    echo
    zip -r output.zip *
)
