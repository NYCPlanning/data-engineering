#!/bin/bash
source bash/config.sh

run_sql_file sql/_create_export.sql -v ccp_v=$ccp_v
python3 python/checkbook_spending_by_year.py

mkdir -p output && (
    cd output
    csv_export cpdb_adminbounds &
    csv_export ccp_commitments cpdb_commitments &
    csv_export ccp_budgets cpdb_budgets &
    csv_export ccp_projects cpdb_projects &
    csv_export cpdb_projects_combined &
    csv_export checkbook_spending_by_year &
    csv_export geospatial_check &
    csv_export source_data_versions &
    shp_export cpdb_dcpattributes_pts MULTIPOINT &
    shp_export cpdb_dcpattributes_poly MULTIPOLYGON &
    echo $DATE > version.txt
    wait 
    echo 
    echo "export complete"
    echo
    zip -r output.zip *
)
