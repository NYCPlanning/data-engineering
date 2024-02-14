#!/bin/bash
source bash/config.sh

set_error_traps

run_sql_file sql/_create_export.sql
python3 python/checkbook_spending_by_year.py

mkdir -p output && (
    cd output
    
    csv_export cpdb_adminbounds &
    csv_export cpdb_projects &
    csv_export ccp_commitments cpdb_planned_commitments &
    csv_export cpdb_budget_data &
    csv_export ccp_budgets cpdb_budgets &
    csv_export checkbook_spending_by_year &
    csv_export geospatial_check &
    csv_export cpdb_projects_without_budget_data &

    cp ../source_data_versions.csv ./
    cp ../build_metadata.json ./

    shp_export cpdb_dcpattributes_pts MULTIPOINT &
    shp_export cpdb_dcpattributes_poly MULTIPOLYGON &
    shp_export cpdb_projects_pts MULTIPOINT &
    shp_export cpdb_projects_poly MULTIPOLYGON &

    echo $VERSION > version.txt
    wait 

    echo 
    echo "export complete"
    echo

    zip -r output.zip *
)
