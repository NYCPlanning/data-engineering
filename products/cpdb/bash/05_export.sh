#!/bin/bash
source ../../bash/utils.sh
set_error_traps

run_sql_file sql/_create_export.sql
python3 python/checkbook_spending_by_year.py

# Create and export tables of projects in geographies of interest
python3 -m python.projects_in_geographies

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
    
    mv ../projects_in_geographies.zip ../projects_in_geographies/
    mv ../projects_in_geographies ./

    echo $VERSION > version.txt
    wait 

    echo 
    echo "export complete"
    echo

    zip -r output.zip *
)
