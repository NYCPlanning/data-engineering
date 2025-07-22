#!/bin/bash
source ../../bash/utils.sh
set_error_traps

# Summary table by managing and sponsor agency
echo 'Creating summary tables by managing and sponsor agency'
run_sql_file sql/analysis/projects_dollars_mapped_categorized_managing.sql
run_sql_file sql/analysis/projects_dollars_mapped_categorized_sponsor.sql

# Planned commitments by community district
echo 'Creating reports by community district'
run_sql_file sql/analysis/projects_by_communitydist.sql
run_sql_file sql/analysis/projects_by_communitydist_spending.sql
run_sql_file sql/analysis/projects_by_communitydist_spending_date.sql

# # Agency validated geoms summary table
echo 'Creating agency validated geoms summary table'
run_sql_file sql/analysis/agency_validated_geoms_summary_table.sql

# Geospatial check 
echo 'Creating geospatial check table'
run_sql_file sql/analysis/geospatial_check.sql -v ccp_v=$ccp_v 

mkdir -p $(pwd)/output/analysis && (
    cd $(pwd)/output/analysis

    csv_export cpdb_summarystats_magency &
    csv_export cpdb_summarystats_sagency &
    csv_export projects_by_communitydist &
    csv_export projects_by_communitydist_spending &
    csv_export projects_by_communitydist_spending_date &
    csv_export agency_validated_geoms_summary_table &
    wait 
    echo "Analysis Done!"
)
