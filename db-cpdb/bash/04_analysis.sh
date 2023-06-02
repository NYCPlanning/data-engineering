#!/bin/bash
source bash/config.sh


# Summary table by managing and sponsor agency
echo 'Creating summary tables by managing and sponsor agency'
psql $BUILD_ENGINE -f analysis/projects_dollars_mapped_categorized_managing.sql
psql $BUILD_ENGINE -f analysis/projects_dollars_mapped_categorized_sponsor.sql

# Planned commitments by community district
echo 'Creating reports by community district'
psql $BUILD_ENGINE -f analysis/projects_by_communitydist.sql
psql $BUILD_ENGINE -f analysis/projects_by_communitydist_spending.sql
psql $BUILD_ENGINE -f analysis/projects_by_communitydist_spending_date.sql

# # Agency validated geoms summary table
echo 'Creating agency validated geoms summary table'
psql $BUILD_ENGINE -f analysis/agency_validated_geoms_summary_table.sql

# Geospatial check 
echo 'Creating geospatial check table'
psql $BUILD_ENGINE -v ccp_v=$ccp_v -f analysis/geospatial_check.sql

mkdir -p $(pwd)/output/analysis && (
    cd $(pwd)/output/analysis
    CSV_export cpdb_summarystats_magency &
    CSV_export cpdb_summarystats_sagency &
    CSV_export projects_by_communitydist &
    CSV_export projects_by_communitydist_spending &
    CSV_export projects_by_communitydist_spending_date &
    CSV_export agency_validated_geoms_summary_table &
    wait 
    echo "Analysis Done!"
)