#!/bin/bash
source ../../bash/utils.sh
set_error_traps

python3 python/checkbook_spending_by_year.py

# Create and export tables of projects in geographies of interest
python3 -m python.projects_in_geographies

mkdir -p output && (
    cd output
    
    csv_export cpdb_adminbounds &
    csv_export cpdb_projects &
    csv_export int__ccp_commitments cpdb_planned_commitments &
    csv_export cpdb_budget_data &
    csv_export cpdb_budgets &
    csv_export checkbook_spending_by_year &
    csv_export geospatial_check &
    csv_export cpdb_projects_without_budget_data &
    csv_export cpdb_mapping_summary_by_source &
    csv_export cpdb_mapping_summary_by_source_and_agency &

    cp ../source_data_versions.csv ./
    cp ../build_metadata.json ./

    shp_export cpdb_dcpattributes_pts MULTIPOINT &
    shp_export cpdb_dcpattributes_poly MULTIPOLYGON &
    shp_export cpdb_projects_pts MULTIPOINT &
    shp_export cpdb_projects_poly MULTIPOLYGON &

    # projects with borough and community district, for sharing without changing the
    # published files: points and polygons shapefiles and a CSV of every project, mapped or
    # not. The GeoPackage has the same three as layers.
    shp_export cpdb_projects_boundaries_pts MULTIPOINT &
    shp_export cpdb_projects_boundaries_poly MULTIPOLYGON &
    csv_export cpdb_projects_boundaries_csv cpdb_projects_boundaries &
    (
        ogr2ogr -f GPKG cpdb_projects_boundaries.gpkg PG:${BUILD_ENGINE} \
            ${BUILD_ENGINE_SCHEMA:-public}.cpdb_projects_boundaries_pts \
            -nln cpdb_projects_boundaries_pts -nlt MULTIPOINT
        ogr2ogr -f GPKG -update cpdb_projects_boundaries.gpkg PG:${BUILD_ENGINE} \
            ${BUILD_ENGINE_SCHEMA:-public}.cpdb_projects_boundaries_poly \
            -nln cpdb_projects_boundaries_poly -nlt MULTIPOLYGON
        ogr2ogr -f GPKG -update cpdb_projects_boundaries.gpkg PG:${BUILD_ENGINE} \
            ${BUILD_ENGINE_SCHEMA:-public}.cpdb_projects_boundaries \
            -nln cpdb_projects_boundaries -nlt GEOMETRY
    ) &

    # one GeoPackage for reviewing how projects were mapped. Layers are written one at a time
    # since a GeoPackage can't take concurrent writes.
    (
        ogr2ogr -f GPKG cpdb_mapping_review.gpkg PG:${BUILD_ENGINE} \
            ${BUILD_ENGINE_SCHEMA:-public}.cpdb_projects -nln cpdb_projects
        for table in \
            cpdb_dcpattributes \
            int__project_geom_candidates \
            int__project_mapping_outcomes \
            int__project_boundaries \
            cpdb_mapping_summary_by_source \
            cpdb_mapping_summary_by_source_and_agency
        do
            ogr2ogr -f GPKG -update cpdb_mapping_review.gpkg PG:${BUILD_ENGINE} \
                ${BUILD_ENGINE_SCHEMA:-public}.${table} -nln ${table}
        done
    ) &

    mv ../projects_in_geographies.zip ../projects_in_geographies/
    mv ../projects_in_geographies ./

    echo $VERSION > version.txt
    wait_for_jobs

    echo 
    echo "export complete"
    echo

    zip -r output.zip *
)
