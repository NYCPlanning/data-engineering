#!/bin/bash
source ./bash/config.sh
set_error_traps

echo "Starting to build PLUTO ..."
run_sql_file sql/preprocessing.sql
run_sql_file sql/create_pts.sql
run_sql_file sql/create_pluto_input_geocodes.sql
run_sql_file sql/create_rpad_geo.sql

echo 'Making DCP edits to RPAD...'
run_sql_file sql/zerovacantlots.sql
run_sql_file sql/lotarea.sql
run_sql_file sql/primebbl.sql
run_sql_file sql/apdate.sql

echo 'Creating table that aggregates condo data and is used to build PLUTO...'
run_sql_file sql/create_allocated.sql
run_sql_file sql/yearbuiltalt.sql

echo 'Creating base PLUTO table'
run_sql_file sql/create.sql -v VERSION=${VERSION}
run_sql_file sql/bbl.sql

echo 'Adding on RPAD data attributes'
run_sql_file sql/allocated.sql

echo 'Adding on spatial data attributes'
run_sql_file sql/geocodes.sql
# clean up numeric fields
run_sql_file sql/numericfields.sql
run_sql_file sql/condono.sql

echo 'Adding on CAMA data attributes'
run_sql_file sql/landuse.sql
run_sql_file sql/create_cama_primebbl.sql

run_sql_file sql/cama_bsmttype.sql
run_sql_file sql/cama_lottype.sql
run_sql_file sql/cama_proxcode.sql
run_sql_file sql/cama_bldgarea_1.sql
run_sql_file sql/cama_bldgarea_2.sql
run_sql_file sql/cama_bldgarea_3.sql
run_sql_file sql/cama_bldgarea_4.sql
run_sql_file sql/cama_easements.sql

echo 'Adding on data attributes from other sources'
run_sql_file sql/lpc.sql
run_sql_file sql/edesignation.sql
run_sql_file sql/ownertype.sql

echo 'Transform RPAD data attributes'
run_sql_file sql/irrlotcode.sql

echo 'Adding DCP data attributes'
run_sql_file sql/address.sql

echo 'Create base DTM'
run_sql_file sql/dedupecondotable.sql
run_sql_file sql/dtmmergepolygons.sql
run_sql_file sql/plutogeoms.sql -v build_schema=${BUILD_ENGINE_SCHEMA}

echo 'Adding in geometries that are in the DTM but not in RPAD'
run_sql_file sql/dtmgeoms.sql
run_sql_command "VACUUM ANALYZE pluto;"

run_sql_file sql/spatialindex.sql
run_sql_command "VACUUM ANALYZE pluto;"

echo 'Computing zoning fields'
run_sql_file sql/zoning_create_priority.sql
run_sql_file sql/zoning_zoningdistrict.sql
run_sql_file sql/zoning_commercialoverlay.sql
run_sql_file sql/zoning_specialdistrict.sql
run_sql_file sql/zoning_limitedheight.sql
run_sql_file sql/zoning_zonemap.sql
run_sql_file sql/zoning_parks.sql
run_sql_file sql/zoning_splitzone.sql
run_sql_command "VACUUM ANALYZE pluto;"

echo 'Filling in FAR values'
run_sql_file sql/far.sql
run_sql_command "VACUUM ANALYZE pluto;"

echo 'Populating building class for condos lots and land use field'
run_sql_file sql/bldgclass.sql
run_sql_file sql/landuse.sql
run_sql_command "VACUUM ANALYZE pluto;"

echo 'Flagging tax lots within the FEMA floodplain'
run_sql_file sql/latlong.sql
run_sql_file sql/update_empty_coord.sql
run_sql_file sql/flood_flag.sql
run_sql_command "VACUUM ANALYZE pluto;"

echo 'Assigning political values with spatial join'
run_sql_file sql/spatialjoins.sql
# clean up numeric fields
run_sql_file sql/numericfields_geomfields.sql
run_sql_file sql/sanitboro.sql
run_sql_file sql/latlong.sql
run_sql_command "VACUUM ANALYZE pluto;"

echo 'Populating PLUTO tags and version fields'
run_sql_file sql/plutomapid.sql
run_sql_command "VACUUM ANALYZE pluto;" & 
run_sql_command "VACUUM ANALYZE dof_shoreline_subdivide;"
run_sql_file sql/plutomapid_1.sql
run_sql_file sql/plutomapid_2.sql
run_sql_file sql/shorelineclip.sql

echo 'Done'
