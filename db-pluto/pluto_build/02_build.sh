#!/bin/bash
source bin/config.sh

echo "Starting to build PLUTO ..."
psql $BUILD_ENGINE -f sql/preprocessing.sql
psql $BUILD_ENGINE -f sql/create_pts.sql
psql $BUILD_ENGINE -f sql/create_rpad_geo.sql

echo 'Making DCP edits to RPAD...'
psql $BUILD_ENGINE -f sql/zerovacantlots.sql
psql $BUILD_ENGINE -f sql/lotarea.sql
psql $BUILD_ENGINE -f sql/primebbl.sql
psql $BUILD_ENGINE -f sql/apdate.sql

echo 'Creating table that aggregates condo data and is used to build PLUTO...'
psql $BUILD_ENGINE -f sql/create_allocated.sql
psql $BUILD_ENGINE -f sql/yearbuiltalt.sql

echo 'Creating base PLUTO table'
psql $BUILD_ENGINE -v version=$VERSION -f sql/create.sql
psql $BUILD_ENGINE -f sql/bbl.sql

echo 'Adding on RPAD data attributes'
psql $BUILD_ENGINE -f sql/allocated.sql

echo 'Adding on spatial data attributes'
psql $BUILD_ENGINE -f sql/geocodes.sql
# clean up numeric fields
psql $BUILD_ENGINE -f sql/numericfields.sql
psql $BUILD_ENGINE -f sql/condono.sql

echo 'Adding on CAMA data attributes'
psql $BUILD_ENGINE -f sql/landuse.sql
psql $BUILD_ENGINE -f sql/create_cama_primebbl.sql

psql $BUILD_ENGINE -f sql/cama_bsmttype.sql
psql $BUILD_ENGINE -f sql/cama_lottype.sql
psql $BUILD_ENGINE -f sql/cama_proxcode.sql
psql $BUILD_ENGINE -f sql/cama_bldgarea_1.sql
psql $BUILD_ENGINE -f sql/cama_bldgarea_2.sql
psql $BUILD_ENGINE -f sql/cama_bldgarea_3.sql
psql $BUILD_ENGINE -f sql/cama_bldgarea_4.sql
psql $BUILD_ENGINE -f sql/cama_easements.sql

echo 'Adding on data attributes from other sources'
psql $BUILD_ENGINE -f sql/lpc.sql
psql $BUILD_ENGINE -f sql/edesignation.sql
psql $BUILD_ENGINE -f sql/ownertype.sql

echo 'Transform RPAD data attributes'
psql $BUILD_ENGINE -f sql/irrlotcode.sql

echo 'Adding DCP data attributes'
psql $BUILD_ENGINE -f sql/address.sql

echo 'Create base DTM'
psql $BUILD_ENGINE -f sql/dedupecondotable.sql
psql $BUILD_ENGINE -f sql/dtmmergepolygons.sql
psql $BUILD_ENGINE -f sql/plutogeoms.sql

echo 'Adding in geometries that are in the DTM but not in RPAD'
psql $BUILD_ENGINE -f sql/dtmgeoms.sql
psql $BUILD_ENGINE -c "VACUUM ANALYZE pluto;"

psql $BUILD_ENGINE -f sql/spatialindex.sql
psql $BUILD_ENGINE -c "VACUUM ANALYZE pluto;"

echo 'Computing zoning fields'
psql $BUILD_ENGINE -f sql/zoning_create_priority.sql
psql $BUILD_ENGINE -f sql/zoning_zoningdistrict_create.sql
psql $BUILD_ENGINE -f sql/zoning_zoningdistrict.sql
psql $BUILD_ENGINE -f sql/zoning_commercialoverlay.sql
psql $BUILD_ENGINE -f sql/zoning_specialdistrict.sql
psql $BUILD_ENGINE -f sql/zoning_limitedheight.sql
psql $BUILD_ENGINE -f sql/zoning_zonemap.sql
psql $BUILD_ENGINE -f sql/zoning_parks.sql
psql $BUILD_ENGINE -f sql/zoning_correctdups.sql
psql $BUILD_ENGINE -f sql/zoning_correctgaps.sql
psql $BUILD_ENGINE -f sql/zoning_splitzone.sql
psql $BUILD_ENGINE -c "VACUUM ANALYZE pluto;"

echo 'Filling in FAR values'
psql $BUILD_ENGINE -f sql/far.sql
psql $BUILD_ENGINE -c "VACUUM ANALYZE pluto;"

echo 'Populating building class for condos lots and land use field'
psql $BUILD_ENGINE -f sql/bldgclass.sql
psql $BUILD_ENGINE -f sql/landuse.sql
psql $BUILD_ENGINE -c "VACUUM ANALYZE pluto;"

echo 'Flagging tax lots within the FEMA floodplain'
psql $BUILD_ENGINE -f sql/latlong.sql
psql $BUILD_ENGINE -f sql/update_empty_coord.sql
psql $BUILD_ENGINE -f sql/flood_flag.sql
psql $BUILD_ENGINE -c "VACUUM ANALYZE pluto;"

echo 'Assigning political values with spatial join'
psql $BUILD_ENGINE -f sql/spatialjoins.sql
# clean up numeric fields
psql $BUILD_ENGINE -f sql/numericfields_geomfields.sql
psql $BUILD_ENGINE -f sql/sanitboro.sql
psql $BUILD_ENGINE -f sql/latlong.sql
psql $BUILD_ENGINE -c "VACUUM ANALYZE pluto;"

echo 'Populating PLUTO tags and version fields'
psql $BUILD_ENGINE -v ON_ERROR_STOP=1 -f sql/plutomapid.sql
psql $BUILD_ENGINE -c "VACUUM ANALYZE pluto;" & 
psql $BUILD_ENGINE -c "VACUUM ANALYZE dof_shoreline_subdivide;"
psql $BUILD_ENGINE -v ON_ERROR_STOP=1 -f sql/plutomapid_1.sql
psql $BUILD_ENGINE -v ON_ERROR_STOP=1 -f sql/plutomapid_2.sql
psql $BUILD_ENGINE -f sql/shorelineclip.sql

echo 'Done'
exit 0