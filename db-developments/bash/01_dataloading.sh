#!/bin/bash
source bash/config.sh

## Default mode is EDM
MODE="${1:-edm}"

max_bg_procs 5

# import spatial bounaries from data library
import_public dcp_cdboundaries $GEOSUPPORT_VERSION &
import_public dcp_cb2010 $GEOSUPPORT_VERSION &
import_public dcp_ct2010 $GEOSUPPORT_VERSION &
import_public dcp_cb2020 $GEOSUPPORT_VERSION &
import_public dcp_ct2020 $GEOSUPPORT_VERSION &
import_public dcp_school_districts $GEOSUPPORT_VERSION &
import_public dcp_boroboundaries_wi $GEOSUPPORT_VERSION &
import_public dcp_councildistricts $GEOSUPPORT_VERSION &
import_public dcp_firecompanies $GEOSUPPORT_VERSION &
import_public dcp_policeprecincts $GEOSUPPORT_VERSION &
import_public doitt_zipcodeboundaries $DOITT_ZIPCODE_VERSION &
import_public dof_shoreline $DOF_VERSION &

import_public dcp_mappluto $DCP_MAPPLUTO_VERSION &
import_public council_members $COUNCIL_MEMBERS_VERSION &
import_public doe_eszones $DOE_ZONES_VERSION &
import_public doe_mszones $DOE_ZONES_VERSION &
import_public doe_school_subdistricts $DOE_SUBDISTRICTS_VERSION &
import_public hpd_hny_units_by_building $HNY_VERSION &
import_public hny_geocode_results $HNY_GEOCODE_VERSION &
import_public dob_now_applications $DOB_NOW_APPS_VERSION &
import_public dob_now_permits $DOB_NOW_PERMITS_VERSION &
import_public dob_cofos $DOB_COFOS_VERSION &
import_public doitt_buildingfootprints $DOITT_BUILDINGS_VERSION &
import_public doitt_buildingfootprints_historical $DOITT_BUILDINGS_HISTORICAL_VERSION &

## Geocode results shares index with _geo_devdb
psql $BUILD_ENGINE -c "DROP TABLE IF EXISTS _geo_devdb;"
case $MODE in
weekly)
    import_public dob_permitissuance &
    import_public dob_jobapplications &
    import_public dob_geocode_results &
    ;;
*)
    import_public dob_permitissuance $DOB_DATA_DATE &
    import_public dob_jobapplications $DOB_DATA_DATE &
    import_public dob_geocode_results $DOB_DATA_DATE &
    ;;
esac

psql $BUILD_ENGINE -f sql/_create.sql

wait
display "data loading is complete"

psql $BUILD_ENGINE -c "
    ALTER TABLE dob_geocode_results
    RENAME TO _GEO_devdb;
"
