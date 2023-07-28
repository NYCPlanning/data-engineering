#!/bin/bash
source bash/config.sh
set_error_traps

## Default mode is EDM
MODE="${1:-edm}"

max_bg_procs 5

create_source_data_table

# import spatial bounaries from data library
import_recipe dcp_cdboundaries $GEOSUPPORT_VERSION &
import_recipe dcp_cb2020 $GEOSUPPORT_VERSION &
import_recipe dcp_ct2020 $GEOSUPPORT_VERSION &
import_recipe dcp_cdta2020 $GEOSUPPORT_VERSION &
import_recipe dcp_nta2020 $GEOSUPPORT_VERSION &
import_recipe dcp_school_districts $GEOSUPPORT_VERSION &
import_recipe dcp_boroboundaries_wi $GEOSUPPORT_VERSION &
import_recipe dcp_councildistricts $GEOSUPPORT_VERSION &
import_recipe dcp_firecompanies $GEOSUPPORT_VERSION &
import_recipe dcp_policeprecincts $GEOSUPPORT_VERSION &
import_recipe doitt_zipcodeboundaries $DOITT_ZIPCODE_VERSION &
import_recipe dof_shoreline $DOF_VERSION &

import_recipe dcp_mappluto $DCP_MAPPLUTO_VERSION &
import_recipe council_members $COUNCIL_MEMBERS_VERSION &
import_recipe doe_eszones $DOE_ZONES_VERSION &
import_recipe doe_mszones $DOE_ZONES_VERSION &
import_recipe doe_school_subdistricts $DOE_SUBDISTRICTS_VERSION &
import_recipe hpd_hny_units_by_building $HNY_VERSION &
import_recipe hny_geocode_results $HNY_GEOCODE_VERSION &
import_recipe hpd_historical_units_by_building &
import_recipe hpd_historical_geocode_results &
import_recipe dob_now_applications $DOB_NOW_APPS_VERSION &
import_recipe dob_now_permits $DOB_NOW_PERMITS_VERSION &
import_recipe dob_cofos $DOB_COFOS_VERSION &
import_recipe doitt_buildingfootprints $DOITT_BUILDINGS_VERSION &
import_recipe doitt_buildingfootprints_historical $DOITT_BUILDINGS_HISTORICAL_VERSION &

import_recipe dcp_censusdata 2020 & 
import_recipe dcp_censusdata_blocks 2020 &

## Geocode results shares index with _geo_devdb
run_sql_command "DROP TABLE IF EXISTS _geo_devdb;"

import_recipe dob_permitissuance $DOB_DATA_DATE &
import_recipe dob_jobapplications $DOB_DATA_DATE &
import_recipe dob_geocode_results $DOB_DATA_DATE &

run_sql_file sql/_create.sql

wait
display "data loading is complete"

run_sql_command "
    ALTER TABLE dob_geocode_results
    RENAME TO _GEO_devdb;
"
