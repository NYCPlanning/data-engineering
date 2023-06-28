#!/bin/bash
source ../../bash/utils.sh
set_env ../../.env
set_env ./version.env

# DROP all tables
if [[ ${1} == "drop" ]]; then
    echo "Dropping all tables"
    run_sql_command "
    DO \$\$ DECLARE
        r RECORD;
    BEGIN
        FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = 'public' and tablename !='spatial_ref_sys') LOOP
            EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(r.tablename) || ' CASCADE';
        END LOOP;
    END \$\$;
    "
fi

echo "Creating source data table"
create_source_data_table

# This shell script is for minor releases of PLUTO where all datasets remain constant
# except for DCP data that is updated monthly and used by ZOLA (e.g. E-Designations, zoning files)

echo "import zoning files from data library - default to latest"
import_recipe dcp_edesignation &
import_recipe dcp_commercialoverlay &
import_recipe dcp_limitedheight &
import_recipe dcp_zoningdistricts &
import_recipe dcp_specialpurpose &
import_recipe dcp_specialpurposesubdistricts &
import_recipe dcp_zoningmapamendments &
import_recipe dcp_zoningmapindex &

echo "import PTS and CAMA from data library"
import_recipe pluto_input_numbldgs ${DOF_WEEKLY_DATA_VERSION} &
import_recipe pluto_input_geocodes ${DOF_WEEKLY_DATA_VERSION} &
import_recipe pluto_pts ${DOF_WEEKLY_DATA_VERSION} &
import_recipe pluto_input_cama_dof ${DOF_CAMA_DATA_VERSION} &

echo "import spatial bounaries from data library"
import_recipe dcp_cdboundaries_wi ${GEOSUPPORT_VERSION} &
import_recipe dcp_cb2010_wi ${GEOSUPPORT_VERSION} &
import_recipe dcp_ct2010_wi ${GEOSUPPORT_VERSION} &
import_recipe dcp_cb2020_wi ${GEOSUPPORT_VERSION} &
import_recipe dcp_ct2020_wi ${GEOSUPPORT_VERSION} &
import_recipe dcp_school_districts ${GEOSUPPORT_VERSION} &
import_recipe dcp_firecompanies ${GEOSUPPORT_VERSION} &
import_recipe dcp_policeprecincts ${GEOSUPPORT_VERSION} &
import_recipe dcp_councildistricts_wi ${GEOSUPPORT_CITYCOUNCIL} &
import_recipe dcp_healthareas ${GEOSUPPORT_VERSION} &
import_recipe dcp_healthcenters ${GEOSUPPORT_VERSION} &
import_recipe fema_firms2007_100yr ${FEMA_FIRPS_VERSION} &
import_recipe fema_pfirms2015_100yr ${FEMA_FIRPS_VERSION} &
import_recipe doitt_zipcodeboundaries ${DOITT_DATA_VERSION} & 
import_recipe dof_shoreline ${DOF_DATA_VERSION} & 

echo "import other"
import_recipe dof_dtm ${DOF_DATA_VERSION_DTM} & 
import_recipe dof_condo ${DOF_DATA_VERSION} &
import_recipe dcp_colp ${DCP_COLP_VERSION} &
import_recipe dpr_greenthumb ${DPR_GREENTHUMB_VERSION} &
import_recipe dsny_frequencies ${DSNY_FREQUENCIES_VERSION} &
import_recipe lpc_historic_districts ${LPC_HISTORIC_DISTRICTS_VERSION} &
import_recipe lpc_landmarks ${LPC_LANDMARKS_VESRSION}

# import pluto corrections
# import_recipe pluto_corrections $PLUTO_CORRECTIONS_VERSION &

wait

## Load local CSV files
run_sql_file sql/_create.sql