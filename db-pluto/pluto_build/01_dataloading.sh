#!/bin/bash
source ../../bash/utils.sh
set_env ../../.env
set_env ./version.env
set_error_traps

# DROP all tables
if [[ $1 == "drop" ]]; then
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

create_source_data_table

# import_recipe PTS and CAMA from data library
import_recipe pluto_pts &
import_recipe pluto_input_cama_dof &
import_recipe pluto_input_numbldgs &
import_recipe pluto_input_geocodes &

# import_recipe spatial bounaries from data library
import_recipe dcp_edesignation &
import_recipe dcp_cdboundaries_wi &
import_recipe dcp_cb2010_wi &
import_recipe dcp_ct2010_wi &
import_recipe dcp_cb2020_wi &
import_recipe dcp_ct2020_wi &
import_recipe dcp_school_districts &
import_recipe dcp_firecompanies &
import_recipe dcp_policeprecincts &
import_recipe dcp_councildistricts_wi ${GEOSUPPORT_CITYCOUNCIL} &
import_recipe dcp_healthareas &
import_recipe dcp_healthcenters &
import_recipe dof_shoreline &
import_recipe doitt_zipcodeboundaries &
import_recipe fema_firms2007_100yr &
import_recipe fema_pfirms2015_100yr &

# import_recipe zoning files from data library
import_recipe dcp_commercialoverlay &
import_recipe dcp_limitedheight &
import_recipe dcp_zoningdistricts &
import_recipe dcp_specialpurpose &
import_recipe dcp_specialpurposesubdistricts &
import_recipe dcp_zoningmapamendments &
import_recipe dcp_zoningmapindex &

# import_recipe other
# import_recipe pluto_corrections &
import_recipe dpr_greenthumb &
import_recipe dsny_frequencies &
import_recipe lpc_historic_districts &
import_recipe lpc_landmarks &
import_recipe dcp_colp &
import_recipe dof_dtm &
import_recipe dof_condo

wait

## Load local CSV files
run_sql_file sql/_create.sql
