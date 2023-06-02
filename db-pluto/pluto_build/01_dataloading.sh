#!/bin/bash
source bin/config.sh
set_error_traps

# DROP all tables
if [[ $1 == "drop" ]]; then
    psql $BUILD_ENGINE -c "
    DO \$\$ DECLARE
        r RECORD;
    BEGIN
        FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = 'public' and tablename !='spatial_ref_sys') LOOP
            EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(r.tablename) || ' CASCADE';
        END LOOP;
    END \$\$;
    "
fi

# import_public PTS and CAMA from data library
import_public pluto_pts &
import_public pluto_input_cama_dof &
import_public pluto_input_numbldgs &
import_public pluto_input_geocodes &

# import_public spatial bounaries from data library
import_public dcp_edesignation &
import_public dcp_cdboundaries_wi &
import_public dcp_cb2010_wi &
import_public dcp_ct2010_wi &
import_public dcp_cb2020_wi &
import_public dcp_ct2020_wi &
import_public dcp_school_districts &
import_public dcp_firecompanies &
import_public dcp_policeprecincts &
import_public dcp_councildistricts_wi $GEOSUPPORT_CITYCOUNCIL &
import_public dcp_healthareas &
import_public dcp_healthcenters &
import_public dof_shoreline &
import_public doitt_zipcodeboundaries &
import_public fema_firms2007_100yr &
import_public fema_pfirms2015_100yr &

# import_public zoning files from data library
import_public dcp_commercialoverlay &
import_public dcp_limitedheight &
import_public dcp_zoningdistricts &
import_public dcp_specialpurpose &
import_public dcp_specialpurposesubdistricts &
import_public dcp_zoningmapamendments &
import_public dcp_zoningmapindex &

# import_public other
# import_public pluto_corrections &
import_public dpr_greenthumb &
import_public dsny_frequencies &
import_public lpc_historic_districts &
import_public lpc_landmarks &
import_public dcp_colp &
import_public dof_dtm &
import_public dof_condo

wait

## Load local CSV files
psql $BUILD_ENGINE -v ON_ERROR_STOP=1 -f sql/_create.sql

# Create data version table
psql $BUILD_ENGINE -v ON_ERROR_STOP=1 -f sql/source_data_versions.sql
