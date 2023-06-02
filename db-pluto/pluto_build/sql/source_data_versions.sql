DROP TABLE IF EXISTS source_data_versions;
CREATE TABLE source_data_versions as(
    select * from (
    --     (SELECT 'pluto_corrections' as schema_name, v from pluto_corrections limit 1)
    -- union
        (SELECT 'dcp_edesignation' as schema_name, v from dcp_edesignation limit 1)
    union
        (SELECT 'dcp_colp' as schema_name, v from dcp_colp limit 1)
    union
        (SELECT 'lpc_historic_districts' as schema_name, v from lpc_historic_districts limit 1)
    union
        (SELECT 'lpc_landmarks' as schema_name, v from lpc_landmarks limit 1)
    union
        (SELECT 'dcp_cdboundaries_wi' as schema_name, v from dcp_cdboundaries_wi limit 1)
    union
        (SELECT 'dcp_ct2010_wi' as schema_name, v from dcp_ct2010_wi limit 1)
    union
        (SELECT 'dcp_cb2010_wi' as schema_name, v from dcp_cb2010_wi limit 1)
    union
        (SELECT 'dcp_ct2020_wi' as schema_name, v from dcp_ct2020_wi limit 1)
    union
        (SELECT 'dcp_cb2020_wi' as schema_name, v from dcp_cb2020_wi limit 1)
    union
        (SELECT 'dcp_school_districts' as schema_name, v from dcp_school_districts limit 1)
    union
        (SELECT 'dcp_councildistricts_wi' as schema_name, v from dcp_councildistricts_wi limit 1)
    union
        (SELECT 'doitt_zipcodeboundaries' as schema_name, v from doitt_zipcodeboundaries limit 1)
    union
        (SELECT 'dcp_firecompanies' as schema_name, v from dcp_firecompanies limit 1)
    union
        (SELECT 'dcp_policeprecincts' as schema_name, v from dcp_policeprecincts limit 1)
    union
        (SELECT 'dcp_healthareas' as schema_name, v from dcp_healthareas limit 1)
    union
        (SELECT 'dcp_healthcenters' as schema_name, v from dcp_healthcenters limit 1)
    union
        (SELECT 'dsny_frequencies' as schema_name, v from dsny_frequencies limit 1)
    union
        (SELECT 'dpr_greenthumb' as schema_name, v from dpr_greenthumb limit 1)
    union
        (SELECT 'pluto_pts' as schema_name, v from pluto_pts limit 1)
    union
        (SELECT 'pluto_input_geocodes' as schema_name, v from pluto_input_geocodes limit 1)
    union
        (SELECT 'pluto_input_cama_dof' as schema_name, v from pluto_input_cama_dof limit 1)
    union
        (SELECT 'dof_dtm' as schema_name, v from dof_dtm limit 1)
    union
        (SELECT 'dof_shoreline' as schema_name, v from dof_shoreline limit 1)
    union
        (SELECT 'dof_condo' as schema_name, v from dof_condo limit 1)
    union
        (SELECT 'dcp_commercialoverlay' as schema_name, v from dcp_commercialoverlay limit 1)
    union
        (SELECT 'dcp_limitedheight' as schema_name, v from dcp_limitedheight limit 1)
    union
        (SELECT 'dcp_zoningdistricts' as schema_name, v from dcp_zoningdistricts limit 1)
    union
        (SELECT 'dcp_specialpurpose' as schema_name, v from dcp_specialpurpose limit 1)
    union
        (SELECT 'dcp_specialpurposesubdistricts' as schema_name, v from dcp_specialpurposesubdistricts limit 1)
    union
        (SELECT 'dcp_zoningmapamendments' as schema_name, v from dcp_zoningmapamendments limit 1)
    union
        (SELECT 'dcp_zoningmapindex' as schema_name, v from dcp_zoningmapindex limit 1)
    union
        (SELECT 'fema_firms2007_100yr' as schema_name, v from fema_firms2007_100yr limit 1)
    union
        (SELECT 'fema_pfirms2015_100yr' as schema_name, v from fema_pfirms2015_100yr limit 1)
    union
        (SELECT 'doitt_buildingcentroids' as schema_name, v from pluto_input_numbldgs limit 1)
) a order by schema_name);