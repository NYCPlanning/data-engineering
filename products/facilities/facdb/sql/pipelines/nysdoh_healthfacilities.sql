DROP TABLE IF EXISTS _nysdoh_healthfacilities;
SELECT
    uid,
    source,
    facility_name AS facname,
    parsed_hnum AS addressnum,
    parsed_sname AS streetname,
    facility_address_1 AS address,
    facility_city AS city,
    zipcode,
    facility_county AS boro,
    NULL AS borocode,
    NULL AS bin,
    NULL AS bbl,
    description AS factype,
    (
        CASE
            WHEN description LIKE '%Hospice%' THEN 'Residential Health Care'
            WHEN description LIKE '%Adult Day Health%' THEN 'Other Health Care'
            WHEN description LIKE '%Home%' THEN 'Other Health Care'
            ELSE 'Hospitals and Clinics'
        END
    ) AS facsubgrp,
    (
        CASE
            WHEN operator_name = 'City of New York' THEN 'NYC Department of Health and Mental Hygiene'
            WHEN
                operator_name = 'New York City Health and Hospital Corporation'
                THEN 'NYC Health and Hospitals Corporation'
            WHEN ownership_type = 'State' THEN 'NYS Department of Health'
            ELSE operator_name
        END
    ) AS opname,
    (
        CASE
            WHEN operator_name = 'City of New York' THEN 'NYCDOHMH'
            WHEN operator_name = 'New York City Health and Hospitals Corporation' THEN 'NYCHHC'
            WHEN ownership_type = 'State' THEN 'NYSDOH'
            ELSE 'Non-public'
        END
    ) AS opabbrev,
    'NYSDOH' AS overabbrev,
    NULL AS capacity,
    NULL AS captype,
    geom AS wkb_geometry,
    geo_1b,
    NULL AS geo_bl,
    NULL AS geo_bn
INTO _nysdoh_healthfacilities
FROM nysdoh_healthfacilities
WHERE description NOT LIKE '%Residential%' AND description NOT LIKE 'Licensed Home Care Services Agency';

CALL append_to_facdb_base('_nysdoh_healthfacilities');
