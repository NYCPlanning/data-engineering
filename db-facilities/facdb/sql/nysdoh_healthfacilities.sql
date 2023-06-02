DROP TABLE IF EXISTS _nysdoh_healthfacilities;
SELECT uid,
    source,
    facility_name as facname,
    parsed_hnum as addressnum,
    parsed_sname as streetname,
    facility_address_1 as address,
    facility_city as city,
    zipcode,
    facility_county as boro,
    NULL as borocode,
    NULL as bin,
    NULL as bbl,
    description as factype,
    (
        CASE
            WHEN description LIKE '%Hospice%' THEN 'Residential Health Care'
            WHEN description LIKE '%Adult Day Health%' THEN 'Other Health Care'
            WHEN description LIKE '%Home%' THEN 'Other Health Care'
            ELSE 'Hospitals and Clinics'
        END
    ) as facsubgrp,
    (
        CASE
            WHEN operator_name = 'City of New York' THEN 'NYC Department of Health and Mental Hygiene'
            WHEN operator_name = 'New York City Health and Hospital Corporation' THEN 'NYC Health and Hospitals Corporation'
            WHEN ownership_type = 'State' THEN 'NYS Department of Health'
            ELSE operator_name
        END
    ) as opname,
    (
        CASE
            WHEN operator_name = 'City of New York' THEN 'NYCDOHMH'
            WHEN operator_name = 'New York City Health and Hospitals Corporation' THEN 'NYCHHC'
            WHEN ownership_type = 'State' THEN 'NYSDOH'
            ELSE 'Non-public'
        END
    ) as opabbrev,
    'NYSDOH' as overabbrev,
    NULL as capacity,
    NULL as captype,
    wkt::geometry as wkb_geometry,
    geo_1b,
    NULL as geo_bl,
    NULL as geo_bn INTO _nysdoh_healthfacilities
FROM nysdoh_healthfacilities
WHERE description NOT LIKE '%Residential%' AND description NOT LIKE 'Licensed Home Care Services Agency';

CALL append_to_facdb_base('_nysdoh_healthfacilities');
