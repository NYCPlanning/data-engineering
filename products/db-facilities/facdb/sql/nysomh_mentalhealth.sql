DROP TABLE IF EXISTS _nysomh_mentalhealth;
SELECT
    uid,
    source,
    program_name as facname,
    parsed_hnum as addressnum,
    parsed_sname as streetname,
    program_address_1 as address,
    program_city as city,
    program_zip as zipcode,
    program_county as boro,
    NULL as borocode,
    NULL as bin,
    NULL as bbl,
    program_category_description || ' Mental Health' as factype,
    'Mental Health' as facsubgrp,
    (CASE
        WHEN program_type_description LIKE '%State%' THEN 'NYS Office of Mental Health'
        WHEN sponsor_name LIKE '%Health and Hospitals Corporation%' THEN 'NYC Health and Hospitals Corporation'
        ELSE agency_Name
    END) as opname,
    'NYS Office of Mental Health' as overagency,
    (CASE
        WHEN program_type_description LIKE '%State%' THEN 'NYSOMH'
        WHEN sponsor_name LIKE '%Health and Hospitals Corporation%' THEN 'NYCHHC'
        ELSE 'Non-public'
    END) as opabbrev,
    'NYSOMH' as overabbrev,
    NULL as capacity,
    NULL as captype,
    ST_AsBinary(ST_AsText(georeference)) as wkb_geometry,
    geo_1b,
    NULL as geo_bl,
    NULL as geo_bn
INTO _nysomh_mentalhealth
FROM nysomh_mentalhealth;

CALL append_to_facdb_base('_nysomh_mentalhealth');
