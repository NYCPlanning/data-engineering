DROP TABLE IF EXISTS _nysomh_mentalhealth;
SELECT
    uid,
    source,
    program_name AS facname,
    parsed_hnum AS addressnum,
    parsed_sname AS streetname,
    program_address_1 AS address,
    program_city AS city,
    program_zip AS zipcode,
    program_county AS boro,
    NULL AS borocode,
    NULL AS bin,
    NULL AS bbl,
    'Mental Health' AS facsubgrp,
    'NYS Office of Mental Health' AS overagency,
    'NYSOMH' AS overabbrev,
    NULL AS capacity,
    NULL AS captype,
    geo_1b,
    NULL AS geo_bl,
    NULL AS geo_bn,
    program_category_description || ' Mental Health' AS factype,
    (CASE
        WHEN program_type_description LIKE '%State%' THEN 'NYS Office of Mental Health'
        WHEN sponsor_name LIKE '%Health and Hospitals Corporation%' THEN 'NYC Health and Hospitals Corporation'
        ELSE agency_name
    END) AS opname,
    (CASE
        WHEN program_type_description LIKE '%State%' THEN 'NYSOMH'
        WHEN sponsor_name LIKE '%Health and Hospitals Corporation%' THEN 'NYCHHC'
        ELSE 'Non-public'
    END) AS opabbrev,
    ST_ASBINARY(ST_ASTEXT(georeference)) AS wkb_geometry
INTO _nysomh_mentalhealth
FROM nysomh_mentalhealth;

CALL APPEND_TO_FACDB_BASE('_nysomh_mentalhealth');
