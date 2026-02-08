DROP TABLE IF EXISTS _foodbankny_foodbanks;

SELECT
    uid,
    source,
    agency AS facname,
    parsed_hnum AS addressnum,
    parsed_sname AS streetname,
    address_1 AS address,
    NULL AS city,
    address_4 AS zipcode,
    address_3 AS boro,
    NULL AS borocode,
    NULL AS bin,
    NULL AS bbl,
    (CASE
        WHEN program_type ~* 'pantry' THEN 'Food Pantry'
        WHEN program_type ~* 'Soup Kitchen' THEN 'Soup Kitchen'
    END) AS factype,
    'Soup Kitchens and Food Pantries' AS facsubgrp,
    agency AS opname,
    'Non-public' AS opabbrev,
    'Non-public' AS overabbrev,
    NULL AS capacity,
    NULL AS captype,
    geom AS wkb_geometry,
    geo_1b,
    NULL AS geo_bl,
    NULL AS geo_bn
INTO _foodbankny_foodbanks
FROM foodbankny_foodbanks
WHERE (
    program_type !~* 'senior'
    AND program_type !~* 'home'
);

CALL append_to_facdb_base('_foodbankny_foodbanks');
