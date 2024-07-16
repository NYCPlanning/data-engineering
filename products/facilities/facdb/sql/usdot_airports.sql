DROP TABLE IF EXISTS _usdot_airports;
SELECT
    uid,
    source,
    name AS facname,
    parsed_hnum AS addressnum,
    parsed_sname AS streetname,
    cleaned_address AS address,
    city,
    zipcode,
    county AS boro,
    NULL AS borocode,
    NULL AS bin,
    NULL AS bbl,
    facility_type AS factype,
    'Airports and Heliports' AS facsubgrp,
    (CASE
        WHEN ownership = 'PR' THEN name
        ELSE 'Public'
    END) AS opname,
    (CASE
        WHEN ownership = 'PR' THEN 'Non-public'
        ELSE 'Public'
    END) AS opabbrev,
    'USDOT' AS overabbrev,
    NULL AS capacity,
    NULL AS captype,
    --- wkt::geometry as wkb_geometry,
    NULL AS wkb_geometry,
    geo_1b,
    NULL AS geo_bl,
    NULL AS geo_bn
INTO _usdot_airports
FROM usdot_airports;

CALL append_to_facdb_base('_usdot_airports');
