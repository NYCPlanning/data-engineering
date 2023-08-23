DROP TABLE IF EXISTS _usdot_airports;
SELECT
    uid,
    source,
    name as facname,
    parsed_hnum as addressnum,
    parsed_sname as streetname,
    cleaned_address as address,
    city,
    zipcode as zipcode,
    county as boro,
    NULL as borocode,
    NULL as bin,
    NULL as bbl,
    facility_type as factype,
    'Airports and Heliports' as facsubgrp,
    (CASE
        WHEN ownership= 'PR' THEN name
        ELSE 'Public'
    END) as opname,
    (CASE
        WHEN ownership = 'PR' THEN 'Non-public'
        ELSE 'Public'
    END) as opabbrev,
    'USDOT' as overabbrev,
    NULL as capacity,
    NULL as captype,
    --- wkt::geometry as wkb_geometry,
    NULL as wkb_geometry,
    geo_1b,
    NULL geo_bl,
    NULL geo_bn
INTO _usdot_airports
FROM usdot_airports;

CALL append_to_facdb_base('_usdot_airports');
