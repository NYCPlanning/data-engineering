DROP TABLE IF EXISTS _hra_snapcenters;

SELECT
    uid,
    source,
    facility_name AS facname,
    parsed_hnum AS addressnum,
    parsed_sname AS streetname,
    street_address AS address,
    city,
    postcode AS zipcode,
    NULL AS boro,
    left(bin::text, 1) AS borocode,
    bin,
    bbl,
    'SNAP Center' AS factype,
    'Financial Assistance and Social Services' AS facsubgrp,
    'NYC Human Resources Administration' AS opname,
    'NYCHRA' AS opabbrev,
    'NYCHRA' AS overabbrev,
    NULL AS capacity,
    NULL AS captype,
    geom AS wkb_geometry,
    geo_1b,
    geo_bl,
    geo_bn
INTO _hra_snapcenters
FROM hra_snapcenters;

CALL append_to_facdb_base('_hra_snapcenters');
