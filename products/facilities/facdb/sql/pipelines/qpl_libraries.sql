DROP TABLE IF EXISTS _qpl_libraries;
SELECT
    uid,
    source,
    name AS facname,
    parsed_hnum AS addressnum,
    parsed_sname AS streetname,
    address,
    city,
    postcode AS zipcode,
    borough AS boro,
    NULL AS borocode,
    bin,
    bbl,
    'Public Library' AS factype,
    'Public Libraries' AS facsubgrp,
    'Queens Public Library' AS opname,
    'Public' AS opabbrev,
    'QPL' AS overabbrev,
    NULL AS capacity,
    NULL AS captype,
    wkt::geometry AS wkb_geometry,
    geo_1b,
    geo_bl,
    geo_bn
INTO _qpl_libraries
FROM qpl_libraries;

CALL append_to_facdb_base('_qpl_libraries');
