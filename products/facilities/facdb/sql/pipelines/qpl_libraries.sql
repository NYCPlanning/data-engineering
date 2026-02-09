DROP TABLE IF EXISTS _qpl_libraries;
SELECT
    uid,
    source,
    name AS facname,
    parsed_hnum AS addressnum,
    parsed_sname AS streetname,
    address,
    city,
    zip_code AS zipcode,
    'Queens' AS boro,
    NULL AS borocode,
    NULL AS bin,
    NULL AS bbl,
    'Public Library' AS factype,
    'Public Libraries' AS facsubgrp,
    'Queens Public Library' AS opname,
    'Public' AS opabbrev,
    'QPL' AS overabbrev,
    NULL AS capacity,
    NULL AS captype,
    geom AS wkb_geometry,
    geo_1b,
    NULL AS geo_bl,
    NULL AS geo_bn
INTO _qpl_libraries
FROM qpl_libraries;

CALL append_to_facdb_base('_qpl_libraries');
