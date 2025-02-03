DROP TABLE IF EXISTS _bpl_libraries;
SELECT
    uid,
    source,
    title AS facname,
    parsed_hnum AS addressnum,
    parsed_sname AS streetname,
    address,
    NULL AS city,
    zipcode,
    borough AS boro,
    NULL AS borocode,
    NULL AS bin,
    NULL AS bbl,
    'Public Library' AS factype,
    'Public Libraries' AS facsubgrp,
    'Brooklyn Public Library' AS opname,
    'BPL' AS opabbrev,
    'BPL' AS overabbrev,
    NULL AS capacity,
    NULL AS captype,
    geom AS wkb_geometry,
    geo_1b,
    NULL AS geo_bl,
    NULL AS geo_bn
INTO _bpl_libraries
FROM bpl_libraries;

CALL append_to_facdb_base('_bpl_libraries');
