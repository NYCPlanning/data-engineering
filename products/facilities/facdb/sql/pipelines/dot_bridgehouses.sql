DROP TABLE IF EXISTS _dot_bridgehouses;

SELECT
    uid,
    source,
    site AS facname,
    parsed_hnum AS addressnum,
    parsed_sname AS streetname,
    raw_address AS address,
    NULL AS city,
    NULL AS zipcode,
    boroname AS boro,
    borocode,
    NULL AS bin,
    NULL AS bbl,
    'Bridge House' AS factype,
    'Other Transportation' AS facsubgrp,
    'NYC Department of Transportation' AS opname,
    'NYCDOT' AS opabbrev,
    'NYCDOT' AS overabbrev,
    NULL AS capacity,
    NULL AS captype,
    geometry AS wkb_geometry,
    geo_1b,
    NULL AS geo_bl,
    NULL AS geo_bn
INTO _dot_bridgehouses
FROM dot_bridgehouses;

CALL append_to_facdb_base('_dot_bridgehouses');
