DROP TABLE IF EXISTS _nypl_libraries;

SELECT
    uid,
    source,
    name AS facname,
    parsed_hnum AS addressnum,
    parsed_sname AS streetname,
    address,
    NULL AS city,
    zipcode,
    locality AS boro,
    NULL AS borocode,
    NULL AS bin,
    NULL AS bbl,
    'Public Library' AS factype,
    'Public Libraries' AS facsubgrp,
    'New York Public Library' AS opname,
    'NYPL' AS opabbrev,
    'NYPL' AS overabbrev,
    NULL AS capacity,
    NULL AS captype,
    wkb_geometry::geometry,
    geo_1b,
    NULL AS geo_bl,
    NULL AS geo_bn
INTO _nypl_libraries
FROM nypl_libraries;

CALL APPEND_TO_FACDB_BASE('_nypl_libraries');
