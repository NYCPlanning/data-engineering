DROP TABLE IF EXISTS _usnps_parks;
SELECT
    uid,
    source,
    unit_name AS facname,
    NULL AS addressnum,
    NULL AS streetname,
    NULL AS address,
    NULL AS city,
    NULL AS zipcode,
    NULL AS boro,
    NULL AS borocode,
    NULL AS bin,
    NULL AS bbl,
    unit_name AS factype,
    'Historical Sites' AS facsubgrp,
    'National Park Service' AS opname,
    'USNPS' AS opabbrev,
    'USNPS' AS overabbrev,
    NULL AS capacity,
    NULL AS captype,
    wkt AS wkb_geometry,
    NULL AS geo_1b,
    NULL AS geo_bl,
    NULL AS geo_bn
INTO _usnps_parks
FROM usnps_parks;

CALL append_to_facdb_base('_usnps_parks');
