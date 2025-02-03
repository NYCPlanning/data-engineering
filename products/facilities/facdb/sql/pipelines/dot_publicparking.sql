DROP TABLE IF EXISTS _dot_publicparking;

SELECT
    uid,
    source,
    site AS facname,
    parsed_hnum AS addressnum,
    parsed_sname AS streetname,
    address,
    NULL AS city,
    NULL AS zipcode,
    boroname AS boro,
    borocode,
    NULL AS bin,
    bbl,
    'Public Parking' AS factype,
    'Parking Lots and Garages' AS facsubgrp,
    'NYC Department of Transportation' AS opname,
    'NYCDOT' AS opabbrev,
    'NYCDOT' AS overabbrev,
    NULL AS capacity,
    NULL AS captype,
    geometry AS wkb_geometry,
    geo_1b,
    geo_bl,
    NULL AS geo_bn
INTO _dot_publicparking
FROM dot_publicparking;

CALL append_to_facdb_base('_dot_publicparking');
