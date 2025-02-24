DROP TABLE IF EXISTS _dsny_specialwastedrop;
SELECT
    uid,
    source,
    name AS facname,
    parsed_hnum AS addressnum,
    parsed_sname AS streetname,
    address,
    city,
    zip AS zipcode,
    NULL AS boro,
    boro AS borocode,
    NULL AS bin,
    NULL AS bbl,
    'Special Waste' AS factype,
    'DSNY Drop-off Facility' AS facsubgrp,
    'NYC Department of Sanitation' AS opname,
    'NYCDSNY' AS opabbrev,
    'NYCDSNY' AS overabbrev,
    NULL AS capacity,
    NULL AS captype,
    geom AS wkb_geometry,
    geo_1b,
    NULL AS geo_bl,
    NULL AS geo_bn
INTO _dsny_specialwastedrop
FROM dsny_specialwastedrop;

CALL append_to_facdb_base('_dsny_specialwastedrop');
