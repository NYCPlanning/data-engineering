DROP TABLE IF EXISTS _dsny_donatenycdirectory;
SELECT
    uid,
    source,
    concat(site, ' Textile Drop-off Site') AS facname,
    NULL AS addressnum,
    NULL AS streetname,
    address,
    NULL AS city,
    NULL AS zipcode,
    borough AS boro,
    NULL AS borocode,
    bin,
    bbl,
    'Textiles' AS factype,
    'DSNY Drop-off Facility' AS facsubgrp,
    site AS opname,
    NULL AS opabbrev,
    'NYCDSNY' AS overabbrev,
    NULL AS capacity,
    NULL AS captype,
    geom AS wkb_geometry,
    geo_1b,
    geo_bl,
    geo_bn
INTO _dsny_donatenycdirectory
FROM dsny_donatenycdirectory;

CALL append_to_facdb_base('_dsny_donatenycdirectory');
