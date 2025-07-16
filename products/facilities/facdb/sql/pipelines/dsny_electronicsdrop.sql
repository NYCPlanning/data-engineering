DROP TABLE IF EXISTS _dsny_electronicsdrop;
SELECT
    uid,
    source,
    concat(dropoff_sitename, ' Electronics Drop-off Site') AS facname,
    NULL AS addressnum,
    NULL AS streetname,
    address,
    NULL AS city,
    zipcode,
    left(censustract, 1) AS boro,
    NULL AS borocode,
    bin,
    bbl,
    'Electronics' AS factype,
    'DSNY Drop-off Facility' AS facsubgrp,
    dropoff_sitename AS opname,
    NULL AS opabbrev,
    'NYCDSNY' AS overabbrev,
    NULL AS capacity,
    NULL AS captype,
    geom AS wkb_geometry,
    geo_1b,
    geo_bl,
    geo_bn
INTO _dsny_electronicsdrop
FROM dsny_electronicsdrop;

CALL append_to_facdb_base('_dsny_electronicsdrop');
