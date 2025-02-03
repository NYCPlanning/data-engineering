DROP TABLE IF EXISTS _dsny_electronicsdrop;
SELECT
    uid,
    source,
    CONCAT(dropoff_sitename, ' Electronics Drop-off Site') AS facname,
    NULL AS addressnum,
    NULL AS streetname,
    address,
    NULL AS city,
    zipcode,
    LEFT(censustract, 1) AS boro,
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

CALL APPEND_TO_FACDB_BASE('_dsny_electronicsdrop');
