DROP TABLE IF EXISTS _dsny_garages;
SELECT
    uid,
    source,
    CONCAT(name, ' ', type) AS facname,
    parsed_hnum AS addressnum,
    parsed_sname AS streetname,
    address,
    city,
    zip AS zipcode,
    NULL AS boro,
    boro AS borocode,
    bin,
    bbl,
    'DSNY Garage' AS factype,
    'Solid Waste Transfer and Carting' AS facsubgrp,
    'NYC Department of Sanitation' AS opname,
    'NYCDSNY' AS opabbrev,
    'NYCDSNY' AS overabbrev,
    NULL AS capacity,
    NULL AS captype,
    wkb_geometry,
    geo_1b,
    geo_bl,
    geo_bn
INTO _dsny_garages
FROM dsny_garages;

CALL APPEND_TO_FACDB_BASE('_dsny_garages');
