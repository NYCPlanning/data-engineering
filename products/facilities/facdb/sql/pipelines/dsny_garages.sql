DROP TABLE IF EXISTS _dsny_garages;
SELECT
    uid,
    source,
    concat(name, ' ', type) AS facname,
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

CALL append_to_facdb_base('_dsny_garages');
