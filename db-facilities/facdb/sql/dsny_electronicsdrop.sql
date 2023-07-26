DROP TABLE IF EXISTS _dsny_electronicsdrop;
SELECT uid,
    source,
    CONCAT(dropoff_sitename, ' Electronics Drop-off Site') as facname,
    NULL as addressnum,
    NULL as streetname,
    address,
    NULL as city,
    zipcode,
    borough as boro,
    NULL as borocode,
    bin,
    bbl,
    'Electronics' as factype,
    'DSNY Drop-off Facility' as facsubgrp,
    dropoff_sitename as opname,
    NULL as opabbrev,
    'NYCDSNY' as overabbrev,
    NULL as capacity,
    NULL as captype,
    wkt::geometry as wkb_geometry,
    geo_1b,
    geo_bl,
    geo_bn
INTO _dsny_electronicsdrop
FROM dsny_electronicsdrop;

CALL append_to_facdb_base('_dsny_electronicsdrop');
