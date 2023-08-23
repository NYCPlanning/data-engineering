DROP TABLE IF EXISTS _dsny_garages;
SELECT uid,
    source,
    CONCAT(name,' ',type) as facname,
    parsed_hnum as addressnum,
    parsed_sname as streetname,
    address,
    city,
    zip as zipcode,
    NULL as boro,
    boro as borocode,
    bin,
    bbl,
    'DSNY Garage' as factype,
    'Solid Waste Transfer and Carting' as facsubgrp,
    'NYC Department of Sanitation' as opname,
    'NYCDSNY' as opabbrev,
    'NYCDSNY' as overabbrev,
    NULL as capacity,
    NULL as captype,
    wkt::geometry as wkb_geometry,
    geo_1b,
    geo_bl,
    geo_bn
INTO _dsny_garages
FROM dsny_garages;

CALL append_to_facdb_base('_dsny_garages');
