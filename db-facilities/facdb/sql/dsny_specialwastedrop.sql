DROP TABLE IF EXISTS _dsny_specialwastedrop;
SELECT uid,
    source,
    name as facname,
    parsed_hnum as addressnum,
    parsed_sname as streetname,
    address,
    city,
    zip as zipcode,
    NULL as boro,
    boro as borocode,
    NULL as bin,
    NULL as bbl,
    'Special Waste' as factype,
    'DSNY Drop-off Facility' as facsubgrp,
    'NYC Department of Sanitation' as opname,
    'NYCDSNY' as opabbrev,
    'NYCDSNY' as overabbrev,
    NULL as capacity,
    NULL as captype,
    wkt::geometry as wkb_geometry,
    geo_1b,
    NULL as geo_bl,
    NULL as geo_bn
INTO _dsny_specialwastedrop
FROM dsny_specialwastedrop;

CALL append_to_facdb_base('_dsny_specialwastedrop');
