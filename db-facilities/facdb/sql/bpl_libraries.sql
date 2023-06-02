DROP TABLE IF EXISTS _bpl_libraries;
SELECT
    uid,
    source,
    title as facname,
    parsed_hnum as addressnum,
    parsed_sname as streetname,
    address,
    NULL as city,
    zipcode,
    borough as boro,
    NULL as borocode,
    NULL as bin,
    NULL as bbl,
    'Public Library' as factype,
    'Public Libraries' as facsubgrp,
    'Brooklyn Public Library' as opname,
    'BPL' as opabbrev,
    'BPL' as overabbrev,
    NULL as capacity,
    NULL as captype,
    wkt::geometry as wkb_geometry,
    geo_1b,
    NULL as geo_bl,
    NULL as geo_bn
INTO _bpl_libraries
FROM bpl_libraries;

CALL append_to_facdb_base('_bpl_libraries');
