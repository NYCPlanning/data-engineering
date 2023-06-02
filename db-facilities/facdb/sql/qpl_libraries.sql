DROP TABLE IF EXISTS _qpl_libraries;
SELECT
    uid,
    source,
    name as facname,
    parsed_hnum as addressnum,
    parsed_sname as streetname,
    address,
    city,
    postcode as zipcode,
    borough as boro,
    NULL as borocode,
    bin,
    bbl,
    'Public Library' as factype,
    'Public Libraries' as facsubgrp,
    'Queens Public Library' as opname,
    'Public' as opabbrev,
    'QPL' as overabbrev,
    NULL as capacity,
    NULL as captype,
    wkt::geometry as wkb_geometry,
    geo_1b,
    geo_bl,
    geo_bn
INTO _qpl_libraries
FROM qpl_libraries;

CALL append_to_facdb_base('_qpl_libraries');
