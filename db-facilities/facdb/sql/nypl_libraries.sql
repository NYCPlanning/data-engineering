DROP TABLE IF EXISTS _nypl_libraries;

SELECT
    uid,
    source,
    name as facname,
    parsed_hnum as addressnum,
    parsed_sname as streetname,
    address,
    NULL as city,
    zipcode,
    locality as boro,
    NULL as borocode,
    NULL as bin,
    NULL as bbl,
    'Public Library' as factype,
    'Public Libraries' as facsubgrp,
    'New York Public Library' as opname,
    'NYPL' as opabbrev,
    'NYPL' as overabbrev,
    NULL as capacity,
    NULL as captype,
    ST_POINT(lon::double precision, lat::double precision) as wkb_geometry,
    geo_1b,
    NULL as geo_bl,
    NULL as geo_bn
INTO _nypl_libraries
FROM nypl_libraries;

CALL append_to_facdb_base('_nypl_libraries');
