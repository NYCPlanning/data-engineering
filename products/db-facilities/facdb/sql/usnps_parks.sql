DROP TABLE IF EXISTS _usnps_parks;
SELECT
    uid,
    source,
    unit_name as facname,
    NULL as addressnum,
    NULL as streetname,
    NULL as address,
    NULL as city,
    NULL as zipcode,
    NULL as boro,
    NULL as borocode,
    NULL as bin,
    NULL as bbl,
    unit_name as factype,
    'Historical Sites' as facsubgrp,
    'National Park Service' as opname,
    'USNPS' as opabbrev,
    'USNPS' as overabbrev,
    NULL as capacity,
    NULL as captype,
    wkt::geometry as wkb_geometry,
    NULL geo_1b,
    NULL geo_bl,
    NULL geo_bn
INTO _usnps_parks
FROM usnps_parks;

CALL append_to_facdb_base('_usnps_parks');
