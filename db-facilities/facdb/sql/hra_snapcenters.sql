DROP TABLE IF EXISTS _hra_snapcenters;

SELECT
    uid,
    source,
    facility_name as facname,
    parsed_hnum as addressnum,
    parsed_sname as streetname,
    street_address as address,
    city,
    postcode as zipcode,
    NULL as boro,
    LEFT(bin::text, 1) as borocode,
    bin,
    bbl,
    'SNAP Center' as factype,
    'Financial Assistance and Social Services' as facsubgrp,
    'NYC Human Resources Administration' as opname,
    'NYCHRA' as opabbrev,
    'NYCHRA' as overabbrev,
    NULL as capacity,
    NULL as captype,
    wkt::geometry as wkb_geometry,
    geo_1b,
    geo_bl,
    geo_bn
INTO _hra_snapcenters
FROM hra_snapcenters;

CALL append_to_facdb_base('_hra_snapcenters');
