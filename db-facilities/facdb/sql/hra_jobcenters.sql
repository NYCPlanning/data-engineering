DROP TABLE IF EXISTS _hra_jobcenters;

SELECT
    uid,
    source,
    facility_name as facname,
    parsed_hnum as addressnum,
    parsed_sname as streetname,
    street_address as address,
    city,
    post_code as zipcode,
    borough as boro,
    LEFT(bin::text, 1) as borocode,
    bin,
    bbl,
    'Jobs and Service Center' as factype,
    'Workforce Development' as facsubgrp,
    'NYC Human Resources Administration' as opname,
    'NYCHRA' as opabbrev,
    'NYCHRA' as overabbrev,
    NULL as capacity,
    NULL as captype,
    wkt::geometry as wkb_geometry,
    geo_1b,
    geo_bl,
    geo_bn
INTO _hra_jobcenters
FROM hra_jobcenters;

CALL append_to_facdb_base('_hra_jobcenters');
