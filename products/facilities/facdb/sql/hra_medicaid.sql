DROP TABLE IF EXISTS _hra_medicaid;

SELECT
    uid,
    source,
    name_of__medicaid_office as facname,
    parsed_hnum as addressnum,
    parsed_sname as streetname,
    office_address as address,
    NULL as city,
    postcode as zipcode,
    borough as boro,
    LEFT(bin::text, 1) as borocode,
    bin,
    bbl,
    'Medicaid Office' as factype,
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
INTO _hra_medicaid
FROM hra_medicaid;

CALL append_to_facdb_base('_hra_medicaid');
