DROP TABLE IF EXISTS _hra_medicaid;

SELECT
    uid,
    source,
    facility_name AS facname,
    parsed_hnum AS addressnum,
    parsed_sname AS streetname,
    street_address AS address,
    NULL AS city,
    postcode AS zipcode,
    borough AS boro,
    left(bin::text, 1) AS borocode,
    bin,
    bbl,
    'Medicaid Office' AS factype,
    'Financial Assistance and Social Services' AS facsubgrp,
    'NYC Human Resources Administration' AS opname,
    'NYCHRA' AS opabbrev,
    'NYCHRA' AS overabbrev,
    NULL AS capacity,
    NULL AS captype,
    geom AS wkb_geometry,
    geo_1b,
    geo_bl,
    geo_bn
INTO _hra_medicaid
FROM hra_medicaid;

CALL append_to_facdb_base('_hra_medicaid');
