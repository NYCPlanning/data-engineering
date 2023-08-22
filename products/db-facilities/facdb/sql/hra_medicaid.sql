DROP TABLE IF EXISTS _hra_medicaid;

SELECT
    uid,
    source,
    name_of__medicaid_office AS facname,
    parsed_hnum AS addressnum,
    parsed_sname AS streetname,
    office_address AS address,
    NULL AS city,
    postcode AS zipcode,
    borough AS boro,
    bin,
    bbl,
    'Medicaid Office' AS factype,
    'Financial Assistance and Social Services' AS facsubgrp,
    'NYC Human Resources Administration' AS opname,
    'NYCHRA' AS opabbrev,
    'NYCHRA' AS overabbrev,
    NULL AS capacity,
    NULL AS captype,
    wkt::geometry AS wkb_geometry,
    geo_1b,
    geo_bl,
    geo_bn,
    LEFT(bin::text, 1) AS borocode
INTO _hra_medicaid
FROM hra_medicaid;

CALL APPEND_TO_FACDB_BASE('_hra_medicaid');
