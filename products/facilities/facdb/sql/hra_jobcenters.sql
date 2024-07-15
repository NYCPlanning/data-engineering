DROP TABLE IF EXISTS _hra_jobcenters;

SELECT
    uid,
    source,
    facility_name AS facname,
    parsed_hnum AS addressnum,
    parsed_sname AS streetname,
    street_address AS address,
    city,
    post_code AS zipcode,
    borough AS boro,
    LEFT(bin::text, 1) AS borocode,
    bin,
    bbl,
    'Jobs and Service Center' AS factype,
    'Workforce Development' AS facsubgrp,
    'NYC Human Resources Administration' AS opname,
    'NYCHRA' AS opabbrev,
    'NYCHRA' AS overabbrev,
    NULL AS capacity,
    NULL AS captype,
    wkt::geometry AS wkb_geometry,
    geo_1b,
    geo_bl,
    geo_bn
INTO _hra_jobcenters
FROM hra_jobcenters;

CALL APPEND_TO_FACDB_BASE('_hra_jobcenters');
