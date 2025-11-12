DROP TABLE IF EXISTS _dycd_service_sites;

SELECT
    uid,
    source,
    program_site_name AS facname,
    parsed_hnum AS addressnum,
    parsed_sname AS streetname,
    address,
    city,
    zipcode,
    borough AS boro,
    NULL AS borocode,
    bin,
    bbl,
    service_category AS factype,
    (CASE
        WHEN
            program_type ~* 'Beacon'
            OR program_type ~* 'High-School Aged Youth'
            OR program_type ~* 'Middle School Youth'
            OR program_type ~* 'Teen Action Program'
            OR program_type ~* 'After-School Programs'
            THEN 'After-School Programs'
        WHEN service_category ~* 'Immigrant Support Services'
            THEN 'Immigrant Services'
        ELSE 'Youth Centers, Literacy Programs, and Job Training Services'
    END) AS facsubgrp,
    provider AS opname,
    NULL AS opabbrev,
    'NYCDYCD' AS overabbrev,
    NULL AS capacity,
    NULL AS captype,
    ST_POINT(longitude::double precision, latitude::double precision) AS wkb_geometry,
    geo_1b,
    geo_bl,
    geo_bn
INTO _dycd_service_sites
FROM dycd_service_sites
WHERE fiscalyear = (SELECT MAX(d.fiscalyear) FROM dycd_service_sites AS d);

CALL APPEND_TO_FACDB_BASE('_dycd_service_sites');
