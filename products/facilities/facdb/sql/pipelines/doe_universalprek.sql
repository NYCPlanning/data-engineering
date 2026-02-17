DROP TABLE IF EXISTS _doe_universalprek;
SELECT
    uid,
    source,
    program_name AS facname,
    parsed_hnum AS addressnum,
    parsed_sname AS streetname,
    address,
    NULL AS city,
    zip_code AS zipcode,
    borough,
    NULL AS borocode,
    NULL AS bin,
    NULL AS bbl,
    (CASE
        WHEN site_type = 'DOE' OR site_type = 'Public School' THEN 'DOE Universal Pre-K'
        WHEN site_type = 'CHARTER' OR site_type = 'Charter' THEN 'DOE Universal Pre-K - Charter'
        WHEN site_type = 'NYCEEC' OR site_type = 'CBO' THEN 'Early Education Program'
        WHEN site_type = 'PKC' THEN 'Pre-K Center'
    END) AS factype,
    'DOE Universal Pre-Kindergarten' AS facsubgrp,
    (CASE
        WHEN site_type = 'DOE' OR site_type = 'Public School' THEN 'NYC Department of Education'
        WHEN site_type IN ('CHARTER', 'Charter', 'NYCEEC', 'CBO') THEN program_name
        ELSE 'Unknown'
    END) AS opname,
    (CASE
        WHEN site_type = 'DOE' OR site_type = 'Public School' THEN 'NYCDOE'
        WHEN site_type = 'Charter' THEN 'Charter'
        WHEN site_type = 'NYCEEC' OR site_type = 'CBO' THEN 'Non-public'
        ELSE 'Unknown'
    END) AS opabbrev,
    'NYCDOE' AS overabbrev,
    NULL AS capacity,
    NULL AS captype,
    NULL AS wkb_geometry,
    geo_1b,
    NULL AS geo_bl,
    NULL AS geo_bn
INTO _doe_universalprek
FROM doe_universalprek;

CALL append_to_facdb_base('_doe_universalprek');
