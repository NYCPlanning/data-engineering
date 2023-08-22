DROP TABLE IF EXISTS _doe_universalprek;
SELECT
    uid,
    source,
    name AS facname,
    parsed_hnum AS addressnum,
    parsed_sname AS streetname,
    address,
    NULL AS city,
    zip AS zipcode,
    boro,
    NULL AS borocode,
    NULL AS bin,
    NULL AS bbl,
    'DOE Universal Pre-Kindergarten' AS facsubgrp,
    'NYCDOE' AS overabbrev,
    NULL AS capacity,
    NULL AS captype,
    NULL AS wkb_geometry,
    geo_1b,
    NULL AS geo_bl,
    NULL AS geo_bn,
    (CASE
        WHEN type = 'DOE' THEN 'DOE Universal Pre-K'
        WHEN type = 'CHARTER' OR type = 'Charter' THEN 'DOE Universal Pre-K - Charter'
        WHEN type = 'NYCEEC' THEN 'Early Education Program'
        WHEN type = 'PKC' THEN 'Pre-K Center'
    END) AS factype,
    (CASE
        WHEN type = 'DOE' THEN 'NYC Department of Education'
        WHEN type = 'CHARTER' OR type = 'NYCEEC' THEN name
        ELSE 'Unknown'
    END) AS opname,
    (CASE
        WHEN type = 'DOE' THEN 'NYCDOE'
        WHEN type = 'CHARTER' THEN 'Charter'
        WHEN type = 'NYCEEC' THEN 'Non-public'
        ELSE 'Unknown'
    END) AS opabbrev
INTO _doe_universalprek
FROM doe_universalprek;

CALL append_to_facdb_base('_doe_universalprek');
