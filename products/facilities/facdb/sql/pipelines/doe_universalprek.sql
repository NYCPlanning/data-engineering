DROP TABLE IF EXISTS _doe_universalprek;
SELECT
    uid,
    source,
    sitename AS facname,
    parsed_hnum AS addressnum,
    parsed_sname AS streetname,
    siteaddress AS address,
    NULL AS city,
    state AS zipcode,
    boro,
    NULL AS borocode,
    NULL AS bin,
    NULL AS bbl,
    (CASE
        WHEN type = 'DOE' OR type = 'Public School' THEN 'DOE Universal Pre-K'
        WHEN type = 'CHARTER' OR type = 'Charter' THEN 'DOE Universal Pre-K - Charter'
        WHEN type = 'NYCEEC' OR type = 'CBO' THEN 'Early Education Program'
        WHEN type = 'PKC' THEN 'Pre-K Center'
    END) AS factype,
    'DOE Universal Pre-Kindergarten' AS facsubgrp,
    (CASE
        WHEN type = 'DOE' OR type = 'Public School' THEN 'NYC Department of Education'
        WHEN type = 'CHARTER' OR type = 'Charter' OR type = 'NYCEEC' OR type = 'CBO' THEN sitename
        ELSE 'Unknown'
    END) AS opname,
    (CASE
        WHEN type = 'DOE' OR type = 'Public School' THEN 'NYCDOE'
        WHEN type = 'Charter' THEN 'Charter'
        WHEN type = 'NYCEEC' OR type = 'CBO' THEN 'Non-public'
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
