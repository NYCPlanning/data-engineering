DROP TABLE IF EXISTS _dot_ferryterminals;

SELECT
    uid,
    source,
    site AS facname,
    parsed_hnum AS addressnum,
    parsed_sname AS streetname,
    address,
    NULL AS city,
    NULL AS zipcode,
    boroname AS boro,
    borocode,
    NULL AS bin,
    bbl,
    (CASE
        WHEN upper(site) LIKE '%TERMINAL%' THEN 'Ferry Terminal'
        WHEN upper(site) LIKE '%LANDING%' THEN 'Ferry Landing'
    END) AS factype,
    'Ports and Ferry Landings' AS facsubgrp,
    'NYC Department of Transportation' AS opname,
    'NYCDOT' AS opabbrev,
    'NYCDOT' AS overabbrev,
    NULL AS capacity,
    NULL AS captype,
    geometry AS wkb_geometry,
    geo_1b,
    geo_bl,
    NULL AS geo_bn
INTO _dot_ferryterminals
FROM dot_ferryterminals;

CALL append_to_facdb_base('_dot_ferryterminals');
