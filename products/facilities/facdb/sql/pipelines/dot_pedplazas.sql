DROP TABLE IF EXISTS _dot_pedplazas;

SELECT
    uid,
    source,
    plazaname AS facname,
    NULL AS addressnum,
    NULL AS streetname,
    fromstreet || ' and ' || tostreet AS address,
    NULL AS city,
    NULL AS zipcode,
    boroname AS boro,
    borocode,
    NULL AS bin,
    NULL AS bbl,
    'Pedestrian Plaza' AS factype,
    'Streetscapes, Plazas, and Malls' AS facsubgrp,
    'NYC Department of Transportation' AS opname,
    'NYCDOT' AS opabbrev,
    'NYCDOT' AS overabbrev,
    NULL AS capacity,
    NULL AS captype,
    geom AS wkb_geometry,
    NULL AS geo_1b,
    NULL AS geo_bl,
    NULL AS geo_bn
INTO _dot_pedplazas
FROM dot_pedplazas;

CALL append_to_facdb_base('_dot_pedplazas');
