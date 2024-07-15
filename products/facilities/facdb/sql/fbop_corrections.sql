DROP TABLE IF EXISTS _fbop_corrections;
SELECT
    uid,
    source,
    nametitle AS facname,
    parsed_hnum AS addressnum,
    parsed_sname AS streetname,
    address,
    city,
    zipcode,
    city AS boro,
    NULL AS borocode,
    NULL AS bin,
    NULL AS bbl,
    'Detention Center' AS factype,
    'Detention and Correctional' AS facsubgrp,
    'Federal Bureau of Prisons' AS opname,
    'FBOP' AS opabbrev,
    'FBOP' AS overabbrev,
    NULL AS capacity,
    NULL AS captype,
    NULL AS wkb_geometry,
    geo_1b,
    NULL AS geo_bl,
    NULL AS geo_bn
INTO _fbop_corrections
FROM fbop_corrections;

CALL append_to_facdb_base('_fbop_corrections');
