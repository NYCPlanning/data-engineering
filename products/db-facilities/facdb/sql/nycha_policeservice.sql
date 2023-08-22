DROP TABLE IF EXISTS _nycha_policeservice;

SELECT
    uid,
    source,
    parsed_hnum AS addressnum,
    parsed_sname AS streetname,
    address AS address,
    NULL AS city,
    zipcode,
    borough AS boro,
    NULL AS borocode,
    NULL AS bin,
    NULL AS bbl,
    'NYCHA Police Service' AS factype,
    'Police Services' AS facsubgrp,
    'NYC Housing Authority' AS opname,
    'NYCHA' AS opabbrev,
    'NYCHA' AS overabbrev,
    NULL AS capacity,
    NULL AS captype,
    wkt::geometry AS wkb_geometry,
    geo_1b,
    NULL AS geo_bl,
    NULL AS geo_bn,
    initcap(psa) AS facname
INTO _nycha_policeservice
FROM nycha_policeservice;

CALL append_to_facdb_base('_nycha_policeservice');
