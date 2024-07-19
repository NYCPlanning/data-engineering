DROP TABLE IF EXISTS _nycdoc_corrections;

SELECT
    uid,
    source,
    name AS facname,
    house_number AS addressnum,
    street_name AS streetname,
    address1 AS address,
    NULL AS city,
    zipcode,
    NULL AS boro,
    NULL AS borocode,
    NULL AS bin,
    NULL AS bbl,
    'Correctional Facility' AS factype,
    'Detention and Correctional' AS facsubgrp,
    'NYC Department of Correction' AS opname,
    'NYCDOC' AS opabbrev,
    'NYCDOC' AS overabbrev,
    NULL AS capacity,
    NULL AS captype,
    NULL AS wkb_geometry,
    geo_1b,
    NULL AS geo_bl,
    NULL AS geo_bn
INTO _nycdoc_corrections
FROM nycdoc_corrections;

CALL append_to_facdb_base('_nycdoc_corrections');
