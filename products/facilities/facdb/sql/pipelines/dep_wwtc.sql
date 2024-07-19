DROP TABLE IF EXISTS _dep_wwtc;
SELECT
    uid,
    source,
    name AS facname,
    house_number AS addressnum,
    street_name AS streetname,
    address,
    NULL AS city,
    zipcode,
    NULL AS boro,
    NULL AS borocode,
    NULL AS bin,
    NULL AS bbl,
    'Wastewater Treatment Plant' AS factype,
    'Wastewater and Pollution Control' AS facsubgrp,
    'NYC Department of Environmental Protection' AS opname,
    'NYCDEP' AS opabbrev,
    'NYCDEP' AS overabbrev,
    NULL AS capacity,
    NULL AS captype,
    NULL AS wkb_geometry,
    geo_1b,
    NULL AS geo_bl,
    NULL AS geo_bn
INTO _dep_wwtc
FROM dep_wwtc;

CALL append_to_facdb_base('_dep_wwtc');
