DROP TABLE IF EXISTS _nysdoccs_corrections;

SELECT
    uid,
    source,
    facility_name AS facname,
    house_number AS addressnum,
    street_name AS streetname,
    address,
    NULL AS city,
    zipcode,
    county AS boro,
    NULL AS borocode,
    NULL AS bin,
    NULL AS bbl,
    'Correctional Facility' AS factype,
    'Detention and Correctional' AS facsubgrp,
    'NYS Department of Corrections and Community Supervision' AS opname,
    'NYSDOCCS' AS opabbrev,
    'NYSDOCCS' AS overabbrev,
    NULL AS capacity,
    NULL AS captype,
    NULL AS wkb_geometry,
    geo_1b,
    NULL AS geo_bl,
    NULL AS geo_bn
INTO _nysdoccs_corrections
FROM nysdoccs_corrections;

CALL append_to_facdb_base('_nysdoccs_corrections');
