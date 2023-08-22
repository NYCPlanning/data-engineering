DROP TABLE IF EXISTS _sbs_workforce1;
SELECT
    uid,
    source,
    name AS facname,
    number AS addressnum,
    street AS streetname,
    city,
    postcode AS zipcode,
    borough AS boro,
    NULL AS borocode,
    bin,
    bbl,
    location_type AS factype,
    'Workforce Development' AS facsubgrp,
    'NYC Department of Small Business Services' AS opname,
    'NYCSBS' AS opabbrev,
    'NYCSBS' AS overabbrev,
    NULL AS capacity,
    NULL AS captype,
    wkt::geometry AS wkb_geometry,
    geo_1b,
    geo_bl,
    geo_bn,
    number || ' ' || street AS address
INTO _sbs_workforce1
FROM sbs_workforce1;

CALL append_to_facdb_base('_sbs_workforce1');
