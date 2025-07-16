DROP TABLE IF EXISTS _dsny_leafdrop;
SELECT
    uid,
    source,
    concat(site_name, ' Leaf Drop-off Site') AS facname,
    parsed_hnum AS addressnum,
    parsed_sname AS streetname,
    address,
    NULL AS city,
    zipcode,
    borough AS boro,
    NULL AS borocode,
    bin,
    bbl,
    'Leaf' AS factype,
    'DSNY Drop-off Facility' AS facsubgrp,
    site_managed_by AS opname,
    NULL AS opabbrev,
    'NYCDSNY' AS overabbrev,
    NULL AS capacity,
    NULL AS captype,
    geom AS wkb_geometry,
    geo_1b,
    geo_bl,
    geo_bn
INTO _dsny_leafdrop
FROM dsny_leafdrop;

CALL append_to_facdb_base('_dsny_leafdrop');
