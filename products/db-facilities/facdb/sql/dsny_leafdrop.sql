DROP TABLE IF EXISTS _dsny_leafdrop;
SELECT
    uid,
    source,
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
    wkt::geometry AS wkb_geometry,
    geo_1b,
    geo_bl,
    geo_bn,
    CONCAT(site_name, ' Leaf Drop-off Site') AS facname
INTO _dsny_leafdrop
FROM dsny_leafdrop;

CALL APPEND_TO_FACDB_BASE('_dsny_leafdrop');
