DROP TABLE IF EXISTS _dsny_fooddrop;
SELECT
    uid,
    source,
    CONCAT(food_scrap_drop_off_site, ' Food Drop-off Site') AS facname,
    parsed_hnum AS addressnum,
    parsed_sname AS streetname,
    location AS address,
    NULL AS city,
    zip_code AS zipcode,
    borough AS boro,
    NULL AS borocode,
    NULL AS bin,
    NULL AS bbl,
    'Compost' AS factype,
    'DSNY Drop-off Facility' AS facsubgrp,
    hosted_by AS opname,
    NULL AS opabbrev,
    'NYCDSNY' AS overabbrev,
    NULL AS capacity,
    NULL AS captype,
    geom AS wkb_geometry,
    geo_1b,
    NULL AS geo_bl,
    NULL AS geo_bn
INTO _dsny_fooddrop
FROM dsny_fooddrop;
CALL APPEND_TO_FACDB_BASE('_dsny_fooddrop');
