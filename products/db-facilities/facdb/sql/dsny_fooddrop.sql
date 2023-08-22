DROP TABLE IF EXISTS _dsny_fooddrop;
SELECT
    uid,
    source,
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
    wkt::geometry AS wkb_geometry,
    geo_1b,
    NULL AS geo_bl,
    NULL AS geo_bn,
    CONCAT(food_scrap_drop_off_site, ' Food Drop-off Site') AS facname
INTO _dsny_fooddrop
FROM dsny_fooddrop;
CALL APPEND_TO_FACDB_BASE('_dsny_fooddrop');
