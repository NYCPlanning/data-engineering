DROP TABLE IF EXISTS _nysdec_lands;

SELECT
    uid,
    source,
    NULL AS addressnum,
    NULL AS streetname,
    NULL AS address,
    NULL AS city,
    NULL AS zipcode,
    county AS boro,
    NULL AS borocode,
    NULL AS bin,
    NULL AS bbl,
    'Preserves and Conservation Areas' AS facsubgrp,
    'NYS Department of Environmental Conservation' AS opname,
    'NYSDEC' AS opabbrev,
    'NYSDEC' AS overabbrev,
    NULL AS capacity,
    NULL AS captype,
    NULL AS geo_1b,
    NULL AS geo_bl,
    NULL AS geo_bn,
    initcap(facility) AS facname,
    (CASE
        WHEN category = 'NRA' THEN 'Natural Resource Area'
        ELSE initcap(category)
    END) AS factype,
    st_asbinary(st_astext(wkt)) AS wkb_geometry
INTO _nysdec_lands
FROM nysdec_lands;

CALL append_to_facdb_base('_nysdec_lands');
