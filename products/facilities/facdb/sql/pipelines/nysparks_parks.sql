DROP TABLE IF EXISTS _nysparks_parks;
SELECT
    uid,
    source,
    name AS facname,
    NULL AS addressnum,
    NULL AS streetname,
    NULL AS address,
    NULL AS city,
    NULL AS zipcode,
    county AS boro,
    NULL AS borocode,
    NULL AS bin,
    NULL AS bbl,
    category AS factype,
    (
        CASE
            WHEN category LIKE '%Preserve%'
                THEN 'Preserves and Conservation Areas'
            ELSE 'Parks'
        END
    ) AS facsubgrp,
    'The New York State Office of Parks, Recreation and Historic Preservation' AS opname,
    'NYSOPRHP' AS opabbrev,
    'NYSOPRHP' AS overabbrev,
    NULL AS capacity,
    NULL AS captype,
    geometry AS wkb_geometry,
    NULL AS geo_1b,
    NULL AS geo_bl,
    NULL AS geo_bn
INTO _nysparks_parks
FROM nysparks_parks;

CALL append_to_facdb_base('_nysparks_parks');
