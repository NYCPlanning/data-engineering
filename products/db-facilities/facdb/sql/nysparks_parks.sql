DROP TABLE IF EXISTS _nysparks_parks;
SELECT
    uid,
    source,
    name as facname,
    NULL as addressnum,
    NULL as streetname,
    NULL as address,
    NULL as city,
    NULL as zipcode,
    county as boro,
    NULL as borocode,
    NULL as bin,
    NULL as bbl,
    category as factype,
    (
        CASE
        WHEN category LIKE '%Preserve%'
        THEN 'Preserves and Conservation Areas'
        ELSE 'Parks'
	    END
    ) as facsubgrp,
    'The New York State Office of Parks, Recreation and Historic Preservation' as opname,
    'NYSOPRHP' as opabbrev,
    'NYSOPRHP' as overabbrev,
    NULL as capacity,
    NULL as captype,
    wkt::geometry as wkb_geometry,
    NULL geo_1b,
    NULL as geo_bl,
    NULL as geo_bn
INTO _nysparks_parks
FROM nysparks_parks;

CALL append_to_facdb_base('_nysparks_parks');
