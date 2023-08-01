DROP TABLE IF EXISTS _nysdec_lands;

SELECT
    uid,
    source,
    initcap(facility) as facname,
    NULL as addressnum,
    NULL as streetname,
    NULL as address,
    NULL as city,
    NULL as zipcode,
    county as boro,
    NULL as borocode,
    NULL as bin,
    NULL as bbl,
    (CASE
		WHEN category = 'NRA' THEN 'Natural Resource Area'
		ELSE initcap(category)
	END) as factype,
    'Preserves and Conservation Areas' as facsubgrp,
    'NYS Department of Environmental Conservation' as opname,
    'NYSDEC' as opabbrev,
    'NYSDEC' as overabbrev,
    NULL as capacity,
    NULL as captype,
    ST_AsBinary(ST_AsText(wkt)) as wkb_geometry,
    NULL as geo_1b,
    NULL as geo_bl,
    NULL as geo_bn
INTO _nysdec_lands
FROM nysdec_lands;

CALL append_to_facdb_base('_nysdec_lands');
