DROP TABLE IF EXISTS _dot_ferryterminals;

SELECT
    uid,
    source,
    site as facname,
    parsed_hnum as addressnum,
    parsed_sname as streetname,
    address,
    NULL as city,
    NULL as zipcode,
    boroname as boro,
    borocode,
    NULL as bin,
    bbl,
    (CASE
        WHEN UPPER(site) LIKE '%TERMINAL%' THEN 'Ferry Terminal'
		WHEN UPPER(site) LIKE '%LANDING%' THEN 'Ferry Landing'
	END) as factype,
    'Ports and Ferry Landings' as facsubgrp,
    'NYC Department of Transportation' as opname,
    'NYCDOT' as opabbrev,
    'NYCDOT' as overabbrev,
    NULL as capacity,
    NULL as captype,
    wkt::geometry as wkb_geometry,
    geo_1b,
    geo_bl,
    NULL as geo_bn
INTO _dot_ferryterminals
FROM dot_ferryterminals;

CALL append_to_facdb_base('_dot_ferryterminals');
