DROP TABLE IF EXISTS _doe_universalprek;
SELECT
    uid,
    source,
    name as facname,
    parsed_hnum as addressnum,
    parsed_sname as streetname,
    address,
    NULL as city,
    zip as zipcode,
    boro,
    NULL as borocode,
    NULL as bin,
    NULL as bbl,
    (CASE
        WHEN type = 'DOE' THEN 'DOE Universal Pre-K'
        WHEN type = 'CHARTER' OR type = 'Charter' THEN 'DOE Universal Pre-K - Charter'
        WHEN type = 'NYCEEC' THEN 'Early Education Program'
		WHEN type = 'PKC' THEN 'Pre-K Center'
	END) as factype,
    'DOE Universal Pre-Kindergarten' as facsubgrp,
    (CASE
        WHEN type = 'DOE' THEN 'NYC Department of Education'
        WHEN type = 'CHARTER' OR type = 'NYCEEC' THEN name
        ELSE 'Unknown'
	END) as opname,
    (CASE
        WHEN type = 'DOE' THEN 'NYCDOE'
        WHEN type = 'CHARTER' THEN 'Charter'
        WHEN type = 'NYCEEC' THEN 'Non-public'
        ELSE 'Unknown'
	END) as opabbrev,
    'NYCDOE' as overabbrev,
    NULL as capacity,
    NULL as captype,
    NULL as wkb_geometry,
    geo_1b,
    NULL as geo_bl,
    NULL as geo_bn
INTO _doe_universalprek
FROM doe_universalprek;

CALL append_to_facdb_base('_doe_universalprek');
