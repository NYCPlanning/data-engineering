DROP TABLE IF EXISTS _doe_busroutesgarages;
WITH
capacity AS(
	SELECT vendor_name, garage__street_address, COUNT(DISTINCT(route_number)) AS route_counts
	FROM doe_busroutesgarages
	GROUP BY vendor_name, garage__street_address
),
min_records AS (
    SELECT MIN(uid) AS min_uid
	FROM doe_busroutesgarages
	GROUP BY vendor_name, garage__street_address
)
SELECT
    a.uid,
    a.source,
    initcap(a.vendor_name) as facname,
    a.parsed_hnum as addressnum,
    a.parsed_sname as streetname,
    a.garage__street_address as address,
    a.garage_city as city,
    a.garage_zip as zipcode,
    a.garage_city as boro,
    NULL as borocode,
    NULL as bin,
    NULL as bbl,
    'School Bus Depot' as factype,
    'Bus Depots and Terminals' as facsubgrp,
    initcap(a.vendor_name) as opname,
    'Non-public' as opabbrev,
    'NYCDOE' as overabbrev,
    b.route_counts as capacity,
    'routes' as captype,
    a.wkt::geometry as wkb_geometry,
    a.geo_1b,
    NULL as geo_bl,
    NULL as geo_bn
INTO _doe_busroutesgarages
FROM doe_busroutesgarages a
LEFT JOIN capacity b
ON a.vendor_name = b.vendor_name
AND a.garage__street_address = b.garage__street_address
WHERE a.uid IN (SELECT min_uid FROM min_records);

CALL append_to_facdb_base('_doe_busroutesgarages');
