DROP TABLE IF EXISTS _doe_busroutesgarages;
WITH
capacity AS (
    SELECT
        vendor_name,
        garage__street_address,
        COUNT(DISTINCT route_number) AS route_counts
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
    a.parsed_hnum AS addressnum,
    a.parsed_sname AS streetname,
    a.garage__street_address AS address,
    a.garage_city AS city,
    a.garage_zip AS zipcode,
    a.garage_city AS boro,
    NULL AS borocode,
    NULL AS bin,
    NULL AS bbl,
    'School Bus Depot' AS factype,
    'Bus Depots and Terminals' AS facsubgrp,
    'Non-public' AS opabbrev,
    'NYCDOE' AS overabbrev,
    b.route_counts AS capacity,
    'routes' AS captype,
    a.wkt::geometry AS wkb_geometry,
    a.geo_1b,
    NULL AS geo_bl,
    NULL AS geo_bn,
    INITCAP(a.vendor_name) AS facname,
    INITCAP(a.vendor_name) AS opname
INTO _doe_busroutesgarages
FROM doe_busroutesgarages AS a
LEFT JOIN capacity AS b
    ON
        a.vendor_name = b.vendor_name
        AND a.garage__street_address = b.garage__street_address
WHERE a.uid IN (SELECT min_uid FROM min_records);

CALL APPEND_TO_FACDB_BASE('_doe_busroutesgarages');
