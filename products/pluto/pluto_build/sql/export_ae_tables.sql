CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

DROP TABLE IF EXISTS ae_tax_lot;
CREATE TABLE ae_tax_lot AS
SELECT
    e.bbl::char(10),
    e.borocode::char(1) AS borough_id,
    e.block::text,
    e.lot::text,
    e.address::text,
    e.landuse::char(2) AS land_use_id,
    st_transform(g.geom_2263, 4326) AS wgs84,
    g.geom_2263 AS li_ft
FROM export_pluto AS e
INNER JOIN pluto_geom AS g ON e.bbl = g.bbl::numeric
WHERE g.geom_2263 IS NOT NULL;

DROP TABLE IF EXISTS ae_zoning_district;
CREATE TABLE ae_zoning_district AS
SELECT
    gen_random_uuid() AS id,
    zonedist AS label,
    geom AS wgs84,
    st_transform(geom, 2263) AS li_ft
FROM dcp_zoningdistricts
WHERE
    zonedist NOT IN ('PARK', 'BALL FIELD', 'PUBLIC PLACE', 'PLAYGROUND', 'BPC', '')
    AND st_geometrytype(st_makevalid(geom)) = 'ST_MultiPolygon';


DROP TABLE IF EXISTS ae_zoning_district_zoning_district_class;
CREATE TABLE ae_zoning_district_zoning_district_class AS
WITH split_zones AS (
    SELECT
        id,
        unnest(string_to_array(label, '/')) AS individual_zoning_district
    FROM ae_zoning_district
)
SELECT
    id AS zoning_district_id,
    (regexp_match(individual_zoning_district, '^(\w\d+)(?:[^\d].*)?$'))[1] AS zoning_district_class_id
FROM split_zones;

DROP TABLE IF EXISTS ae_zoning_district_class_colors;
CREATE TABLE ae_zoning_district_class_colors AS
SELECT t.* FROM (
    VALUES
    ('BP', '#808080ff'),
    ('C1', '#ffa89cff'),
    ('C2', '#fd9a8fff'),
    ('C3', '#fa867cff'),
    ('C4', '#f76e67ff'),
    ('C5', '#f2544eff'),
    ('C6', '#ee3a36ff'),
    ('C7', '#ea2220ff'),
    ('C8', '#e50000ff'),
    ('M1', '#f3b3ffff'),
    ('M2', '#e187f3ff'),
    ('M3', '#cf5ce6ff'),
    ('PA', '#78D271ff'),
    ('R1', '#fff8a6ff'),
    ('R2', '#fff7a6ff'),
    ('R3', '#fff797ff'),
    ('R4', '#fff584ff'),
    ('R5', '#fff36cff'),
    ('R6', '#fff153ff'),
    ('R7', '#ffee39ff'),
    ('R8', '#ffec22ff'),
    ('R9', '#ffeb0eff'),
    ('R10', '#ffea00ff')
) AS t (zoning_district_class_id, color);

DROP TABLE IF EXISTS ae_zoning_district_class_descriptions;
CREATE TABLE ae_zoning_district_class_descriptions (
    zoning_district_class_id text,
    description text
);
\COPY ae_zoning_district_class_descriptions FROM 'data/zoning_district_class_descriptions.csv' DELIMITER ',' CSV;
DROP TABLE IF EXISTS ae_zoning_district_class;
CREATE TABLE ae_zoning_district_class AS
WITH distinct_classes AS (
    SELECT DISTINCT zoning_district_class_id AS id
    FROM ae_zoning_district_zoning_district_class
)
SELECT
    zdc.id,
    CASE
        WHEN id LIKE 'C%' THEN 'Commercial'
        WHEN id LIKE 'M%' THEN 'Manufacturing'
        WHEN id LIKE 'R%' THEN 'Residential'
    END AS category,
    d.description,
    NULL AS url,
    c.color
FROM distinct_classes AS zdc
LEFT JOIN ae_zoning_district_class_colors AS c ON zdc.id = c.zoning_district_class_id
LEFT JOIN ae_zoning_district_class_descriptions AS d ON zdc.id = d.zoning_district_class_id;
