-- One row per polygon. The source has one multipolygon per category, none of them valid as a whole, so they're
-- split into their parts (about 35,000) and each part is made valid.
WITH parts AS (
    SELECT
        flooding_category AS flooding_category_code,
        unnest(ST_Dump(ST_GeomFromWKB(geometry))).geom AS geom
    FROM {{ source('sources', 'dep_stormwater_flooding_2050') }}
)

SELECT
    flooding_category_code,
    CASE flooding_category_code
        WHEN 1 THEN 'Nuisance Flooding'
        WHEN 2 THEN 'Deep and Contiguous Flooding'
        WHEN 3 THEN 'Future High Tides 2050'
    END AS flooding_category,
    ST_Transform(ST_MakeValid(geom), 'EPSG:2263', 'EPSG:4326', true) AS geom
FROM parts
