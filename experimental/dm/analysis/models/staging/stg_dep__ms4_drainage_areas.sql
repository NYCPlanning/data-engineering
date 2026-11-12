-- The source splits one drainage area into several polygons, typically a city-owned and an unknown-ownership
-- part. Only polygons whose attributes all match are merged. About a fifth of the source polygons are invalid.
-- Simplified to 3 feet before the repair, since ST_MakeValid on the full-detail source takes minutes. That
-- loses well under 0.1% of the area.
SELECT
    id AS drains_to,
    waterbody AS waterbody,
    ownership AS assumed_ownership,
    floatables = 'YES' AS floatables_of_concern,
    pathogens = 'YES' AS pathogens_of_concern,
    nitrogen = 'YES' AS nitrogen_of_concern,
    phosphorus = 'YES' AS phosphorus_of_concern,
    ST_Transform(
        ST_Union_Agg(ST_MakeValid(ST_SimplifyPreserveTopology(ST_GeomFromWKB(geometry), 3))),
        'EPSG:2263',
        'EPSG:4326',
        true
    ) AS geom
FROM {{ source('sources', 'ms4_drainage_areas') }}
GROUP BY ALL
