-- Pieces of one type overlap by a few acres citywide and are counted twice. Unioning them by type first
-- avoids that but takes about ten times as long.
-- EPSG:2263 is in feet, 43,560 square feet to an acre. always_xy because the source is lon/lat.
WITH pieces AS (
    SELECT
        district.borocd,
        drainage.drains_to_type,
        sum(
            ST_Area(
                ST_Transform(ST_Intersection(district.geom, drainage.geom), 'EPSG:4326', 'EPSG:2263', true)
            )
        ) / 43560 AS overlap_acres,
        any_value(ST_Area(ST_Transform(district.geom, 'EPSG:4326', 'EPSG:2263', true))) / 43560 AS district_acres
    FROM {{ ref('int_cd_spine') }} AS district
    INNER JOIN {{ ref('int_ms4_drainage_area_spine') }} AS drainage ON ST_Intersects(district.geom, drainage.geom)
    GROUP BY district.borocd, drainage.drains_to_type
)

SELECT
    *,
    overlap_acres / district_acres AS share_of_district
FROM pieces
WHERE overlap_acres > 0
