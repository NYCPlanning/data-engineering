-- EPSG:2263 is in feet, 43,560 square feet to an acre. always_xy because the source is lon/lat.
WITH pieces AS (
    SELECT
        district.borocd,
        area.sewer_area_type,
        ST_Area(
            ST_Transform(ST_Intersection(district.geom, area.geom), 'EPSG:4326', 'EPSG:2263', true)
        ) / 43560 AS overlap_acres,
        ST_Area(ST_Transform(district.geom, 'EPSG:4326', 'EPSG:2263', true)) / 43560 AS district_acres
    FROM {{ ref('int_cd_spine') }} AS district
    INNER JOIN {{ ref('int_sewer_area_spine') }} AS area
        ON ST_Intersects(district.geom, area.geom)
)

SELECT
    *,
    overlap_acres / district_acres AS share_of_district
FROM pieces
WHERE overlap_acres > 0
