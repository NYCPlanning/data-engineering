-- EPSG:2263 is in feet, 43,560 square feet to an acre. always_xy because the source is lon/lat.
WITH pieces AS (
    SELECT
        district.borocd,
        subdistrict.school_subdistrict_id,
        ST_Area(
            ST_Transform(ST_Intersection(district.geom, subdistrict.geom), 'EPSG:4326', 'EPSG:2263', true)
        ) / 43560 AS overlap_acres,
        ST_Area(ST_Transform(district.geom, 'EPSG:4326', 'EPSG:2263', true)) / 43560 AS district_acres,
        ST_Area(ST_Transform(subdistrict.geom, 'EPSG:4326', 'EPSG:2263', true)) / 43560 AS subdistrict_acres
    FROM {{ ref('int_cd_spine') }} AS district
    INNER JOIN {{ ref('int_school_subdistrict_spine') }} AS subdistrict
        ON ST_Intersects(district.geom, subdistrict.geom)
)

SELECT
    *,
    overlap_acres / district_acres AS share_of_district,
    overlap_acres / subdistrict_acres AS share_of_subdistrict
FROM pieces
WHERE overlap_acres > 0
