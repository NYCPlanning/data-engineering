-- EPSG:2263 is in feet, 43,560 square feet to an acre. always_xy because the source is lon/lat.
WITH pieces AS (
    SELECT
        district.borocd,
        sewershed.wastewater_facility_id,
        ST_Area(
            ST_Transform(ST_Intersection(district.geom, sewershed.geom), 'EPSG:4326', 'EPSG:2263', true)
        ) / 43560 AS overlap_acres,
        ST_Area(ST_Transform(district.geom, 'EPSG:4326', 'EPSG:2263', true)) / 43560 AS district_acres
    FROM {{ ref('int_cd_spine') }} AS district
    INNER JOIN {{ ref('int_wastewater_sewershed_spine') }} AS sewershed
        ON ST_Intersects(district.geom, sewershed.geom)
)

SELECT
    *,
    overlap_acres / district_acres AS share_of_district
FROM pieces
WHERE overlap_acres > 0
