-- Most flood polygons are wholly inside one district, and those skip the intersection with the district's
-- detailed boundary, which is most of the cost. The geometry is the flooding in the district, for its map.
-- EPSG:2263 is in feet, 43,560 square feet to an acre. always_xy because the source is lon/lat.
WITH pieces AS (
    SELECT
        district.borocd,
        flooding.flooding_category,
        CASE
            WHEN ST_Within(flooding.geom, district.geom) THEN flooding.geom
            ELSE ST_Intersection(district.geom, flooding.geom)
        END AS geom
    FROM {{ ref('int_cd_spine') }} AS district
    INNER JOIN {{ ref('stg_dep__stormwater_flooding_2050') }} AS flooding
        ON ST_Intersects(district.geom, flooding.geom)
),

districts AS (
    SELECT
        borocd,
        flooding_category,
        sum(ST_Area(ST_Transform(geom, 'EPSG:4326', 'EPSG:2263', true))) / 43560 AS overlap_acres,
        -- polygons only: an intersection along the district's edge can leave lines and points, and Leaflet stops
        -- drawing the map at a geometry collection holding them
        ST_CollectionExtract(ST_Collect(list(geom)), 3) AS geom
    FROM pieces
    GROUP BY borocd, flooding_category
)

SELECT
    districts.borocd,
    districts.flooding_category,
    districts.overlap_acres,
    ST_Area(ST_Transform(spine.geom, 'EPSG:4326', 'EPSG:2263', true)) / 43560 AS district_acres,
    districts.overlap_acres / district_acres AS share_of_district,
    districts.geom
FROM districts
INNER JOIN {{ ref('int_cd_spine') }} AS spine ON districts.borocd = spine.borocd
WHERE districts.overlap_acres > 0
