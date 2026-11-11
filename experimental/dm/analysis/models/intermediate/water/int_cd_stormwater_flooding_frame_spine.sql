-- The flooding a district's map shows, not just what's inside the district (see int_cd_map_frame_spine).
WITH pieces AS (
    SELECT
        frames.borocd,
        flooding.flooding_category,
        CASE
            WHEN ST_Within(flooding.geom, frames.frame) THEN flooding.geom
            ELSE ST_Intersection(frames.frame, flooding.geom)
        END AS geom
    FROM {{ ref('int_cd_map_frame_spine') }} AS frames
    INNER JOIN {{ ref('stg_dep__stormwater_flooding_2050') }} AS flooding
        ON ST_Intersects(frames.frame, flooding.geom)
)

SELECT
    borocd,
    flooding_category,
    -- polygons only, as in int_cd_stormwater_flooding_spine
    ST_CollectionExtract(ST_Collect(list(geom)), 3) AS geom,
    -- for maps: simplified to about 1 m and rounded to 6 decimals, ahead of time since it takes seconds per district.
    -- Plain simplification, which only drops pieces under a meter or so.
    ST_ReducePrecision(ST_Simplify(ST_CollectionExtract(ST_Collect(list(geom)), 3), 0.00001), 0.000001) AS map_geom
FROM pieces
GROUP BY borocd, flooding_category
