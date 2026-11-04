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
    ST_CollectionExtract(ST_Collect(list(geom)), 3) AS geom
FROM pieces
GROUP BY borocd, flooding_category
