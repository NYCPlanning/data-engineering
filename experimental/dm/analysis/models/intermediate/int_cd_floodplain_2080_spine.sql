-- Every district, with the part of it in the 2080s 100-year floodplain (none for most inland districts), and the
-- floodplain in its map frame (see int_cd_map_frame_spine). The geometries are polygons only, as in
-- int_cd_stormwater_flooding_spine. EPSG:2263 is in feet, 43,560 square feet to an acre.
WITH district_pieces AS (
    SELECT
        district.borocd,
        sum(
            ST_Area(
                ST_Transform(
                    CASE
                        WHEN ST_Within(floodplain.geom, district.geom) THEN floodplain.geom
                        ELSE ST_Intersection(district.geom, floodplain.geom)
                    END,
                    'EPSG:4326',
                    'EPSG:2263',
                    true
                )
            )
        ) / 43560 AS floodplain_acres
    FROM {{ ref('int_cd_spine') }} AS district
    INNER JOIN {{ ref('stg_npcc__floodplain_2080_100yr') }} AS floodplain
        ON ST_Intersects(district.geom, floodplain.geom)
    GROUP BY district.borocd
),

frame_pieces AS (
    SELECT
        frames.borocd,
        ST_CollectionExtract(
            ST_Collect(
                list(
                    CASE
                        WHEN ST_Within(floodplain.geom, frames.frame) THEN floodplain.geom
                        ELSE ST_Intersection(frames.frame, floodplain.geom)
                    END
                )
            ),
            3
        ) AS frame_geom
    FROM {{ ref('int_cd_map_frame_spine') }} AS frames
    INNER JOIN {{ ref('stg_npcc__floodplain_2080_100yr') }} AS floodplain
        ON ST_Intersects(frames.frame, floodplain.geom)
    GROUP BY frames.borocd
),

-- the floodplain's outer edges in the frame, without the edges of its dry holes, for outlining it on a map. Taken
-- from the unclipped polygons, so the frame's own sides don't show up as edges.
frame_edges AS (
    SELECT
        borocd,
        ST_CollectionExtract(ST_Collect(list(ST_Intersection(frame, ST_ExteriorRing(polygon)))), 2) AS frame_edge_geom
    FROM (
        SELECT
            frames.borocd,
            frames.frame,
            unnest(ST_Dump(floodplain.geom)).geom AS polygon
        FROM {{ ref('int_cd_map_frame_spine') }} AS frames
        INNER JOIN {{ ref('stg_npcc__floodplain_2080_100yr') }} AS floodplain
            ON ST_Intersects(frames.frame, floodplain.geom)
    )
    GROUP BY borocd
)

SELECT
    spine.borocd,
    coalesce(district_pieces.floodplain_acres, 0) AS floodplain_acres,
    ST_Area(ST_Transform(spine.geom, 'EPSG:4326', 'EPSG:2263', true)) / 43560 AS district_acres,
    coalesce(district_pieces.floodplain_acres, 0) / district_acres AS floodplain_share_of_district,
    frame_pieces.frame_geom,
    frame_edges.frame_edge_geom
FROM {{ ref('int_cd_spine') }} AS spine
LEFT JOIN district_pieces ON spine.borocd = district_pieces.borocd
LEFT JOIN frame_pieces ON spine.borocd = frame_pieces.borocd
LEFT JOIN frame_edges ON spine.borocd = frame_edges.borocd
