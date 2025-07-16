{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['segmentid']},
    ]
) }}
WITH centerline_offsets AS (
    SELECT * FROM {{ ref("int__centerline_offsets") }}
)

SELECT
    co.segmentid,
    left(left_beat.post, 1) AS left_nypd_service_area,
    left(right_beat.post, 1) AS right_nypd_service_area
FROM centerline_offsets AS co
-- using a cte around reference can confus the postgres compiler to not use index
LEFT JOIN {{ source("recipe_sources", "dcp_cscl_nypdbeat") }} AS left_beat
    ON
        ST_Within(co.left_offset_point, left_beat.geom)
        AND left_beat.geo_type = 'HP'
LEFT JOIN {{ source("recipe_sources", "dcp_cscl_nypdbeat") }} AS right_beat
    ON
        ST_Within(co.right_offset_point, right_beat.geom)
        AND right_beat.geo_type = 'HP'
