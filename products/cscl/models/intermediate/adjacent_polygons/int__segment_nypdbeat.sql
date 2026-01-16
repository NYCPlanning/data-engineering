{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['globalid']},
    ]
) }}
WITH segment_offsets AS (
    SELECT * FROM {{ ref("int__segment_offsets") }}
)

SELECT
    co.globalid,
    co.lionkey,
    co.segmentid,
    left(left_beat.post, 1) AS left_nypd_service_area,
    left(right_beat.post, 1) AS right_nypd_service_area
FROM segment_offsets AS co
-- using a cte around reference can confus the postgres compiler to not use index
LEFT JOIN {{ ref("stg__nypdbeat") }} AS left_beat
    ON
        st_within(co.left_offset_point, left_beat.geom)
        AND left_beat.geo_type = 'HP'
LEFT JOIN {{ ref("stg__nypdbeat") }} AS right_beat
    ON
        st_within(co.right_offset_point, right_beat.geom)
        AND right_beat.geo_type = 'HP'
