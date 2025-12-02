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
    left_beat.sector AS left_nypd_sector,
    left_beat.geo_type AS left_beat_geo_type,
    CASE WHEN left_beat.geo_type = 'HP' THEN left(left_beat.post, 1) END AS left_nypd_service_area,
    right_beat.sector AS right_nypd_sector,
    right_beat.geo_type AS right_beat_geo_type,
    CASE WHEN right_beat.geo_type = 'HP' THEN left(left_beat.post, 1) END AS right_nypd_service_area
FROM segment_offsets AS co
-- using a cte around reference can confus the postgres compiler to not use index
LEFT JOIN {{ ref("stg__nypdbeat") }} AS left_beat
    ON st_within(co.left_offset_point, left_beat.geom)
LEFT JOIN {{ ref("stg__nypdbeat") }} AS right_beat
    ON st_within(co.right_offset_point, right_beat.geom)
