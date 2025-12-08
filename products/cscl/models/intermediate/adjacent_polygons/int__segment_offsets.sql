{{ config(
    materialized = 'table'
) }}

WITH segments AS (
    SELECT * FROM {{ ref("int__segments") }}
)

SELECT
    globalid,
    lionkey,
    segmentid,
    boroughcode,
    feature_type,
    geom,
    offsets.left_offset_point,
    offsets.right_offset_point
FROM segments,
    LATERAL offset_points(segments.geom, 2) AS offsets (left_offset_point geometry, right_offset_point geometry)
