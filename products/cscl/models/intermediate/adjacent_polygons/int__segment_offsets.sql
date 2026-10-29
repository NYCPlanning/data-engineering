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
FROM (
    SELECT
        s.*,
        offset_points(s.geom, offset_length := 2) AS offsets
    FROM segments AS s
) AS segments_with_offsets
