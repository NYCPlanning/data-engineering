-- int__centerline_offsets_original
-- takes centerline segment, generates offset points
-- method in docs is 
--     "To determine the given segment’s left and right APs, the ETL tool will generate 
--     temporary left and right “offset points” positioned on both sides of the segment,
--     offset two feet perpendicularly from the segment’s midpoint"
-- TODO: decide on current method or appropriate replacement

{{ config(
    materialized = 'table'
) }}

WITH centerline AS (
    SELECT * FROM {{ ref("stg__centerline") }}
    -- TODO remove this -> likely from odd source data.
    WHERE segmentid NOT IN (
        354785, -- Large circular segment extending into other states
        9008702 -- Strange small simple line that is discontinuous
    )
)

SELECT
    segmentid,
    boroughcode,
    geom,
    left_offset_point,
    right_offset_point
FROM centerline
CROSS JOIN LATERAL offset_points(geom, 2) AS (left_offset_point geometry, right_offset_point geometry)
