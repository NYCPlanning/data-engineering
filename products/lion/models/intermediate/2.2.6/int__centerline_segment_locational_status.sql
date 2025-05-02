{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['segmentid']},
    ]
) }}
WITH atomicpolygons AS (
    SELECT
        segmentid,
        centerline_segment_borocode AS centerline_borocode,
        left_atomicid,
        left_borocode,
        left_2020_census_tract,
        right_atomicid,
        right_borocode,
        right_2020_census_tract
    FROM {{ ref("int__centerline_atomicpolygons") }}
),
-- TODO:
-- Create CTE with atomicpolygons and indicator for segment endpoints intersecting other segments

-- All CTEs below divide the atomicpolygons into different categories. 
-- When unioned together, they should have 1-1 match with atomicpolygons
left_city_boundary_aps AS (
    SELECT 
        *,
        NULL AS borough_boundary_indicator,
        NULL AS segment_locational_status
    FROM atomicpolygons
    WHERE left_atomicid IS NULL AND right_atomicid IS NOT NULL

),
right_city_boundary_aps AS (
    SELECT
        *,
        NULL AS borough_boundary_indicator,
        NULL AS segment_locational_status
    FROM atomicpolygons
    WHERE left_atomicid IS NOT NULL AND right_atomicid IS NULL
),
same_ap AS (
    SELECT
        *,
        NULL AS borough_boundary_indicator,
        NULL AS segment_locational_status
    FROM atomicpolygons
    WHERE left_atomicid = right_atomicid
),
different_aps_different_boros AS (
    SELECT
        *,
        NULL AS borough_boundary_indicator,
        NULL AS segment_locational_status
    FROM atomicpolygons
    WHERE left_atomicid <> right_atomicid AND left_borocode <> right_borocode   -- borocode alone should be sufficient but who knows...
),
different_aps_same_boro AS (
    SELECT
        *,
        NULL AS borough_boundary_indicator,
        NULL AS segment_locational_status
    FROM atomicpolygons
    WHERE left_atomicid <> right_atomicid AND left_borocode = right_borocode
),
-- the CTE below accounts for erroneous segments. This is done for 1-1 matching with the segments table
segments_without_aps AS (
    SELECT
        *,
        NULL AS borough_boundary_indicator,
        true AS is_ap_boro_boundary_error,
        NULL AS segment_locational_status
    FROM atomicpolygons
    WHERE left_atomicid IS NULL AND right_atomicid IS NULL
)

SELECT * FROM left_city_boundary_aps
UNION
SELECT * FROM right_city_boundary_aps
UNION
SELECT * FROM same_ap
UNION
SELECT * FROM different_aps_different_boros
UNION
SELECT * FROM different_aps_same_boro
UNION
SELECT * FROM segments_without_aps
