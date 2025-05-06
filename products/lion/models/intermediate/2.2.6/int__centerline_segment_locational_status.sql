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
        left_2010_census_tract, -- TODO all these 2010 fields should be 2020, but this aligns with current ETL tool
        right_atomicid,
        right_borocode,
        right_2010_census_tract
    FROM {{ ref("int__centerline_atomicpolygons") }}
),
segments_with_neighbors AS (
    SELECT * FROM {{ ref("int__centerline_segment_neighbors") }}
),
segments_with_orphan_node AS (
    SELECT
        segmentid,
        MAX(node_missing_neighbor) AS segment_missing_neighbor
    FROM segments_with_neighbors
    GROUP BY segmentid
),

-- All CTEs below divide the atomicpolygons into different categories. 
-- When unioned together, they should have 1-1 match with atomicpolygons
left_city_boundary_aps AS (
    SELECT
        *,
        'L' AS borough_boundary_indicator,
        NULL::boolean AS is_ap_boro_boundary_error,
        '9' AS segment_locational_status
    FROM atomicpolygons
    WHERE left_atomicid IS NULL AND right_atomicid IS NOT NULL

),
right_city_boundary_aps AS (
    SELECT
        *,
        'R' AS borough_boundary_indicator,
        NULL::boolean AS is_ap_boro_boundary_error,
        '9' AS segment_locational_status
    FROM atomicpolygons
    WHERE left_atomicid IS NOT NULL AND right_atomicid IS NULL
),
same_ap AS (
    SELECT
        ap.*,
        NULL AS borough_boundary_indicator,
        centerline_borocode <> left_borocode AS is_ap_boro_boundary_error,
        CASE
            WHEN o.segment_missing_neighbor = 1 THEN 'I'
            WHEN o.segment_missing_neighbor = 0 THEN 'H'
        END AS segment_locational_status
    FROM atomicpolygons AS ap
    LEFT JOIN segments_with_orphan_node AS o
        ON ap.segmentid = o.segmentid
    WHERE ap.left_atomicid = ap.right_atomicid
),
different_aps_different_boros AS (
    SELECT
        *,
        CASE
            WHEN centerline_borocode = right_borocode THEN 'L'
            WHEN centerline_borocode = left_borocode THEN 'R'
        END AS borough_boundary_indicator,
        centerline_borocode <> left_borocode AND centerline_borocode <> right_borocode AS is_ap_boro_boundary_error,
        CASE
            WHEN centerline_borocode = right_borocode THEN left_borocode::char(1)
            WHEN centerline_borocode = left_borocode THEN right_borocode::char(1)
        END AS segment_locational_status
    FROM atomicpolygons
    WHERE left_atomicid <> right_atomicid AND left_borocode <> right_borocode   -- borocode alone should be sufficient but who knows...
),
different_aps_same_boro AS (
    SELECT
        *,
        NULL AS borough_boundary_indicator,
        centerline_borocode <> left_borocode AS is_ap_boro_boundary_error,
        CASE
            WHEN left_2010_census_tract <> right_2010_census_tract THEN 'X'
        END AS segment_locational_status
    FROM atomicpolygons
    WHERE left_atomicid <> right_atomicid AND left_borocode = right_borocode
),
-- the CTE below accounts for erroneous segments. This is done for 1-1 matching with the segments table
segments_without_aps AS (
    SELECT
        *,
        NULL AS borough_boundary_indicator,
        TRUE AS is_ap_boro_boundary_error,
        NULL AS segment_locational_status
    FROM atomicpolygons
    WHERE left_atomicid IS NULL AND right_atomicid IS NULL
)

SELECT * FROM left_city_boundary_aps
UNION ALL
SELECT * FROM right_city_boundary_aps
UNION ALL
SELECT * FROM same_ap
UNION ALL
SELECT * FROM different_aps_different_boros
UNION ALL
SELECT * FROM different_aps_same_boro
UNION ALL
SELECT * FROM segments_without_aps
