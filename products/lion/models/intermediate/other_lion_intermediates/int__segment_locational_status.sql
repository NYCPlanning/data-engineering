{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['lionkey_dev']},
    ]
) }}

WITH atomicpolygons AS (
    SELECT
        lionkey_dev,
        segmentid,
        segment_borocode,
        left_atomicid,
        left_borocode,
        left_2010_census_tract, -- TODO all these 2010 fields should be 2020, but this aligns with current ETL tool
        right_atomicid,
        right_borocode,
        right_2010_census_tract
    FROM {{ ref("int__segment_atomicpolygons") }}
),

-- many-to-many of segments to nodes
segments_to_nodes AS (
    SELECT * FROM {{ ref("int__segments_to_nodes") }}
),

-- total number of segments associated with each node
segments_by_node AS (
    SELECT
        nodeid,
        count(*) AS n_segments
    FROM segments_to_nodes
    GROUP BY nodeid
),

segments_n_neighbors AS (
    SELECT
        lionkey_dev,
        min(n_segments) AS minimum_neighbors -- between the two nodes, minimum number of segments joined
    FROM segments_to_nodes AS s2n
    INNER JOIN segments_by_node AS sbn ON s2n.nodeid = sbn.nodeid
    GROUP BY lionkey_dev
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
        segment_borocode <> left_borocode AS is_ap_boro_boundary_error,
        CASE
            -- not joined or impossible < 1 case -> NULL
            WHEN snn.minimum_neighbors = 1 THEN 'I'
            WHEN snn.minimum_neighbors > 1 THEN 'H'
        END AS segment_locational_status
    FROM atomicpolygons AS ap
    LEFT JOIN segments_n_neighbors AS snn ON ap.lionkey_dev = snn.lionkey_dev
    WHERE ap.left_atomicid = ap.right_atomicid
),
different_aps_different_boros AS (
    SELECT
        *,
        CASE
            WHEN segment_borocode = right_borocode THEN 'L'
            WHEN segment_borocode = left_borocode THEN 'R'
        END AS borough_boundary_indicator,
        segment_borocode <> left_borocode AND segment_borocode <> right_borocode AS is_ap_boro_boundary_error,
        CASE
            WHEN segment_borocode = right_borocode THEN left_borocode::char(1)
            WHEN segment_borocode = left_borocode THEN right_borocode::char(1)
        END AS segment_locational_status
    FROM atomicpolygons
    WHERE left_atomicid <> right_atomicid AND left_borocode <> right_borocode   -- borocode alone should be sufficient but who knows...
),
different_aps_same_boro AS (
    SELECT
        *,
        NULL AS borough_boundary_indicator,
        segment_borocode <> left_borocode AS is_ap_boro_boundary_error,
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
