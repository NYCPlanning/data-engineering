{{ config(
    materialized = 'table'
) }}
-- Segment locational status (LION L41) and borough boundary indicator (L46) per segment.
-- Two answers for L41, side by side: prod_segment_locational_status, prod's (an 'X' where
-- the left and right AtomicPolygons' 2010 census tracts differ, CenterlineExtractFile.cs
-- GetSegmentLocationalStatus), and spec_segment_locational_status, the spec's (their 2020
-- tracts, CENSUSTRACT2020). They differ only in the 'X' case (CSCL-LION-11).
-- fix_seg_loc_status_2020_tract picks which fills segment_locational_status.
{% set chosen = 'spec' if var('fix_seg_loc_status_2020_tract') else 'prod' %}

WITH atomicpolygons AS (
    SELECT
        globalid,
        lionkey,
        segmentid,
        segment_borocode,
        left_atomicid,
        left_borocode,
        left_2010_census_tract,
        left_2020_census_tract,
        right_atomicid,
        right_borocode,
        right_2010_census_tract,
        right_2020_census_tract
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
        globalid,
        min(n_segments) AS minimum_neighbors -- between the two nodes, minimum number of segments joined
    FROM segments_to_nodes AS s2n
    INNER JOIN segments_by_node AS sbn ON s2n.nodeid = sbn.nodeid
    GROUP BY globalid
),

-- All CTEs below divide the atomicpolygons into different categories. 
-- When unioned together, they should have 1-1 match with atomicpolygons
left_city_boundary_aps AS (
    SELECT
        *,
        'L' AS borough_boundary_indicator,
        NULL::boolean AS is_ap_boro_boundary_error,
        '9' AS prod_segment_locational_status,
        '9' AS spec_segment_locational_status
    FROM atomicpolygons
    WHERE left_atomicid IS NULL AND right_atomicid IS NOT NULL

),
right_city_boundary_aps AS (
    SELECT
        *,
        'R' AS borough_boundary_indicator,
        NULL::boolean AS is_ap_boro_boundary_error,
        '9' AS prod_segment_locational_status,
        '9' AS spec_segment_locational_status
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
        END AS prod_segment_locational_status,
        prod_segment_locational_status AS spec_segment_locational_status
    FROM atomicpolygons AS ap
    LEFT JOIN segments_n_neighbors AS snn ON ap.globalid = snn.globalid
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
        END AS prod_segment_locational_status,
        prod_segment_locational_status AS spec_segment_locational_status
    FROM atomicpolygons
    WHERE left_atomicid <> right_atomicid AND left_borocode <> right_borocode   -- borocode alone should be sufficient but who knows...
),
different_aps_same_boro AS (
    SELECT
        *,
        NULL AS borough_boundary_indicator,
        segment_borocode <> left_borocode AS is_ap_boro_boundary_error,
        -- prod compares the 2010 tracts (CenterlineExtractFile.cs:1447-1452), the spec the
        -- 2020 tracts (CSCL-LION-11). Each is the full tract, basic and suffix.
        CASE
            WHEN left_2010_census_tract <> right_2010_census_tract THEN 'X'
        END AS prod_segment_locational_status,
        CASE
            WHEN left_2020_census_tract <> right_2020_census_tract THEN 'X'
        END AS spec_segment_locational_status
    FROM atomicpolygons
    WHERE left_atomicid <> right_atomicid AND left_borocode = right_borocode
),
-- the CTE below accounts for erroneous segments. This is done for 1-1 matching with the segments table
segments_without_aps AS (
    SELECT
        *,
        NULL AS borough_boundary_indicator,
        TRUE AS is_ap_boro_boundary_error,
        NULL AS prod_segment_locational_status,
        NULL AS spec_segment_locational_status
    FROM atomicpolygons
    WHERE left_atomicid IS NULL AND right_atomicid IS NULL
),

all_aps AS (
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
)

SELECT
    *,
    {{ chosen }}_segment_locational_status AS segment_locational_status
FROM all_aps
