{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['segmentid']},
    ]
) }}

WITH spatial_matches AS (
    SELECT
        segmentid,
        COUNT(*) AS sm_count
    FROM {{ ref('int__noncenterline_coincident_segments') }}
    WHERE distance < 1
    GROUP BY segmentid
),
proto_segments AS (
    SELECT
        segmentid,
        COUNT(*) AS ps_count
    FROM {{ source('recipe_sources', 'dcp_cscl_altsegmentdata') }}
    WHERE alt_segdata_type <> 'S'
    GROUP BY segmentid
)
SELECT
    sm.segmentid, -- fyi, Full Join causes 831 cases of null sm segmentids
    COALESCE(sm_count, 1) + COALESCE(ps_count, 0) AS coincident_seg_count
FROM spatial_matches AS sm
LEFT JOIN proto_segments AS ps ON sm.segmentid = ps.segmentid
