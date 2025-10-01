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
        count(*) AS ps_count
    FROM {{ source('recipe_sources', 'dcp_cscl_altsegmentdata') }}
    WHERE alt_segdata_type <> 'S'
    GROUP BY segmentid
)
SELECT
    sm.segmentid,
    coalesce(sm_count, 1) + coalesce(ps_count, 0) AS coincident_seg_count
FROM spatial_matches AS sm
FULL JOIN proto_segments AS ps ON sm.segmentid = ps.segmentid
