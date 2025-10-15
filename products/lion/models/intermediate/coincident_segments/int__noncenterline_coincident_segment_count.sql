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
    FROM {{ ref('stg__altsegmentdata_proto') }}
    GROUP BY segmentid
)
SELECT
    sm.segmentid,
    COALESCE(sm_count, 1) + COALESCE(ps_count, 0) AS coincident_seg_count
FROM spatial_matches AS sm
FULL JOIN proto_segments AS ps ON sm.segmentid = ps.segmentid
