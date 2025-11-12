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
    -- Note: this distance is somewhat arbitrary, but through trial and error it seems
    -- to be about the sweet spot where decreasing it results in our missing matches,
    -- and vice-versa.
    WHERE distance < .001
    GROUP BY segmentid
),
proto_segments AS (
    SELECT
        segmentid,
        COUNT(*) AS ps_count
    FROM {{ source('recipe_sources', 'dcp_cscl_altsegmentdata') }}
    WHERE segmentid NOT IN (SELECT segmentid FROM {{ ref('int__underground_rail') }})
    --    WHERE alt_segdata_type <> 'S' TODO - this is behavior described in docs
    --                                  but C# code does not filter on this
    GROUP BY segmentid
)
SELECT
    COALESCE(sm.segmentid, ps.segmentid) AS segmentid,
    COALESCE(sm_count, 1) + COALESCE(ps_count, 0) AS coincident_seg_count
FROM spatial_matches AS sm
FULL JOIN proto_segments AS ps ON sm.segmentid = ps.segmentid
