-- Potential Spatial Matches between centerlines and underground trains (subways + rail)
-- Leaving this separate from the counts, for diagnostic/exporatory purposes.

{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['segmentid']},
    ]
) }}

-- Exact matches catch all but ~50 or so
WITH exact_matches AS (
    SELECT
        s.segmentid,
        r.segmentid AS joined_segment_id,
        r.feature_type AS joined_segment_feature_type,
        'exact' AS match_type,
        0 AS distance
    FROM {{ ref('int__segments') }} AS s
    INNER JOIN {{ ref('int__segments') }} AS r ON ST_EQUALS(r.geom, s.geom)
    WHERE s.feature_type NOT IN ('centerline', 'nonstreetfeatures')
),
fuzzy_matches AS (
    SELECT
        s.segmentid,
        r.segmentid AS joined_segment_id,
        r.feature_type AS joined_segment_feature_type,
        'fuzzy' AS match_type,
        ST_DISTANCE(r.midpoint, s.midpoint) AS distance
    FROM {{ ref('int__segments') }} AS s
    INNER JOIN {{ ref('int__segments') }} AS r
        -- 2.5 is probably too wide, but convenient for diagnostic/exporatory purposes
        ON ST_DWITHIN(r.midpoint, s.midpoint, 2.5)
    -- `WHERE NOT...` below shaves off a few seconds from the more conventional:
    -- WHERE r.segmentid NOT IN (SELECT trains_segment_id FROM exact_matches)
    WHERE s.feature_type NOT IN ('centerline', 'nonstreetfeatures') AND NOT EXISTS (
        SELECT 1 FROM exact_matches AS em
        WHERE em.joined_segment_id = r.segmentid
    )
)
SELECT * FROM exact_matches
UNION ALL
SELECT * FROM fuzzy_matches
