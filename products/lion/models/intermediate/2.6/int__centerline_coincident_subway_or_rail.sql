{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['segmentid']},
    ]
) }}

-- Exact matches catches all but ~50 or so
WITH exact_matches AS (
    SELECT
        r.segmentid AS trains_segment_id,
        c.segmentid AS all_segments_id,
        rail_type,
        'exact' AS match_type
    FROM {{ ref('stg__underground_rail') }} AS r
    INNER JOIN {{ ref('stg__centerline') }} AS c ON ST_EQUALS(r.geom, c.geom)
    -- WHERE a.coincident_seg_count > 1 # TODO: Probably reinstate this check
),
inexact_matches AS (
    SELECT
        r.segmentid AS trains_segment_id,
        c.segmentid AS all_segments_id,
        r.rail_type,
        'fuzzy' AS match_type
    FROM {{ ref('stg__underground_rail') }} AS r
    INNER JOIN {{ ref('stg__centerline') }} AS c
        ON ST_DWITHIN(r.midpoint, c.midpoint, .1)
    -- `WHERE NOT...` below shaves off a few seconds from the more conventional:
    -- WHERE r.segmentid NOT IN (SELECT trains_segment_id FROM exact_matches)
    WHERE NOT EXISTS (
        SELECT 1 FROM exact_matches AS em
        WHERE em.trains_segment_id = r.segmentid
    )
    -- AND a.coincident_seg_count > 1 # TODO: Probably reinstate this check
),
all_together AS (
    SELECT * FROM inexact_matches
    UNION
    SELECT * FROM exact_matches
)
SELECT
    all_segments_id AS segmentid,
    COUNT(*) AS subway_or_rail_count
FROM all_together
GROUP BY all_segments_id
