{{ config(
    materialized = 'table',
) }}

WITH exact_matches AS (
    SELECT
        r.segmentid AS trains_segment_id,
        a.segmentid AS all_segments_id,
        rail_type,
        'exact' AS match_type
    FROM {{ ref('stg__rail_aboveground') }} r
    INNER JOIN {{ ref('stg__centerline') }} a ON ST_EQUALS(r.geom, a.geom)
    WHERE a.coincident_seg_count > 1
),
inexact_matches AS (
    SELECT
        r.segmentid AS trains_segment_id,
        a.segmentid AS all_segments_id,
        r.rail_type,
        'fuzzy' AS match_type
    FROM {{ ref('stg__rail_aboveground') }} r
    INNER JOIN {{ ref('stg__centerline') }} a
        ON (
            -- Check if start points are within 1 foot
            -- ST_DWithin(ST_StartPoint(r.geom), ST_StartPoint(a.geom), 1) AND
            -- Check if end points are within 1 foot
            --ST_DWithin(ST_EndPoint(r.geom), ST_EndPoint(a.geom), 1) AND
            -- Check if centroids are within 1 foot
            ST_DWITHIN(r.centroid, a.centroid, .1)
        )
    -- `WHERE NOT...` below shaves off a few seconds from the more conventional:
    -- WHERE r.segmentid NOT IN (SELECT trains_segment_id FROM exact_matches)
    WHERE NOT EXISTS (
        SELECT 1 FROM exact_matches em
        WHERE em.trains_segment_id = r.segmentid
    )
    AND a.coincident_seg_count > 1
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
