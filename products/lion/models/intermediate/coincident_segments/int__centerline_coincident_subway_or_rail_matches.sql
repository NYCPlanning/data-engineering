-- Potential Spatial Matches between centerlines and underground trains (subways + rail)
-- Leaving this separate from the counts, for diagnostic/exporatory purposes.

{{ config(
    materialized = 'table',
) }}

-- Exact matches catch all but ~50 or so
WITH exact_matches AS (
    SELECT
        r.segmentid AS trains_segment_id,
        c.segmentid AS all_segments_id,
        r.source_feature_type AS rail_type,
        'exact' AS match_type,
        c.coincident_seg_count AS starting_coincident_seg_count,
        0 AS distance,
        r.geom AS rail_geom,
        r.midpoint AS rail_midpoint,
        s.geom AS centerline_geom,
        s.midpoint AS centerline_midpoint
    FROM {{ ref('stg__centerline') }} AS c
    INNER JOIN {{ ref('int__segments') }} AS s ON c.segmentid = s.segmentid
    INNER JOIN {{ ref('int__underground_rail') }} AS r ON ST_EQUALS(r.geom, s.geom)
),
fuzzy_matches AS (
    SELECT
        r.segmentid AS trains_segment_id,
        c.segmentid AS all_segments_id,
        r.source_feature_type AS rail_type,
        'fuzzy' AS match_type,
        c.coincident_seg_count AS starting_coincident_seg_count,
        ST_DISTANCE(r.midpoint, s.midpoint) AS distance,
        r.geom AS rail_geom,
        r.midpoint AS rail_midpoint,
        s.geom AS centerline_geom,
        s.midpoint AS centerline_midpoint
    FROM {{ ref('stg__centerline') }} AS c
    INNER JOIN {{ ref('int__segments') }} AS s ON c.segmentid = s.segmentid
    INNER JOIN {{ ref('int__underground_rail') }} AS r
        -- 2.5 is probably too wide, but convenient for diagnostic/exporatory purposes
        ON ST_DWITHIN(r.midpoint, s.midpoint, 2.5)
    -- `WHERE NOT...` below shaves off a few seconds from the more conventional:
    -- WHERE r.segmentid NOT IN (SELECT trains_segment_id FROM exact_matches)
    WHERE NOT EXISTS (
        SELECT 1 FROM exact_matches AS em
        WHERE em.trains_segment_id = r.segmentid
    )
)
SELECT * FROM exact_matches
UNION ALL
SELECT * FROM fuzzy_matches
