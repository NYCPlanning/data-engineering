-- Spatial matches for non-centerline segments.
-- The current ETL logic (which differs from the legacy ETL docs...) is:
-- 1. match non-centerline segments to their own type (e.g. shorelines with shorelines)
-- 2. then match them to centerlines (except when they're subeterranean rail)
--
-- "own type" is source_table (the literal ArcGIS feature class - centerline, shoreline,
-- nonstreetfeatures, rail, subway), not feature_type: legacy scopes this to the feature
-- class, so a Subway/Rail crossing is never "same type" there. Our feature_type merges
-- Subway+Rail into one 'rail_and_subway' value, so using it here double-counts a
-- subway/rail crossing as a same-type coincidence on top of the centerline match. See
-- https://github.com/NYCPlanning/data-engineering/issues/2616.

{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['segmentid']},
    ]
) }}

--
SELECT
    s.segmentid,
    coinc.segmentid AS coinc_segment_id,
    coinc.feature_type AS coinc_segment_feature_type,
    'fuzzy' AS match_type,
    ST_DISTANCE(s.midpoint, coinc.midpoint) AS distance
FROM {{ ref('int__primary_segments') }} AS s
INNER JOIN {{ ref('int__primary_segments') }} AS coinc
    ON
        ST_DWITHIN(s.midpoint, coinc.midpoint, 2.5)
        AND
        (
            s.source_table = coinc.source_table  -- match to segments of same type
            OR
            (
                coinc.source_table = 'centerline'
                AND s.segmentid NOT IN (SELECT segmentid FROM {{ ref('int__underground_rail') }})  -- to centerlines without underground rail
            )
        )
WHERE s.feature_type <> 'centerline'
