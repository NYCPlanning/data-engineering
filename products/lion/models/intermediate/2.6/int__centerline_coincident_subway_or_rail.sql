{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['segmentid']},
    ]
) }}

SELECT
    all_segments_id AS segmentid,
    count(*) AS subway_or_rail_count
FROM {{ ref('int__centerline_coincident_subway_or_rail_matches') }}
WHERE starting_coincident_seg_count > 1 AND distance < 1
GROUP BY all_segments_id
