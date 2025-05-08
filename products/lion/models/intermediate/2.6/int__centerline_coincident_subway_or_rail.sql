{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['segmentid']},
    ]
) }}

WITH proto AS (
    SELECT * FROM {{ ref("stg__protosegments") }}
)

SELECT
    segmentid,
    count(*) AS subway_or_rail_count
FROM proto
WHERE feature_type = '1' -- TODO - there are none found. This seems wrong
GROUP BY segmentid
