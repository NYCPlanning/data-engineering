{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['lionkey_dev']}
    ]
) }}

WITH segments AS (
    SELECT
        {{ dbt_utils.star(ref('int__primary_segments')) }}
    FROM {{ ref("int__primary_segments") }}
    UNION ALL
    SELECT
        {{ dbt_utils.star(ref('int__primary_segments')) }}
    FROM {{ ref("int__protosegments") }}
)
SELECT
    CONCAT(boroughcode, face_code, segment_seqnum, segmentid) AS lionkey_dev, -- TODO remove segmentid, rename field
    *
FROM segments
ORDER BY lionkey_dev
