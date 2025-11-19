{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['lionkey']}
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
    CONCAT(boroughcode, face_code, segment_seqnum) AS lionkey,
    *
FROM segments
ORDER BY lionkey
