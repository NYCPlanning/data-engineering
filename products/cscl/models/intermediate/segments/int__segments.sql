{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['globalid']}
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
    WHERE geom IS NOT NULL -- proxy for joined to a segment
)
SELECT
    CONCAT(boroughcode, face_code, segment_seqnum) AS lionkey,
    *
FROM segments
WHERE face_code IS NOT NULL
ORDER BY lionkey
