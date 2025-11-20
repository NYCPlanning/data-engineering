WITH segments AS (
    SELECT
        {{ dbt_utils.star(ref('int__primary_segments')) }}
    FROM {{ ref("int__primary_segments") }}
    UNION ALL
    SELECT
        {{ dbt_utils.star(ref('int__primary_segments')) }}
    FROM {{ ref("int__protosegments") }}
)
SELECT DISTINCT
    'missing facecode' AS error,
    globalid,
    source_table,
    'segmentid' AS record_id_type,
    segmentid AS record_id
FROM segments
WHERE face_code IS NULL
