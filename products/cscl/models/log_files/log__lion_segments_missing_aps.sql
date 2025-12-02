SELECT
    'segment joined to no atomic polygon' AS error,
    globalid,
    source_table,
    'segmentid' AS record_id_type,
    segmentid AS record_id,
    '' AS message
FROM {{ ref('int__lion') }}
WHERE left_atomicid IS NULL AND right_atomicid IS NULL
