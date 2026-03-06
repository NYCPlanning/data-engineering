SELECT
    'error' AS log_level,
    'segment joined to no atomic polygon' AS error_category,
    globalid,
    source_table AS source_feature_layer,
    'segmentid' AS record_id_type,
    segmentid AS record_id,
    '' AS message
FROM {{ ref('int__lion') }}
WHERE left_atomicid IS NULL AND right_atomicid IS NULL
