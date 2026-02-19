SELECT
    'error' AS log_level,
    'segment missing node' AS error_category,
    globalid,
    source_table AS source_feature_layer,
    'segmentid' AS record_id_type,
    segmentid AS record_id,
    '' AS message
FROM {{ ref('int__lion') }}
WHERE from_nodeid IS NULL OR to_nodeid IS NULL
