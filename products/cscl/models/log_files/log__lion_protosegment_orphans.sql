SELECT
    'error' AS log_level,
    'protosegment without geometry-modeled segment' AS error_category,
    'CSCL-LOG-OURS-02' AS log_message_id,
    'dropped' AS effect,
    globalid,
    source_table AS source_feature_layer,
    'segmentid' AS record_id_type,
    segmentid AS record_id,
    FORMAT(
        '[CSCL-LOG-OURS-02] Protosegment with globalid "{}" and segmentid "{}" has no '
        || 'corresponding geometry-modeled segment.',
        COALESCE(globalid::VARCHAR, ''),
        COALESCE(segmentid::INT::VARCHAR, '')
    ) AS message
FROM {{ ref('int__protosegments') }}
WHERE geom IS NULL
