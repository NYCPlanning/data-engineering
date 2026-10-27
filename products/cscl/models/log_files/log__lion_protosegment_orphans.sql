SELECT
    'error' AS log_level,
    'protosegment without geometry-modeled segment' AS error_category,
    'CSCL-LOG-OURS-02' AS log_message_id,
    globalid,
    source_table AS source_feature_layer,
    'segmentid' AS record_id_type,
    segmentid AS record_id,
    {{ log_message(
        'Protosegment with globalid "{}" and segmentid "{}" has no corresponding geometry-modeled segment.',
        ["globalid", "segmentid::INT"]
    ) }} AS message
FROM {{ ref('int__protosegments') }}
WHERE geom IS NULL
