SELECT
    'error' AS log_level,
    'segment missing node' AS error_category,
    'CSCL-LOG-OURS-01' AS log_message_id,
    globalid,
    source_table AS source_feature_layer,
    'segmentid' AS record_id_type,
    segmentid AS record_id,
    {{ log_message(
        "The {} feature with a GlobalID = {} has no {} node.",
        [
            "source_table",
            "globalid",
            "CASE WHEN from_nodeid IS NULL AND to_nodeid IS NULL THEN 'from or to' "
            ~ "WHEN from_nodeid IS NULL THEN 'from' ELSE 'to' END",
        ]
    ) }} AS message
FROM {{ ref('int__lion') }}
WHERE from_nodeid IS NULL OR to_nodeid IS NULL
