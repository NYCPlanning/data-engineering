SELECT
    'error' AS log_level,
    'segment missing node' AS error_category,
    'CSCL-LOG-OURS-01' AS log_message_id,
    'warned' AS effect,
    globalid,
    source_table AS source_feature_layer,
    'segmentid' AS record_id_type,
    segmentid AS record_id,
    FORMAT(
        '[CSCL-LOG-OURS-01] The {} feature with a GlobalID = {} has no {} node.',
        COALESCE(source_table::VARCHAR, ''),
        COALESCE(globalid::VARCHAR, ''),
        CASE
            WHEN from_nodeid IS NULL AND to_nodeid IS NULL THEN 'from or to'
            WHEN from_nodeid IS NULL THEN 'from'
            ELSE 'to'
        END
    ) AS message
FROM {{ ref('int__lion') }}
WHERE from_nodeid IS NULL OR to_nodeid IS NULL
