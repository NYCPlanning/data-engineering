SELECT
    'warning' AS log_level,
    'Non-street feature invalid line type' AS error_category,
    'CSCL-LOG-LION-23' AS log_message_id,
    'warned' AS effect,
    globalid,
    source_table AS source_feature_layer,
    'segmentid' AS record_id_type,
    segmentid AS record_id,
    FORMAT(
        '[CSCL-LOG-LION-23] LineType appears to be set incorrectly for NonStreetFeature with a GlobalID ''{}''',
        COALESCE(globalid::VARCHAR, '')
    ) AS message
FROM {{ ref('stg__nonstreetfeatures') }}
WHERE linetype NOT BETWEEN 1 AND 6
