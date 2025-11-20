SELECT
    'warning' AS log_level,
    'Non-street feature invalid line type' AS error_category,
    globalid,
    source_table AS source_feature_layer,
    'segmentid' AS record_id_type,
    segmentid AS record_id,
    FORMAT(
        'LineType appears to be set incorrectly for NonStreetFeature with an OID = ''%s''. '
        || 'The line type value is ''%s''.',
        globalid,
        linetype
    ) AS message
FROM {{ ref('stg__nonstreetfeatures') }}
WHERE linetype NOT BETWEEN 1 AND 6
