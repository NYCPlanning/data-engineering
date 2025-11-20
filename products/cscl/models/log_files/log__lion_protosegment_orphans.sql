SELECT
    'error' AS log_level,
    'protosegment without geometry-modeled segment' AS error_category,
    globalid,
    source_table AS source_feature_layer,
    'segmentid' AS record_id_type,
    segmentid AS record_id,
    FORMAT(
        'Protosegment with globalid "%s" and segmentid "%s" has no corresponding geometry-modeled segment.',
        globalid,
        segmentid::INT
    ) AS message
FROM {{ ref('int__protosegments') }}
WHERE geom IS NULL
