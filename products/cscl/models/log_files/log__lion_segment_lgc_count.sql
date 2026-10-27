WITH lgc AS (
    SELECT * FROM {{ ref("int__lgc") }}
),
segments AS (
    SELECT * FROM {{ ref("int__primary_segments") }}
),
max_lgc AS (
    SELECT
        segmentid,
        MAX(lgc_rank) AS lgc_count
    FROM lgc
    WHERE lgc_rank > 9
    GROUP BY segmentid
)
SELECT
    'error' AS log_level,
    'more than 9 lgcs for a given segmentid' AS error_category,
    'CSCL-LOG-LION-17' AS log_message_id,
    'warned' AS effect,
    segments.globalid,
    segments.source_table AS source_feature_layer,
    'segmentid' AS record_id_type,
    max_lgc.segmentid AS record_id,
    FORMAT(
        '[CSCL-LOG-LION-17] [LGC DATA ERROR] More than 9 LGC values were found for the {} '
        || 'feature with a GlobalID = {}.  {} values were found for the record.',
        COALESCE(source_table::VARCHAR, ''),
        COALESCE(globalid::VARCHAR, ''),
        COALESCE(lgc_count::VARCHAR, '')
    ) AS message
FROM max_lgc
INNER JOIN segments ON max_lgc.segmentid = segments.segmentid
