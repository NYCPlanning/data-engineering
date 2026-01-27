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
    segments.globalid,
    segments.source_table AS source_feature_layer,
    'segmentid' AS record_id_type,
    segmentid AS record_id,
    FORMAT(
        '[LGC DATA ERROR] More than 9 LGC values were found for the "%s" '
        || 'feature with an OID = "%s" and segment id "%s". '
        || '"%s" values were found for the record.',
        source_table,
        globalid,
        segmentid::INT,
        lgc_count
    ) AS message
FROM max_lgc
INNER JOIN segments ON max_lgc.segmentid = segments.segmentid
