-- Principal StreetName (CSCL-LOG-FC-01) and FeatureName (CSCL-LOG-FC-03) rows with a blank
-- FACECODE, which the borough Face files leave out. Prod logs every one; there are
-- thousands, so log.csv gets one row per borough and table with the count, and the
-- records go to log_face_code_missing.csv (log_face_code_missing). A row whose B7SC is
-- some segment's preferred LGC means a segment loses its face code (CSCL-LOG-LION-18 logs
-- that segment): prod logs it again as CSCL-LOG-FC-02 (StreetName) or CSCL-LOG-FC-04
-- (FeatureName), which stay one row per record here.
WITH missing AS (
    SELECT * FROM {{ ref('log_face_code_missing') }}
),
preferred_b7sc AS (
    SELECT DISTINCT b7sc
    FROM {{ source('recipe_sources', 'dcp_cscl_segment_lgc') }}
    WHERE LOWER(TRIM(preferred_lgc_flag)) = 'y'
)
SELECT
    'error' AS log_level,
    'face code missing' AS error_category,
    log_message_id,
    NULL AS globalid,
    source_feature_layer,
    NULL AS record_id_type,
    NULL AS record_id,
    {{ log_message(
        "{} principal {} rows in {} have no FACECODE and are left out of the "
        "Face file; see log_face_code_missing.csv",
        [
            "COUNT(*)",
            "CASE source_feature_layer WHEN 'streetname' THEN 'StreetName' ELSE 'FeatureName' END",
            "borough_name",
        ]
    ) }} AS message,
    COUNT(*) AS record_count,
    borough_name || 'Face.txt' AS output_filename
FROM missing
GROUP BY log_message_id, source_feature_layer, borough_name
UNION ALL
SELECT
    'error' AS log_level,
    'face code missing' AS error_category,
    CASE missing.log_message_id WHEN 'CSCL-LOG-FC-01' THEN 'CSCL-LOG-FC-02' ELSE 'CSCL-LOG-FC-04' END
        AS log_message_id,
    missing.globalid,
    missing.source_feature_layer,
    missing.record_id_type,
    missing.record_id,
    {{ log_message(
        "Missing FaceCode, Failed to add [FC_{}] record from [{}] table with "
        "{} = [{}] and B10SC = [{}] as FACECODE is NULL or EMPTY",
        [
            "missing.borough_name",
            "CASE missing.source_feature_layer WHEN 'streetname' THEN 'StreetName' ELSE 'FeatureName' END",
            "UPPER(missing.record_id_type)",
            "missing.record_id",
            "missing.b10sc",
        ]
    ) }} AS message,
    NULL AS record_count,
    NULL AS output_filename
FROM missing
INNER JOIN preferred_b7sc ON missing.b7sc = preferred_b7sc.b7sc
