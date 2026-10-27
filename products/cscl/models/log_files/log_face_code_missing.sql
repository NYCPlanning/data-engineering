-- Attachment to log.csv, exported as log_face_code_missing.csv: one row per principal
-- StreetName (CSCL-LOG-FC-01) or FeatureName (CSCL-LOG-FC-03) row with a blank FACECODE,
-- which the borough Face files leave out (face_code_by_field keeps only rows with a face
-- code). Prod (FCExtractClass) logs each one; there are thousands, so log.csv carries one
-- count per borough and id (log__face_code_missing) and the records live here, in prod's
-- wording. Prod reads rows borough by borough (B10SC LIKE '<borough>%').
WITH names AS (
    SELECT
        'CSCL-LOG-FC-01' AS log_message_id,
        'streetname' AS source_feature_layer,
        'streetnameid' AS record_id_type,
        'StreetName' AS table_name,
        'STREETNAMEID' AS id_name,
        globalid,
        streetnameid AS record_id,
        b10sc,
        b7sc,
        lookup_key,
        principal_flag,
        facecode
    FROM {{ source('recipe_sources', 'dcp_cscl_streetname') }}
    UNION ALL
    SELECT
        'CSCL-LOG-FC-03' AS log_message_id,
        'featurename' AS source_feature_layer,
        'featurenameid' AS record_id_type,
        'FeatureName' AS table_name,
        'FEATURENAMEID' AS id_name,
        globalid,
        featurenameid AS record_id,
        b10sc,
        b7sc,
        lookup_key,
        principal_flag,
        facecode
    FROM {{ source('recipe_sources', 'dcp_cscl_featurename') }}
),
missing AS (
    SELECT
        *,
        CASE LEFT(b10sc, 1)
            WHEN '1' THEN 'Manhattan'
            WHEN '2' THEN 'Bronx'
            WHEN '3' THEN 'Brooklyn'
            WHEN '4' THEN 'Queens'
            WHEN '5' THEN 'StatenIsland'
        END AS borough_name
    FROM names
    WHERE
        principal_flag = 'Y'
        AND (facecode IS NULL OR facecode = '')
        AND LEFT(b10sc, 1) IN ('1', '2', '3', '4', '5')
)
SELECT
    'error' AS log_level,
    'face code missing' AS error_category,
    missing.log_message_id,
    log_messages.effect,
    missing.globalid,
    missing.source_feature_layer,
    missing.record_id_type,
    missing.record_id::VARCHAR AS record_id,
    missing.borough_name,
    missing.b10sc,
    missing.b7sc,
    missing.lookup_key,
    '[' || missing.log_message_id || '] ' || {{ log_message(
        "Failed to add [FC_{}] record from [{}] table with {} = [{}] "
        "and B10SC = [{}] as FACECODE is NULL or EMPTY",
        [
            "missing.borough_name", "missing.table_name", "missing.id_name",
            "missing.record_id", "missing.b10sc",
        ]
    ) }} AS message
FROM missing
LEFT JOIN {{ ref('log_messages') }} AS log_messages ON missing.log_message_id = log_messages.id
ORDER BY missing.log_message_id, missing.b10sc, missing.record_id
