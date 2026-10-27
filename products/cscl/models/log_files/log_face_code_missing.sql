-- Attachment to log.csv, exported as log_face_code_missing.csv: one row per principal
-- StreetName row with a blank FACECODE, which the borough Face files leave out
-- (face_code_by_field keeps only rows with a face code). Prod (FCExtractClass) logs each
-- as CSCL-LOG-FC-01; there are thousands, so log.csv carries one count per borough
-- (log__face_code_missing) and the records live here, in prod's wording. Prod reads rows
-- borough by borough (B10SC LIKE '<borough>%'); FeatureName rows are FC-03/04.
WITH streetname AS (
    SELECT
        globalid,
        streetnameid,
        b10sc,
        b7sc,
        lookup_key,
        CASE LEFT(b10sc, 1)
            WHEN '1' THEN 'Manhattan'
            WHEN '2' THEN 'Bronx'
            WHEN '3' THEN 'Brooklyn'
            WHEN '4' THEN 'Queens'
            WHEN '5' THEN 'StatenIsland'
        END AS borough_name
    FROM {{ source('recipe_sources', 'dcp_cscl_streetname') }}
    WHERE
        principal_flag = 'Y'
        AND (facecode IS NULL OR facecode = '')
        AND LEFT(b10sc, 1) IN ('1', '2', '3', '4', '5')
)
SELECT
    'error' AS log_level,
    'face code missing' AS error_category,
    'CSCL-LOG-FC-01' AS log_message_id,
    log_messages.effect,
    streetname.globalid,
    'streetname' AS source_feature_layer,
    'streetnameid' AS record_id_type,
    streetname.streetnameid AS record_id,
    streetname.borough_name,
    streetname.b10sc,
    streetname.b7sc,
    streetname.lookup_key,
    '[CSCL-LOG-FC-01] ' || {{ log_message(
        "Failed to add [FC_{}] record from [StreetName] table with STREETNAMEID = [{}] "
        "and B10SC = [{}] as FACECODE is NULL or EMPTY",
        ["streetname.borough_name", "streetname.streetnameid", "streetname.b10sc"]
    ) }} AS message
FROM streetname
LEFT JOIN {{ ref('log_messages') }} AS log_messages ON log_messages.id = 'CSCL-LOG-FC-01'
ORDER BY streetname.b10sc, streetname.streetnameid
