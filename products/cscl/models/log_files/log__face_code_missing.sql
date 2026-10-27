-- Principal StreetName rows with a blank FACECODE, which the borough Face files leave out
-- (face_code_by_field keeps only rows with a face code). Prod (FCExtractClass) logs each
-- as CSCL-LOG-FC-01, and again as CSCL-LOG-FC-02 when the row's B7SC is some segment's
-- preferred LGC, i.e. a segment will lose its face code (CSCL-LOG-LION-18 logs that
-- segment). Prod reads rows borough by borough (B10SC LIKE '<borough>%') and logs the
-- FeatureName table's under other ids (FC-03/04).
WITH streetname AS (
    SELECT
        globalid,
        streetnameid,
        b10sc,
        b7sc,
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
),
preferred_b7sc AS (
    SELECT DISTINCT b7sc
    FROM {{ source('recipe_sources', 'dcp_cscl_segment_lgc') }}
    WHERE LOWER(TRIM(preferred_lgc_flag)) = 'y'
)
SELECT
    'error' AS log_level,
    'face code missing' AS error_category,
    'CSCL-LOG-FC-01' AS log_message_id,
    globalid,
    'streetname' AS source_feature_layer,
    'streetnameid' AS record_id_type,
    streetnameid AS record_id,
    {{ log_message(
        "Failed to add [FC_{}] record from [StreetName] table with STREETNAMEID = [{}] "
        "and B10SC = [{}] as FACECODE is NULL or EMPTY",
        ["borough_name", "streetnameid", "b10sc"]
    ) }} AS message
FROM streetname
UNION ALL
SELECT
    'error' AS log_level,
    'face code missing' AS error_category,
    'CSCL-LOG-FC-02' AS log_message_id,
    streetname.globalid,
    'streetname' AS source_feature_layer,
    'streetnameid' AS record_id_type,
    streetname.streetnameid AS record_id,
    {{ log_message(
        "Missing FaceCode, Failed to add [FC_{}] record from [StreetName] table with "
        "STREETNAMEID = [{}] and B10SC = [{}] as FACECODE is NULL or EMPTY",
        ["streetname.borough_name", "streetname.streetnameid", "streetname.b10sc"]
    ) }} AS message
FROM streetname
INNER JOIN preferred_b7sc ON streetname.b7sc = preferred_b7sc.b7sc
