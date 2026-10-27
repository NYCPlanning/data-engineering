-- StreetName rows SND, Enders or the Exception file leave out because their LOOKUP_KEY is
-- blank. Each prod extract reads its own rows and checks the key its own way:
--   SND (CSCL-LOG-SND-01): ENDERS_FLAG not 'Y' and LOOKUP_KEY IS NOT NULL in the query,
--     so only an empty or all-space key is logged; a NULL one is skipped silently.
--   Enders (CSCL-LOG-END-01): ENDERS_FLAG = 'Y', NULL or empty key (spaces pass).
--   Exception (CSCL-LOG-EXC-02): EXCEPTION_FLAG = 'Y', ENDERS_FLAG 'N' or NULL, NULL,
--     empty or all-space key.
-- The FeatureName table's rows are logged under other ids (SND-02, END-02, EXC-04).
WITH streetname AS (
    SELECT * FROM {{ source('recipe_sources', 'dcp_cscl_streetname') }}
),
blank AS (
    SELECT
        'CSCL-LOG-SND-01' AS log_message_id,
        'SND' AS extract_name,
        globalid,
        streetnameid
    FROM streetname
    WHERE
        enders_flag IS DISTINCT FROM 'Y'
        AND lookup_key IS NOT NULL
        AND TRIM(lookup_key) = ''
    UNION ALL
    SELECT
        'CSCL-LOG-END-01' AS log_message_id,
        'ENDERS' AS extract_name,
        globalid,
        streetnameid
    FROM streetname
    WHERE
        enders_flag = 'Y'
        AND (lookup_key IS NULL OR lookup_key = '')
    UNION ALL
    SELECT
        'CSCL-LOG-EXC-02' AS log_message_id,
        'EXCEPTION' AS extract_name,
        globalid,
        streetnameid
    FROM streetname
    WHERE
        exception_flag = 'Y'
        AND (enders_flag = 'N' OR enders_flag IS NULL)
        AND TRIM(COALESCE(lookup_key, '')) = ''
)
SELECT
    'error' AS log_level,
    'lookup key missing' AS error_category,
    log_message_id,
    globalid,
    'streetname' AS source_feature_layer,
    'streetnameid' AS record_id_type,
    streetnameid AS record_id,
    CASE log_message_id
        WHEN 'CSCL-LOG-EXC-02'
            THEN {{ log_message(
                "Failed to add EXCEPTION record from [StreetName] table with "
                "STREETNAMEID = [{}] as LOOKUP_KEY is NULL or EMPTY",
                ["streetnameid"]
            ) }}
        ELSE {{ log_message(
            "Failed to add [{}] record from [StreetName] table with STREETNAMEID = [{}] "
            "as LOOKUP_KEY is NULL or EMPTY",
            ["extract_name", "streetnameid"]
        ) }}
    END AS message
FROM blank
