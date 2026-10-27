-- StreetName and FeatureName rows SND, Enders or the Exception file leave out because their
-- LOOKUP_KEY is blank, or (Exception) their name is shorter than 2 characters. Each prod
-- extract reads both tables with its own query and checks the key its own way:
--   SND (CSCL-LOG-SND-01 StreetName, -02 FeatureName): ENDERS_FLAG not 'Y' and LOOKUP_KEY
--     IS NOT NULL in the query, so only an empty or all-space key is logged; a NULL one
--     is skipped silently.
--   Enders (CSCL-LOG-END-01, -02): ENDERS_FLAG = 'Y', NULL or empty key (spaces pass).
--   Exception (CSCL-LOG-EXC-02, -04): EXCEPTION_FLAG = 'Y', ENDERS_FLAG 'N' or NULL, NULL,
--     empty or all-space key; then (CSCL-LOG-EXC-01, -03) a name of fewer than 2
--     characters once put in sort format (ExceptionExtractClass.cs:150-160, :206-219).
WITH names AS (
    SELECT
        names.*,
        COALESCE(streetname.streetnameid, featurename.featurenameid)::VARCHAR AS record_id
    FROM {{ ref('stg__facecode_and_featurename') }} AS names
    LEFT JOIN {{ source('recipe_sources', 'dcp_cscl_streetname') }} AS streetname
        ON names.feature_type = 'street' AND names.globalid = streetname.globalid
    LEFT JOIN {{ source('recipe_sources', 'dcp_cscl_featurename') }} AS featurename
        ON names.feature_type = 'feature' AND names.globalid = featurename.globalid
),
blank AS (
    SELECT
        CASE feature_type WHEN 'street' THEN 'CSCL-LOG-SND-01' ELSE 'CSCL-LOG-SND-02' END AS log_message_id,
        'SND' AS extract_name,
        feature_type,
        globalid,
        record_id
    FROM names
    WHERE
        enders_flag IS DISTINCT FROM 'Y'
        AND lookup_key IS NOT NULL
        AND TRIM(lookup_key) = ''
    UNION ALL
    SELECT
        CASE feature_type WHEN 'street' THEN 'CSCL-LOG-END-01' ELSE 'CSCL-LOG-END-02' END AS log_message_id,
        'ENDERS' AS extract_name,
        feature_type,
        globalid,
        record_id
    FROM names
    WHERE
        enders_flag = 'Y'
        AND (lookup_key IS NULL OR lookup_key = '')
    UNION ALL
    SELECT
        CASE
            WHEN TRIM(COALESCE(lookup_key, '')) = ''
                THEN CASE feature_type WHEN 'street' THEN 'CSCL-LOG-EXC-02' ELSE 'CSCL-LOG-EXC-04' END
            ELSE CASE feature_type WHEN 'street' THEN 'CSCL-LOG-EXC-01' ELSE 'CSCL-LOG-EXC-03' END
        END AS log_message_id,
        'EXCEPTION' AS extract_name,
        feature_type,
        globalid,
        record_id
    FROM names
    WHERE
        exception_flag = 'Y'
        AND (enders_flag = 'N' OR enders_flag IS NULL)
        AND (
            TRIM(COALESCE(lookup_key, '')) = ''
            OR LENGTH(TRIM(COALESCE(place_name_sort_order, ''))) < 2
        )
)
SELECT
    'error' AS log_level,
    CASE
        WHEN log_message_id IN ('CSCL-LOG-EXC-01', 'CSCL-LOG-EXC-03') THEN 'name too short'
        ELSE 'lookup key missing'
    END AS error_category,
    log_message_id,
    globalid,
    CASE feature_type WHEN 'street' THEN 'streetname' ELSE 'featurename' END AS source_feature_layer,
    CASE feature_type WHEN 'street' THEN 'streetnameid' ELSE 'featurenameid' END AS record_id_type,
    record_id,
    CASE
        WHEN log_message_id IN ('CSCL-LOG-EXC-01', 'CSCL-LOG-EXC-03')
            THEN {{ log_message(
                "Failed to add EXCEPTION record from [{}] table with {} = [{}] as Name entry "
                "less than 2 characters long",
                [
                    "CASE feature_type WHEN 'street' THEN 'StreetName' ELSE 'FeatureName' END",
                    "CASE feature_type WHEN 'street' THEN 'STREETNAMEID' ELSE 'FEATURENAMEID' END",
                    "record_id",
                ]
            ) }}
        WHEN log_message_id IN ('CSCL-LOG-EXC-02', 'CSCL-LOG-EXC-04')
            THEN {{ log_message(
                "Failed to add EXCEPTION record from [{}] table with {} = [{}] as LOOKUP_KEY is "
                "NULL or EMPTY",
                [
                    "CASE feature_type WHEN 'street' THEN 'StreetName' ELSE 'FeatureName' END",
                    "CASE feature_type WHEN 'street' THEN 'STREETNAMEID' ELSE 'FEATURENAMEID' END",
                    "record_id",
                ]
            ) }}
        ELSE {{ log_message(
            "Failed to add [{}] record from [{}] table with {} = [{}] as LOOKUP_KEY is NULL or EMPTY",
            [
                "extract_name",
                "CASE feature_type WHEN 'street' THEN 'StreetName' ELSE 'FeatureName' END",
                "CASE feature_type WHEN 'street' THEN 'STREETNAMEID' ELSE 'FEATURENAMEID' END",
                "record_id",
            ]
        ) }}
    END AS message
FROM blank
