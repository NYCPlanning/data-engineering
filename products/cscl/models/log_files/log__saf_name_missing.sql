-- G/N/X CommonPlaces left out of SAF because their B7SC has no principal name in the
-- FeatureName or StreetName table (int__saf_gnx drops them). Prod logs these as "[NAME
-- MISSING] The SAF record with an ObjectID = N is missing a name". Records with an invalid
-- B7SC are logged by log__saf_invalid_b7sc instead, and SECURITY_LEVEL 3 places are left
-- out of SAF regardless.
WITH saf AS (
    SELECT DISTINCT saf_globalid
    FROM {{ ref('int__saf_segments') }}
    WHERE saf_source_table = 'commonplace' AND saftype IN ('G', 'N', 'X') AND security_level <> 3
),
commonplace AS (
    SELECT * FROM {{ source('recipe_sources', 'dcp_cscl_commonplace_gdb') }}
),
names AS (
    SELECT DISTINCT b7sc FROM {{ ref('stg__facecode_and_featurename_principal') }}
)
SELECT
    'error' AS log_level,
    'saf record missing name' AS error_category,
    commonplace.globalid,
    'commonplace' AS source_feature_layer,
    'placeid' AS record_id_type,
    commonplace.placeid AS record_id,
    FORMAT(
        '[NAME MISSING] The SAF record with B7SC = ''{}'' is missing a name.  '
        || 'The SAF record will not be included in the output file.',
        commonplace.b7sc
    ) AS message
FROM saf
INNER JOIN commonplace ON saf.saf_globalid = commonplace.globalid
LEFT JOIN names ON commonplace.b7sc = names.b7sc
WHERE
    names.b7sc IS NULL
    AND LENGTH(TRIM(commonplace.b7sc)) >= 8
