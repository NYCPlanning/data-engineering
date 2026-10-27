-- SAF records left out of SAF because their street code has no principal name in the
-- StreetName or FeatureName table. Prod logs both as "[NAME MISSING] The SAF record with an
-- ObjectID = N is missing a name"; we write the PlaceID or GlobalID instead.
--   SAF-23: G/N/X CommonPlaces whose B7SC has no name (int__saf_gnx drops them). Records
--     with an invalid B7SC are logged by log__saf_invalid_b7sc instead, and SECURITY_LEVEL
--     3 places are left out of SAF regardless.
--   SAF-20: A/B/E/P AltSegmentData rows on a centerline whose B5SC + LGC1 has no name
--     (SAFExtractClass.cs:1462-1495; type C rows are always named 75 STREET). int__saf_abcep
--     drops them.
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
),
named AS (
    SELECT DISTINCT b7sc FROM {{ ref('stg__facecode_and_featurename_principal') }}
    WHERE TRIM(COALESCE(lookup_key, '')) <> ''
),
centerline AS (
    SELECT DISTINCT segmentid FROM {{ source('recipe_sources', 'dcp_cscl_centerline') }}
),
altsegdata AS (
    SELECT * FROM {{ ref('stg__altsegmentdata_saf') }}
    WHERE saftype IN ('A', 'B', 'E', 'P')
)
SELECT
    'error' AS log_level,
    'saf record missing name' AS error_category,
    'CSCL-LOG-SAF-23' AS log_message_id,
    commonplace.globalid,
    'commonplace' AS source_feature_layer,
    'placeid' AS record_id_type,
    commonplace.placeid AS record_id,
    {{ log_message(
        "[NAME MISSING] The SAF record with a PlaceID = {} is missing a name.  "
        "The SAF record will not be included in the output file.",
        ["commonplace.placeid"]
    ) }} AS message
FROM saf
INNER JOIN commonplace ON saf.saf_globalid = commonplace.globalid
LEFT JOIN names ON commonplace.b7sc = names.b7sc
WHERE
    names.b7sc IS NULL
    AND LENGTH(TRIM(commonplace.b7sc)) >= 8
UNION ALL
SELECT
    'error' AS log_level,
    'saf record missing name' AS error_category,
    'CSCL-LOG-SAF-20' AS log_message_id,
    altsegdata.globalid,
    'altsegmentdata' AS source_feature_layer,
    'segmentid' AS record_id_type,
    altsegdata.segmentid::VARCHAR AS record_id,
    {{ log_message(
        "[NAME MISSING] The SAF record with a GlobalID = {} is missing a name.  "
        "The SAF record will not be included in the output file.",
        ["altsegdata.globalid"]
    ) }} AS message
FROM altsegdata
INNER JOIN centerline ON altsegdata.segmentid = centerline.segmentid
LEFT JOIN named ON (COALESCE(altsegdata.b5sc, '') || COALESCE(altsegdata.lgc1, '')) = named.b7sc
WHERE named.b7sc IS NULL
