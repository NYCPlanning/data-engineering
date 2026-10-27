-- Special SEDAT rows whose B7SC has no principal row in the StreetName or FeatureName table
-- (SpecialSEDATExtractFile.cs:280-304). Prod leaves the street name blank and then skips
-- the row (IsValidFaceCode, :158-164); special_sedat_fields drops rows with no street name
-- the same way. Prod writes the row's OBJECTID; we write its GlobalID.
WITH special_sedat AS (
    SELECT * FROM {{ source('recipe_sources', 'dcp_cscl_specialsedat') }}
),
names AS (
    SELECT DISTINCT b7sc FROM {{ ref('stg__facecode_and_featurename_principal') }}
)
SELECT
    'error' AS log_level,
    'special sedat name missing' AS error_category,
    'CSCL-LOG-SSEDAT-01' AS log_message_id,
    special_sedat.globalid,
    'specialsedat' AS source_feature_layer,
    'segmentid' AS record_id_type,
    special_sedat.segmentid::VARCHAR AS record_id,
    {{ log_message(
        "[NAME TABLE ERROR] The name for the SpecialSEDAT row with a GlobalID = {} was not "
        "found in the StreetName table or the Featurename table.  The record will not be "
        "included in the output file.",
        ["special_sedat.globalid"]
    ) }} AS message
FROM special_sedat
LEFT JOIN names ON special_sedat.b7sc = names.b7sc
WHERE names.b7sc IS NULL
