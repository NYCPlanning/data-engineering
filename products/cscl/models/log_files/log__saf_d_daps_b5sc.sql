-- SAF D/F AltSegmentData rows (DAPS) whose B5SC isn't one of prod's hard-coded DAPS street
-- codes (seed saf_d_hardcoded_regular_b5sc, the cases of SAFExtractClass.cs:773-833).
-- Prod's switch runs for type F rows too, so an F row with another street code is left
-- out as well; int__saf_d drops them. Rows with no centerline are logged as CSCL-LOG-SAF-15
-- instead.
WITH altsegdata AS (
    SELECT * FROM {{ ref('stg__altsegmentdata_saf') }}
    WHERE saftype IN ('D', 'F')
),
centerline AS (
    SELECT DISTINCT segmentid FROM {{ source('recipe_sources', 'dcp_cscl_centerline') }}
),
daps AS (
    SELECT daps_b5sc FROM {{ ref('saf_d_hardcoded_regular_b5sc') }}
)
SELECT
    'warning' AS log_level,
    'saf d unrecognized daps street code' AS error_category,
    'CSCL-LOG-SAF-14' AS log_message_id,
    altsegdata.globalid,
    'altsegmentdata' AS source_feature_layer,
    'segmentid' AS record_id_type,
    altsegdata.segmentid::VARCHAR AS record_id,
    {{ log_message(
        "AltSegmentData SAF type D entry contains unrecognized DAPS B5SC ({}).  The SAF "
        "output record not written to the output file.",
        ["altsegdata.b5sc"]
    ) }} AS message
FROM altsegdata
INNER JOIN centerline ON altsegdata.segmentid = centerline.segmentid
LEFT JOIN daps ON TRY_CAST(altsegdata.b5sc AS INT) = daps.daps_b5sc
WHERE daps.daps_b5sc IS NULL
