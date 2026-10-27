-- SEDAT rows prod writes with SEDAT3 (street name) and/or SEDAT11 (B7SC) blank
-- (SEDATExtractFile.cs:276-327, GetSpecialRowValues):
--   SEDAT-01: no SEGMENTID, so both are blank.
--   SEDAT-02: the segment has no preferred Segment_LGC row, so both are blank.
--   SEDAT-03: the B7SC prod picks has no principal StreetName row, so the name is blank.
--     With several preferred rows prod picks the one in the LIONKEY's borough, and
--     looks up B7SC = '' when none is.
-- int__sedat blanks the same fields, except that it only takes a preferred row in the
-- LIONKEY's borough, where prod takes a lone preferred row whatever its borough.
WITH sedat AS (
    SELECT * FROM {{ source('recipe_sources', 'dcp_cscl_sedat') }}
),
preferred AS (
    SELECT
        segmentid,
        b5sc || lgc AS b7sc
    FROM {{ source('recipe_sources', 'dcp_cscl_segment_lgc') }}
    WHERE preferred_lgc_flag = 'Y'
),
principal AS (
    SELECT DISTINCT b7sc
    FROM {{ source('recipe_sources', 'dcp_cscl_streetname') }}
    WHERE principal_flag = 'Y'
),
picked AS (
    SELECT
        sedat.globalid,
        sedat.segmentid,
        COUNT(preferred.b7sc) AS n_preferred,
        CASE
            WHEN COUNT(preferred.b7sc) = 1 THEN ANY_VALUE(preferred.b7sc)
            ELSE COALESCE(
                MIN(preferred.b7sc) FILTER (WHERE LEFT(preferred.b7sc, 1) = LEFT(sedat.lionkey, 1)), ''
            )
        END AS b7sc
    FROM sedat
    LEFT JOIN preferred ON sedat.segmentid = preferred.segmentid
    WHERE sedat.segmentid IS NOT NULL
    GROUP BY sedat.globalid, sedat.segmentid, sedat.lionkey
)
SELECT
    'warning' AS log_level,
    'sedat segment missing' AS error_category,
    'CSCL-LOG-SEDAT-01' AS log_message_id,
    globalid,
    'sedat' AS source_feature_layer,
    'lionkey' AS record_id_type,
    lionkey AS record_id,
    {{ log_message(
        "The SEDAT record with a GlobalID = {} does not have a SegmentID value, unable to "
        "set the output values for SEDAT3 and SEDAT11.",
        ["globalid"]
    ) }} AS message
FROM sedat
WHERE segmentid IS NULL
UNION ALL
SELECT
    'error' AS log_level,
    'sedat street code missing' AS error_category,
    'CSCL-LOG-SEDAT-02' AS log_message_id,
    globalid,
    'sedat' AS source_feature_layer,
    'segmentid' AS record_id_type,
    segmentid::VARCHAR AS record_id,
    {{ log_message(
        "Unable to locate the Preferred B7SC for the SEDAT row with a SegmentID = {}.",
        ["segmentid"]
    ) }} AS message
FROM picked
WHERE n_preferred = 0
UNION ALL
SELECT
    'error' AS log_level,
    'sedat street name missing' AS error_category,
    'CSCL-LOG-SEDAT-03' AS log_message_id,
    picked.globalid,
    'sedat' AS source_feature_layer,
    'segmentid' AS record_id_type,
    picked.segmentid::VARCHAR AS record_id,
    {{ log_message(
        "Unable to locate a matching record in the StreetName table using the query "
        "'B7SC = '{}' AND PRINCIPAL_FLAG = 'Y'.",
        ["picked.b7sc"]
    ) }} AS message
FROM picked
LEFT JOIN principal ON picked.b7sc = principal.b7sc
WHERE picked.n_preferred > 0 AND principal.b7sc IS NULL
