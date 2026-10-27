-- SAF records whose SEGMENTID matches no segment. Prod looks each record's segment up by
-- SEGMENTID (A/B/C/E/P AltSegmentData rows in Centerline only; G/N/X CommonPlaces in
-- Centerline, Shoreline, Rail and Subway); when it finds none it logs the record and leaves
-- it out of SAF (CSCL-LOG-SAF-21, CSCL-LOG-SAF-25). A CommonPlace whose B7SC prod rejects
-- first (shorter than 8 characters, or 7 zeros after the borough) never gets that far.
WITH centerline AS (
    SELECT segmentid FROM {{ source('recipe_sources', 'dcp_cscl_centerline') }}
),
segments AS (
    SELECT segmentid FROM centerline
    UNION
    SELECT segmentid FROM {{ source('recipe_sources', 'dcp_cscl_shoreline') }}
    UNION
    SELECT segmentid FROM {{ source('recipe_sources', 'dcp_cscl_rail') }}
    UNION
    SELECT segmentid FROM {{ source('recipe_sources', 'dcp_cscl_subway') }}
)
SELECT
    'CSCL-LOG-SAF-21' AS log_message_id,
    altsegdata.globalid,
    'altsegmentdata' AS source_table,
    altsegdata.segmentid,
    altsegdata.lionkey,
    NULL::VARCHAR AS placeid
FROM {{ ref('stg__altsegmentdata_saf') }} AS altsegdata
ANTI JOIN centerline ON altsegdata.segmentid = centerline.segmentid
WHERE altsegdata.saftype IN ('A', 'B', 'C', 'E', 'P')
UNION ALL
SELECT
    'CSCL-LOG-SAF-25' AS log_message_id,
    commonplace.globalid,
    'commonplace' AS source_table,
    commonplace.segmentid,
    NULL AS lionkey,
    commonplace.placeid::VARCHAR AS placeid
FROM {{ source('recipe_sources', 'dcp_cscl_commonplace_gdb') }} AS commonplace
ANTI JOIN segments ON commonplace.segmentid = segments.segmentid
WHERE
    commonplace.saftype IN ('G', 'N', 'X')
    AND LENGTH(TRIM(commonplace.b7sc)) >= 8
    AND SUBSTRING(commonplace.b7sc, 2, 7) <> '0000000'
