-- SAF records whose SEGMENTID matches no segment. Prod looks each record's segment up by
-- SEGMENTID (A/B/C/E/P, D/F and O AltSegmentData rows and S/V AddressPoints in Centerline
-- only; G/N/X CommonPlaces in Centerline, Shoreline, Rail and Subway); when it finds none
-- it logs the record and leaves it out of SAF (CSCL-LOG-SAF-21, CSCL-LOG-SAF-25; S, D/F, V
-- and O: CSCL-LOG-SAF-12, -15, -16, -18). A CommonPlace whose B7SC prod rejects first
-- (shorter than 8 characters, or 7 zeros after the borough) never gets that far, and prod
-- reads only V points with a HYPHEN_TYPE other than U (a NULL one included in neither).
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
),
altsegdata AS (
    SELECT
        CASE
            WHEN saftype IN ('A', 'B', 'C', 'E', 'P') THEN 'CSCL-LOG-SAF-21'
            WHEN saftype IN ('D', 'F') THEN 'CSCL-LOG-SAF-15'
            WHEN saftype = 'O' THEN 'CSCL-LOG-SAF-18'
        END AS log_message_id,
        globalid,
        segmentid,
        lionkey
    FROM {{ ref('stg__altsegmentdata_saf') }}
    WHERE saftype IN ('A', 'B', 'C', 'E', 'P', 'D', 'F', 'O')
),
address_points AS (
    SELECT
        CASE special_condition WHEN 'S' THEN 'CSCL-LOG-SAF-12' ELSE 'CSCL-LOG-SAF-16' END AS log_message_id,
        globalid,
        segmentid,
        addresspointid
    FROM {{ source('recipe_sources', 'dcp_cscl_addresspoints') }}
    WHERE special_condition = 'S' OR (special_condition = 'V' AND hyphen_type <> 'U')
)
SELECT
    altsegdata.log_message_id,
    altsegdata.globalid,
    'altsegmentdata' AS source_table,
    altsegdata.segmentid,
    altsegdata.lionkey,
    NULL::VARCHAR AS placeid,
    NULL::VARCHAR AS addresspointid
FROM altsegdata
ANTI JOIN centerline ON altsegdata.segmentid = centerline.segmentid
UNION ALL
SELECT
    address_points.log_message_id,
    address_points.globalid,
    'addresspoints' AS source_table,
    address_points.segmentid,
    NULL AS lionkey,
    NULL::VARCHAR AS placeid,
    address_points.addresspointid::VARCHAR AS addresspointid
FROM address_points
ANTI JOIN centerline ON address_points.segmentid = centerline.segmentid
UNION ALL
SELECT
    'CSCL-LOG-SAF-25' AS log_message_id,
    commonplace.globalid,
    'commonplace' AS source_table,
    commonplace.segmentid,
    NULL AS lionkey,
    commonplace.placeid::VARCHAR AS placeid,
    NULL::VARCHAR AS addresspointid
FROM {{ source('recipe_sources', 'dcp_cscl_commonplace_gdb') }} AS commonplace
ANTI JOIN segments ON commonplace.segmentid = segments.segmentid
WHERE
    commonplace.saftype IN ('G', 'N', 'X')
    AND LENGTH(TRIM(commonplace.b7sc)) >= 8
    AND SUBSTRING(commonplace.b7sc, 2, 7) <> '0000000'
