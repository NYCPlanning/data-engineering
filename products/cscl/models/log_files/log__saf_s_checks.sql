-- SAF S address points (AddressPoint, SPECIAL_CONDITION 'S') prod leaves out of SAF after
-- finding their centerline, in the order it checks them (SAFExtractClass.cs:335-463); a
-- point is logged for the first check it fails only. A point with no centerline is
-- CSCL-LOG-SAF-12 (log__saf_segment_not_found).
--   SAF-06 (:341-356): no face code for B7SC_ACTUAL in StreetName (first principal row;
--     GetFaceCode pads a blank one to '0000', CSCLBowneHelperMethods.cs:1303), nor for
--     the segment's preferred B7SC (CSCLDataHelpersClass.GetSegmentB7SC).
--   SAF-07..10 (:366-416): the borough of B7SC_ACTUAL differs from that of the segment's
--     first Segment_LGC row (GetB5SC reads SEGMENTID's rows unsorted and takes the first,
--     CSCLBowneHelperMethods.cs:1177), so the point must apply to a proto: the first
--     AltSegmentData row with its SEGMENTID (any ALT_SEGDATA_TYPE, SAF rows included;
--     ArcObjectsHelpersClass.GetRow) must exist (07), be type B (08), be in the point's
--     BOROUGHCODE (09), and have B5SC + LGC1 = B7SC_ACTUAL (10).
--   SAF-11 (:425-463, ConvertToLIONAddressFormat at CSCLBowneHelperMethods.cs:2032): on a
--     street of SND feature type E, neither the house number nor its suffix ends in a
--     section letter A-E. Prod logs this at INFO, so it never reaches its log; the other
--     ConvertToLIONAddressFormat messages change nothing and aren't logged here. Type F
--     streets keep the record.
-- "First" is ogc_fid order, which follows the source FGDB's OBJECTID order. Prod writes the
-- AddressPoint OBJECTID into each message; we write the ADDRESSPOINTID.
WITH centerline AS (
    SELECT DISTINCT segmentid FROM {{ source('recipe_sources', 'dcp_cscl_centerline') }}
),
s_points AS (
    SELECT ap.*
    FROM {{ source('recipe_sources', 'dcp_cscl_addresspoints') }} AS ap
    INNER JOIN centerline ON ap.segmentid = centerline.segmentid
    WHERE ap.special_condition = 'S'
),
principal_street AS (
    SELECT
        b7sc,
        CASE WHEN LPAD(COALESCE(facecode, ''), 4, '0') <> '0000' THEN facecode END AS face_code,
        LOWER(TRIM(snd_feature_type)) AS snd_feature_type
    FROM {{ source('recipe_sources', 'dcp_cscl_streetname') }}
    WHERE principal_flag = 'Y'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY b7sc ORDER BY ogc_fid) = 1
),
segment_lgc AS (
    SELECT * FROM {{ source('recipe_sources', 'dcp_cscl_segment_lgc') }}
),
first_lgc AS (
    SELECT
        segmentid,
        TRIM(b5sc) AS b5sc
    FROM segment_lgc
    QUALIFY ROW_NUMBER() OVER (PARTITION BY segmentid ORDER BY ogc_fid) = 1
),
preferred_lgc AS (
    SELECT
        segmentid,
        b7sc
    FROM segment_lgc
    WHERE preferred_lgc_flag = 'Y'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY segmentid ORDER BY ogc_fid) = 1
),
first_altsegdata AS (
    SELECT
        segmentid,
        COALESCE(TRIM(alt_segdata_type), '') AS alt_segdata_type,
        COALESCE(TRIM(borough), '') AS borough,
        TRIM(COALESCE(b5sc, '') || COALESCE(lgc1, '')) AS b7sc
    FROM {{ source('recipe_sources', 'dcp_cscl_altsegmentdata') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY segmentid ORDER BY ogc_fid) = 1
),
checked AS (
    SELECT
        s_points.globalid,
        s_points.addresspointid,
        s_points.segmentid,
        s_points.b7sc_actual,
        TRIM(s_points.boroughcode) AS boroughcode,
        actual_street.face_code IS NULL AND segment_street.face_code IS NULL AS no_face_code,
        LEFT(s_points.b7sc_actual, 1) <> LEFT(first_lgc.b5sc, 1) AS other_borough,
        first_altsegdata.segmentid IS NOT NULL AS has_proto,
        first_altsegdata.alt_segdata_type AS proto_type,
        first_altsegdata.borough AS proto_borough,
        first_altsegdata.b7sc AS proto_b7sc,
        actual_street.snd_feature_type = 'e'
        AND TRIM(
            CASE
                WHEN LOWER(TRIM(s_points.hyphen_type)) = 'r' THEN SPLIT_PART(s_points.house_number, '-', 1)
                ELSE COALESCE(s_points.house_number, '')
            END
        ) <> ''
        AND TRIM(COALESCE(s_points.house_number_suffix, '')) <> ''
        AND LOWER(RIGHT(TRIM(
            CASE
                WHEN LOWER(TRIM(s_points.hyphen_type)) = 'r' THEN SPLIT_PART(s_points.house_number, '-', 1)
                ELSE s_points.house_number
            END
        ), 1)) NOT IN ('a', 'b', 'c', 'd', 'e')
        AND LOWER(RIGHT(TRIM(s_points.house_number_suffix), 1)) NOT IN ('a', 'b', 'c', 'd', 'e')
            AS no_section_letter
    FROM s_points
    LEFT JOIN principal_street AS actual_street ON s_points.b7sc_actual = actual_street.b7sc
    LEFT JOIN preferred_lgc ON s_points.segmentid = preferred_lgc.segmentid
    LEFT JOIN principal_street AS segment_street ON preferred_lgc.b7sc = segment_street.b7sc
    LEFT JOIN first_lgc ON s_points.segmentid = first_lgc.segmentid
    LEFT JOIN first_altsegdata ON s_points.segmentid = first_altsegdata.segmentid
),
failed AS (
    SELECT
        *,
        CASE
            WHEN no_face_code THEN 'CSCL-LOG-SAF-06'
            WHEN other_borough AND NOT has_proto THEN 'CSCL-LOG-SAF-07'
            WHEN other_borough AND LOWER(proto_type) <> 'b' THEN 'CSCL-LOG-SAF-08'
            WHEN other_borough AND proto_borough <> boroughcode THEN 'CSCL-LOG-SAF-09'
            WHEN other_borough AND LOWER(proto_b7sc) <> LOWER(TRIM(b7sc_actual)) THEN 'CSCL-LOG-SAF-10'
            WHEN no_section_letter THEN 'CSCL-LOG-SAF-11'
        END AS log_message_id
    FROM checked
)
SELECT
    CASE log_message_id WHEN 'CSCL-LOG-SAF-11' THEN 'info' ELSE 'error' END AS log_level,
    'saf s record not written' AS error_category,
    log_message_id,
    globalid,
    'addresspoints' AS source_feature_layer,
    'addresspointid' AS record_id_type,
    addresspointid::VARCHAR AS record_id,
    CASE log_message_id
        WHEN 'CSCL-LOG-SAF-06'
            THEN {{ log_message(
                "[SAF S: NO FACE CODE]  No FaceCode found using the query 'B7SC = '{}' AND "
                "PRINCIPAL_FLAG = 'Y'' in the StreetName table.  The AddressPoint feature with "
                "an AddressPointID = {} will not be written to the output file.",
                ["b7sc_actual", "addresspointid"]
            ) }}
        WHEN 'CSCL-LOG-SAF-07'
            THEN {{ log_message(
                "[SAF S: MISSING PROTO SEGMENT] The AltSegmentData does not contain a matching "
                "record using the query 'SegmentID = {}'.  The AddressPoint feature with an "
                "AddressPointID = {} will not be written to the output file.",
                ["segmentid", "addresspointid"]
            ) }}
        WHEN 'CSCL-LOG-SAF-08'
            THEN {{ log_message(
                "[SAF S: INCORRECT ALT_SEGDATA_TYPE] The matching proto segment does not have "
                "the correct Alt_SegData_Type, expected 'B' but found '{}.  The AddressPoint "
                "feature with an AddressPointID = {} will not be written to the output file.",
                ["UPPER(proto_type)", "addresspointid"]
            ) }}
        WHEN 'CSCL-LOG-SAF-09'
            THEN {{ log_message(
                "[SAF S: INCORRECT BOROUGH] The matching proto segment does not have the "
                "correct Borough, expected '{}' but found '{}.  The AddressPoint feature with "
                "an AddressPointID = {} will not be written to the output file.",
                ["proto_borough", "boroughcode", "addresspointid"]
            ) }}
        WHEN 'CSCL-LOG-SAF-10'
            THEN {{ log_message(
                "[SAF S: INCORRECT B7SC] The matching proto segment does not have the correct "
                "B7SC, expected '{}' but found '{}.  The AddressPoint feature with an "
                "AddressPointID = {} will not be written to the output file.",
                ["proto_b7sc", "TRIM(b7sc_actual)", "addresspointid"]
            ) }}
        ELSE {{ log_message(
            "Neither the house number or suffix values contain the section letter, unable to "
            "convert to the LION address format.  The SAF row with an AddressPointID = {} will "
            "not be written to the output file.",
            ["addresspointid"]
        ) }}
    END AS message
FROM failed
WHERE log_message_id IS NOT NULL
