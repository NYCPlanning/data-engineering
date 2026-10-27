-- G/N/X CommonPlaces prod leaves out of SAF while looking up their face code and sequence
-- number (GetSAFGNXFaceCodeSeqNum, SAFExtractClass.cs:2021-2105). On a centerline with a
-- COINCIDENT_SEG_COUNT above 1 and at least one proto (AltSegmentData row of a type other
-- than S), prod reads the first such proto (ogc_fid order, which follows the source FGDB's
-- OBJECTID order):
--   SAF-26 (:2071): a type B proto, but neither the centerline nor the proto is in the
--     place's BOROUGHCODE (IsGeometryBasedSegment / IsProtoBasedSegment).
--   SAF-27 (:2098): a proto of a type other than B or R.
-- Only places prod gets that far with: a valid B7SC (log__saf_invalid_b7sc) on an existing
-- centerline (log__saf_segment_not_found). int__saf_gnx keeps these places.
WITH places AS (
    SELECT * FROM {{ source('recipe_sources', 'dcp_cscl_commonplace_gdb') }}
    WHERE
        saftype IN ('G', 'N', 'X')
        AND LENGTH(TRIM(b7sc)) >= 8
        AND SUBSTRING(b7sc, 2, 7) <> '0000000'
),
centerline AS (
    SELECT * FROM {{ source('recipe_sources', 'dcp_cscl_centerline') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY segmentid ORDER BY ogc_fid) = 1
),
first_proto AS (
    SELECT
        segmentid,
        UPPER(alt_segdata_type) AS alt_segdata_type,
        UPPER(TRIM(borough)) AS borough
    FROM {{ source('recipe_sources', 'dcp_cscl_altsegmentdata') }}
    WHERE alt_segdata_type <> 'S'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY segmentid ORDER BY ogc_fid) = 1
),
failed AS (
    SELECT
        places.globalid,
        places.placeid,
        places.segmentid,
        first_proto.alt_segdata_type,
        CASE
            WHEN
                first_proto.alt_segdata_type = 'B'
                AND COALESCE(UPPER(TRIM(centerline.boroughcode)), '') <> UPPER(TRIM(places.boroughcode))
                AND COALESCE(first_proto.borough, '') <> UPPER(TRIM(places.boroughcode))
                THEN 'CSCL-LOG-SAF-26'
            WHEN first_proto.alt_segdata_type NOT IN ('B', 'R') THEN 'CSCL-LOG-SAF-27'
        END AS log_message_id
    FROM places
    INNER JOIN centerline ON places.segmentid = centerline.segmentid
    INNER JOIN first_proto ON places.segmentid = first_proto.segmentid
    WHERE TRY_CAST(centerline.coincident_seg_count AS INT) > 1
)
SELECT
    'error' AS log_level,
    'saf place segment not matched' AS error_category,
    log_message_id,
    globalid,
    'commonplace' AS source_feature_layer,
    'placeid' AS record_id_type,
    placeid::VARCHAR AS record_id,
    CASE log_message_id
        WHEN 'CSCL-LOG-SAF-26'
            THEN {{ log_message(
                "Failed to add [SAFA] record from [CommonPlace] featureclass with SEGMENTID = [{}] "
                "and PLACEID = [{}] as it is neither Geometry based nor Proto based",
                ["segmentid", "placeid"]
            ) }}
        ELSE {{ log_message(
            "Failed to add [SAFA] record from [CommonPlace] featureclass with SEGMENTID = [{}] "
            "and PLACEID = [{}] as ALT_SEGDATA_TYPE = '{}'",
            ["segmentid", "placeid", "alt_segdata_type"]
        ) }}
    END AS message
FROM failed
WHERE log_message_id IS NOT NULL
