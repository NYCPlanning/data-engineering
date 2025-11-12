WITH proto AS (
    SELECT * FROM {{ ref("stg__altsegmentdata_proto") }}
),
primary_segments AS (
    SELECT * FROM {{ ref("int__primary_segments") }}
),
facecode AS (
    SELECT * FROM {{ ref("int__b7sc_codes") }}
),
seqnum AS (
    SELECT * FROM {{ ref("int__nonstreetfeature_seqnum") }}
)
SELECT
    proto.borough AS boroughcode,
    CASE
        WHEN proto.feature_type = 'centerline' THEN facecode.street_facecode
        ELSE facecode.feature_facecode
    END AS face_code, -- TODO error report when null
    CASE
        WHEN proto.feature_type <> 'nonstreetfeatures' THEN proto.alt_segment_seqnum
        ELSE seqnum.segment_seqnum
    END AS segment_seqnum,
    proto.segmentid,
    proto.five_digit_street_code,
    proto.lgc1,
    proto.lgc2,
    proto.lgc3,
    proto.lgc4,
    proto.lgc5,
    proto.lgc6,
    proto.lgc7,
    proto.lgc8,
    proto.lgc9,
    proto.boe_lgc_pointer,
    primary_segments.legacy_segmentid,
    primary_segments.from_level_code, -- TODO this definitely isn't quite right
    primary_segments.to_level_code, -- TODO this definitely isn't quite right
    primary_segments.geom,
    primary_segments.midpoint,
    primary_segments.start_point,
    primary_segments.end_point,
    primary_segments.shape_length,
    proto.feature_type,
    proto.source_table
FROM proto
INNER JOIN primary_segments ON proto.segmentid = primary_segments.segmentid -- TODO error report for non-matches
LEFT JOIN facecode ON proto.b7sc = facecode.b7sc
LEFT JOIN seqnum ON proto.source_table = seqnum.source_table AND proto.ogc_fid = seqnum.unique_id
