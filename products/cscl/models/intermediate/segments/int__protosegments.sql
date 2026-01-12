WITH proto AS (
    SELECT * FROM {{ ref("stg__altsegmentdata_proto") }}
),
primary_segments AS (
    SELECT * FROM {{ ref("int__primary_segments") }}
),
facecode AS (
    SELECT * FROM {{ ref("stg__facecode_and_featurename") }}
),
seqnum AS (
    SELECT * FROM {{ ref("int__nonstreetfeature_seqnum") }}
),
feature_type_codes AS (
    SELECT * FROM {{ ref("feature_type_codes") }}
)
SELECT
    proto.borough AS boroughcode,
    facecode.face_code, -- TODO error report when null
    CASE
        WHEN feature_type_codes.source_feature_class <> 'nonstreetfeatures' THEN proto.alt_segment_seqnum
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
    proto.feature_type_code,
    primary_segments.legacy_segmentid,
    CASE
        WHEN proto.feature_type_code IN ('5', '9') THEN 99
        WHEN proto.reversed THEN primary_segments.to_level_code
        ELSE primary_segments.from_level_code
    END AS from_level_code,
    CASE
        WHEN proto.feature_type_code IN ('5', '9') THEN 99
        WHEN proto.reversed THEN primary_segments.from_level_code
        ELSE primary_segments.to_level_code
    END AS to_level_code,
    CASE
        WHEN proto.reversed THEN ST_REVERSE(primary_segments.geom)
        ELSE primary_segments.geom
    END AS geom,
    CASE
        WHEN proto.reversed THEN ST_REVERSE(primary_segments.raw_geom)
        ELSE primary_segments.raw_geom
    END AS raw_geom,
    primary_segments.midpoint,
    CASE
        WHEN proto.reversed THEN primary_segments.end_point
        ELSE primary_segments.start_point
    END AS start_point,
    CASE
        WHEN proto.reversed THEN primary_segments.start_point
        ELSE primary_segments.end_point
    END AS end_point,
    primary_segments.shape_length,
    feature_type_codes.source_feature_class AS feature_type,
    primary_segments.feature_type AS primary_feature_type,
    feature_type_codes.description AS feature_type_description,
    proto.alt_segdata_type,
    proto.source_table,
    proto.globalid
FROM proto
INNER JOIN primary_segments ON proto.segmentid = primary_segments.segmentid -- TODO error report for non-matches
LEFT JOIN facecode ON proto.b7sc = facecode.b7sc
LEFT JOIN seqnum ON proto.globalid = seqnum.globalid
LEFT JOIN feature_type_codes ON proto.feature_type_code IS NOT DISTINCT FROM feature_type_codes.code -- NULL -> centerline
WHERE facecode.face_code IS NOT NULL -- TODO - clean up in #2073
