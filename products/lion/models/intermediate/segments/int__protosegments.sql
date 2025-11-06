WITH proto AS (
    SELECT * FROM {{ ref("stg__altsegmentdata_proto") }}
),
primary_segments AS (
    SELECT * FROM {{ ref("int__segment_geometries") }}
),
facecode AS (
    SELECT * FROM {{ ref("int__b7sc_codes") }}
)
SELECT
    proto.borough AS boroughcode,
    CASE
        WHEN proto.feature_type = 'centerline' THEN facecode.street_facecode
        ELSE facecode.feature_facecode
    END AS face_code, -- TODO error report when null
    CASE
        WHEN proto.feature_type <> 'nonstreetfeatures' THEN alt_segment_seqnum
    END AS segment_seqnum,
    proto.segmentid,
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
