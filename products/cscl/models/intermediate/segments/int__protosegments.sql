WITH proto AS (
    SELECT * FROM {{ ref("stg__altsegmentdata_proto") }}
),
primary_segments AS (
    SELECT * FROM {{ ref("int__primary_segments") }}
),
facecode AS (
    SELECT * FROM {{ ref("stg__facecode_and_featurename_principal") }}
),
seqnum AS (
    SELECT * FROM {{ ref("int__nonstreetfeature_seqnum") }}
),
feature_type_codes AS (
    SELECT * FROM {{ ref("feature_type_codes") }}
),
-- CSCL-LION-01: prod writes a centerline's proto-segments one after another, in
-- AltSegmentData order (LIONExtractorClass.cs:159-176, GetProtoSegments), from one feature
-- object. Each reversed proto off the borough boundary swaps its node, XY and left/right
-- values and then reverses that object's shape in place (CenterlineExtractFile.cs:3801-3842,
-- SwapFeatureGeometry :4953), so the next proto starts from the reversed shape: after an
-- odd number of such reversals, a proto's geometry-derived values come out the other way
-- round. flips_before counts them.
proto_flips AS (
    SELECT
        globalid,
        COALESCE(SUM(CASE WHEN reversed AND alt_segdata_type IS DISTINCT FROM 'B' THEN 1 ELSE 0 END) OVER (
            PARTITION BY segmentid ORDER BY ogc_fid ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ), 0) AS flips_before
    FROM proto
),
oriented AS (
    SELECT
        proto.*,
        proto_flips.flips_before,
        primary_segments.feature_type = 'centerline' AND proto_flips.flips_before % 2 = 1 AS is_flipped_by_prod,
        proto.reversed AS geometry_reversed_fixed,
        -- prod's orientation: flipped back on a centerline proto after an odd number of
        -- reversals (only centerlines' extract reverses its shape)
        proto.reversed <> COALESCE(is_flipped_by_prod, FALSE) AS geometry_reversed_prod,
        {% if var('fix_reversed_proto_pairs') -%}
            geometry_reversed_fixed
        {%- else -%}
            geometry_reversed_prod
        {%- endif %} AS geometry_reversed,
        -- CSCL-LION-22: a centerline proto's face code comes from StreetName. Where its B5SC +
        -- LGC1 has one only in FeatureName, prod means to take that one and log a warning,
        -- but compares the table name as stored with a lowercased one
        -- (CenterlineExtractFile.cs:3487 vs :3501), looks in StreetName again, finds nothing
        -- and drops the proto (LIONExtractorClass.cs:443-458, CSCL-LOG-LION-19).
        facecode.face_code AS face_code_fixed,
        CASE
            WHEN feature_type_codes.source_feature_class = 'centerline' AND facecode.feature_type <> 'street'
                THEN NULL
            ELSE facecode.face_code
        END AS face_code_prod,
        facecode.face_code IS NOT NULL
        AND feature_type_codes.source_feature_class = 'centerline'
        AND facecode.feature_type <> 'street' AS is_featurename_face_code
    FROM proto
    LEFT JOIN proto_flips ON proto.globalid = proto_flips.globalid
    LEFT JOIN primary_segments ON proto.segmentid = primary_segments.segmentid
    LEFT JOIN facecode ON proto.b7sc = facecode.b7sc
    LEFT JOIN feature_type_codes ON proto.feature_type_code IS NOT DISTINCT FROM feature_type_codes.code
)
SELECT
    proto.borough AS boroughcode,
    {% if var('fix_featurename_face_code_protos') -%}
        proto.face_code_fixed
    {%- else -%}
        proto.face_code_prod
    {%- endif %} AS face_code,
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
        WHEN proto.geometry_reversed THEN ST_REVERSE(primary_segments.geom)
        ELSE primary_segments.geom
    END AS geom,
    CASE
        WHEN proto.geometry_reversed THEN ST_REVERSE(primary_segments.raw_geom)
        ELSE primary_segments.raw_geom
    END AS raw_geom,
    primary_segments.midpoint,
    CASE
        WHEN proto.geometry_reversed THEN primary_segments.end_point
        ELSE primary_segments.start_point
    END AS start_point,
    CASE
        WHEN proto.geometry_reversed THEN primary_segments.start_point
        ELSE primary_segments.end_point
    END AS end_point,
    primary_segments.shape_length,
    feature_type_codes.source_feature_class AS feature_type,
    primary_segments.feature_type AS primary_feature_type,
    feature_type_codes.description AS feature_type_description,
    proto.alt_segdata_type,
    proto.source_table,
    -- file order: right after the parent segment, in AltSegmentData OBJECTID order
    primary_segments.source_class_rank,
    primary_segments.source_objectid,
    proto.ogc_fid AS proto_objectid,
    proto.globalid,
    proto.reversed,
    proto.flips_before,
    proto.geometry_reversed_fixed,
    proto.geometry_reversed_prod,
    proto.geometry_reversed,
    proto.face_code_fixed,
    proto.face_code_prod,
    proto.is_featurename_face_code
FROM oriented AS proto
LEFT JOIN primary_segments ON proto.segmentid = primary_segments.segmentid
LEFT JOIN seqnum ON proto.globalid = seqnum.globalid
LEFT JOIN feature_type_codes ON proto.feature_type_code IS NOT DISTINCT FROM feature_type_codes.code -- NULL -> centerline
