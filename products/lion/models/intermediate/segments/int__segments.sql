{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['lionkey_dev']}
    ]
) }}

WITH primary_segments AS (
    SELECT * FROM {{ ref("int__primary_segments") }}
),

proto_segments AS (
    SELECT * FROM {{ ref("stg__altsegmentdata_proto") }}
),

resolved_proto_segments AS (
    SELECT
        -- proto-segment fields
        proto_segments.segmentid,
        primary_segments.segmentid IS NOT NULL AS is_based_on_primary_segment,
        primary_segments.segmentid AS primary_segment_segmentid,
        proto_segments.source_table,
        proto_segments.feature_type,
        proto_segments.boroughcode,
        proto_segments.face_code,
        proto_segments.segment_seqnum,
        proto_segments.five_digit_street_code,
        proto_segments.lgc1,
        proto_segments.lgc2,
        proto_segments.lgc3,
        proto_segments.lgc4,
        proto_segments.lgc5,
        proto_segments.lgc6,
        proto_segments.lgc7,
        proto_segments.lgc8,
        proto_segments.lgc9,
        proto_segments.boe_lgc_pointer,
        -- primary segment fields
        legacy_segmentid,
        shape_length,
        from_level_code,
        to_level_code,
        midpoint,
        start_point,
        end_point,
        include_in_geosupport_lion,
        include_in_bytes_lion,
        geom
    FROM
        proto_segments
    LEFT JOIN primary_segments ON proto_segments.segmentid = primary_segments.segmentid
),

-- We exclude the sqlfluff rule ambiguous.column_count
-- https://docs.sqlfluff.com/en/stable/reference/rules.html#query-produces-an-unknown-number-of-result-columns
-- but UNION clauses require the inputs have the same number of columns and compatible types
-- so columns from int__primary_segments have to be explicitly specified at some point 
segments AS (
    SELECT
        segmentid,
        segmentid AS primary_segment_segmentid,
        boroughcode,
        face_code,
        segment_seqnum,
        feature_type,
        source_table,
        five_digit_street_code,
        lgc1,
        lgc2,
        lgc3,
        lgc4,
        lgc5,
        lgc6,
        lgc7,
        lgc8,
        lgc9,
        boe_lgc_pointer,
        legacy_segmentid,
        shape_length,
        from_level_code,
        to_level_code,
        midpoint,
        start_point,
        end_point,
        TRUE AS is_based_on_primary_segment,
        include_in_geosupport_lion,
        include_in_bytes_lion,
        geom
    FROM primary_segments
    UNION ALL
    SELECT
        segmentid,
        primary_segment_segmentid,
        boroughcode,
        face_code,
        segment_seqnum,
        feature_type,
        source_table,
        five_digit_street_code,
        lgc1,
        lgc2,
        lgc3,
        lgc4,
        lgc5,
        lgc6,
        lgc7,
        lgc8,
        lgc9,
        boe_lgc_pointer,
        legacy_segmentid,
        shape_length,
        from_level_code,
        to_level_code,
        midpoint,
        start_point,
        end_point,
        is_based_on_primary_segment,
        include_in_geosupport_lion,
        include_in_bytes_lion,
        geom
    FROM resolved_proto_segments
)

SELECT
    CONCAT(segmentid, boroughcode, face_code, segment_seqnum) AS lionkey_dev,
    *
FROM segments
ORDER BY lionkey_dev DESC
