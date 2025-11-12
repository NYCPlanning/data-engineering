/*
TODO - this logic is a little duplicated at the moment
    with int__protosegments and int__segments. But it also seems
    annoying to have multiple segment intermediate models as we
    add face code, seqnum, etc one at a time.

    For now, slightly duplicated logic. But we should revisit

    Also TODO - get this actually matching prod
*/
{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['source_table', 'unique_id']},
    ]
) }}
WITH nsf AS (
    SELECT * FROM {{ ref('stg__nonstreetfeatures') }}
),
facecode AS (
    SELECT * FROM {{ ref('int__streetcode_and_facecode') }}
),
proto AS (
    SELECT * FROM {{ ref('stg__altsegmentdata_proto') }}
    WHERE feature_type = 'nonstreetfeatures'
),
proto_facecode AS (
    SELECT * FROM {{ ref('int__b7sc_codes') }}
),
all_nsf_segments AS (
    SELECT
        'dcp_cscl_nonstreetfeatures' AS source_table,
        nsf.segmentid AS unique_id,
        nsf.segmentid,
        nsf.ogc_fid,
        facecode.boroughcode,
        facecode.face_code
    FROM nsf
    INNER JOIN facecode ON nsf.segmentid = facecode.segmentid
    UNION ALL
    SELECT
        proto.source_table,
        proto.ogc_fid AS unique_id,
        proto.segmentid,
        proto.ogc_fid,
        proto.borough AS boroughcode,
        proto_facecode.feature_facecode AS face_code
    FROM proto
    INNER JOIN proto_facecode ON proto.b7sc = proto_facecode.b7sc
)
SELECT
    source_table,
    unique_id,
    segmentid,
    ogc_fid,
    boroughcode,
    face_code,
    LPAD(ROW_NUMBER() OVER (
        PARTITION BY boroughcode, face_code
        ORDER BY source_table DESC, ogc_fid ASC
    )::TEXT, 5, '0') AS segment_seqnum
FROM all_nsf_segments
