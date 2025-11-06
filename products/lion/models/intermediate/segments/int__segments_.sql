{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['lionkey_dev']}
    ]
) }}

WITH primary_segments AS (
    SELECT * FROM {{ ref("int__segment_geometries")}}
),
lgc AS (
    SELECT * from {{ ref("int__lgc_rank") }}
),
facecode AS (
    SELECT * FROM {{ ref("int__b7sc_codes") }}
),
proto AS (
    SELECT * FROM {{ ref("int__protosegments") }}
),
primary_segments_with_facecode AS (
    SELECT 
        CASE 
            WHEN feature_type = 'centerline' THEN lgc.borough_code -- TODO - need centerline attribute
            ELSE lgc.borough_code
        END AS boroughcode,
        CASE
            WHEN feature_type = 'centerline' THEN COALESCE(facecode.street_facecode, facecode.feature_facecode)
            ELSE facecode.feature_facecode
        END AS face_code,
        CASE
            WHEN feature_type <> 'nonstreetfeatures' THEN NULL::INT -- todo segment.segment_seqnum
        END AS segment_seqnum,
        segment.segmentid,
        segment.geom,
        segment.midpoint,
        segment.start_point,
        segment.end_point,
        segment.shape_length,
        segment.feature_type,
        segment.source_table
    FROM primary_segments AS segment
    INNER JOIN lgc ON segment.segmentid = lgc.segmentid AND lgc.lgc_rank = 1
    INNER JOIN facecode ON lgc.b7sc = facecode.b7sc
),
segments AS (
    SELECT
        {{ dbt_utils.star(ref('int__protosegments')) }}
    FROM primary_segments_with_facecode
    UNION ALL
    SELECT
        {{ dbt_utils.star(ref('int__protosegments')) }}
    FROM proto
)
SELECT
    CONCAT(boroughcode, face_code, segment_seqnum, segmentid) AS lionkey_dev, -- TODO remove segmentid, rename field
    *
FROM segments
ORDER BY lionkey_dev 
