{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['saf_globalid']}
    ]
) }}

WITH saf_segment_records AS (
    SELECT
        globalid,
        borough AS boroughcode,
        saftype,
        segmentid,
        'altsegmentdata' AS saf_source_table
    FROM {{ ref('stg__altsegmentdata_saf') }}
    UNION ALL
    SELECT
        globalid,
        boroughcode,
        special_condition AS saftype,
        segmentid,
        'addresspoints' AS saf_source_table
    FROM {{ source('recipe_sources', 'dcp_cscl_addresspoints') }}
    WHERE special_condition IN ('S', 'V') -- TODO staging table maybe - these two handled differently though
    UNION ALL
    SELECT
        globalid,
        boroughcode,
        saftype,
        segmentid,
        'commonplace' AS saf_source_table
    FROM {{ source('recipe_sources', 'dcp_cscl_commonplace_gdb') }}
    WHERE saftype IN ('G', 'N', 'X') AND b7sc IS NOT NULL AND security_level <> '3' -- TODO staging table
),
roadbed_pointer_list AS (
    SELECT * FROM {{ source("recipe_sources", "dcp_cscl_roadbed_pointer_list") }}
),
with_roadbed_to_generic AS (
    SELECT
        globalid,
        boroughcode,
        saftype,
        segmentid,
        segmentid AS base_segmentid,
        saf_source_table,
        FALSE AS rpl_flag
    FROM saf_segment_records
    UNION ALL
    SELECT
        saf.globalid,
        saf.boroughcode,
        saf.saftype,
        rpl.generic_segmentid AS segmentid,
        rpl.roadbed_segmentid AS base_segmentid,
        saf.saf_source_table,
        TRUE AS rpl_flag
    FROM saf_segment_records AS saf
    INNER JOIN roadbed_pointer_list AS rpl ON saf.segmentid = rpl.roadbed_segmentid
    WHERE saf.saftype IN ('G', 'N', 'S', 'V', 'X')
),
lion AS (
    SELECT * FROM {{ ref("int__lion") }}
),
resolved_segment AS (
    SELECT
        saf.globalid AS saf_globalid,
        saf.boroughcode,
        saf.saftype,
        saf.segmentid,
        COALESCE(proto.lionkey, primary_segment.lionkey) AS segment_lionkey,
        COALESCE(proto.segment_type, primary_segment.segment_type) AS segment_type,
        COALESCE(proto.segment_type, primary_segment.segment_type) AS segment_incex_flag,
        saf.saf_source_table
        --saf.source_attrs
    FROM with_roadbed_to_generic AS saf
    LEFT JOIN lion AS primary_segment -- TODO error report when none joined?
        ON
            saf.segmentid = primary_segment.segmentid
            AND (
                primary_segment.source_table = 'centerline'
                OR (saf.saf_source_table = 'commonplace' AND primary_segment.source_table = 'shoreline')
            )
    LEFT JOIN lion AS proto
        ON
            saf.segmentid = proto.segmentid
            AND primary_segment.source_table = 'centerline'
            AND proto.source_table = 'altsegmentdata'
            AND saf.boroughcode = proto.boroughcode
            AND primary_segment.boroughcode <> proto.boroughcode
            AND NOT saf.rpl_flag
)
SELECT
    *,
    segment_type IN ('B', 'E', 'U') OR (segment_type = 'G' AND segment_incex_flag <> 'E') AS generic,
    segment_type IN ('E', 'R', 'S', 'U') AS roadbed
FROM resolved_segment
