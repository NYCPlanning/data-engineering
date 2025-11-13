{{ config(
    materialized = 'table',
    indexes=[ {'columns': ['segmentid']} ]
) }}

SELECT
    segmentid,
    row_type AS right_of_way_type,
    segment_seqnum,
    legacy_segmentid,
    from_level_code,
    to_level_code,
    '1' AS feature_type_code,
    'rail_and_subway' AS feature_type,
    'rail' AS source_table,
    row_type NOT IN ('1', '8') AS include_in_geosupport_lion,
    geom,
    shape_length
FROM {{ source("recipe_sources", "dcp_cscl_rail") }}
UNION ALL
SELECT
    segmentid,
    row_type AS right_of_way_type,
    segment_seqnum,
    legacy_segmentid,
    from_level_code,
    to_level_code,
    '1' AS feature_type_code,
    'rail_and_subway' AS feature_type,
    'subway' AS source_table,
    row_type NOT IN ('1', '8') AS include_in_geosupport_lion,
    geom,
    shape_length
FROM {{ source("recipe_sources", "dcp_cscl_subway") }}
