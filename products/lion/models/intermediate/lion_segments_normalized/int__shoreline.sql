{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['segmentid']}
    ]
) }}
SELECT
    boro.boroughcode,
    shoreline.segmentid,
    shoreline.segment_seqnum,
    '2' AS feature_type_code,
    '$' AS from_level_code,
    '$' AS to_level_code,
    'U' AS segment_type,
    NULL::INT AS legacy_segmentid,
    geom,
    shape_length,
    'shoreline' AS source_feature_type,
    TRUE AS include_in_geosupport_lion,
    TRUE AS include_in_bytes_lion
FROM {{ source("recipe_sources", "dcp_cscl_shoreline") }} AS shoreline
LEFT JOIN {{ ref("int__streetcode_and_facecode") }} AS boro ON shoreline.segmentid = boro.segmentid
