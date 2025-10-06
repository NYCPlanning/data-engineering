{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['geom'], 'type': 'gist'},
      {'columns': ['midpoint'], 'type': 'gist'},
      {'columns': ['segmentid']}
    ]
) }}

SELECT
    rail.segmentid,
    geoms.geom,
    geoms.midpoint,
    geoms.source_feature_type
FROM {{ ref("stg__rail_and_subway") }} AS rail
INNER JOIN {{ ref("int__segments") }} AS geoms ON rail.segmentid = geoms.segmentid
WHERE rail.row_type = '1'
