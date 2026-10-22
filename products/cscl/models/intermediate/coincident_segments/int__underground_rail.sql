{{ config(
    materialized = 'table'
) }}

SELECT
    rail.segmentid,
    geoms.geom,
    geoms.midpoint,
    rail.source_table AS rail_type
FROM {{ ref("stg__rail_and_subway") }} AS rail
INNER JOIN {{ ref("int__primary_segments") }} AS geoms ON rail.segmentid = geoms.segmentid
WHERE rail.right_of_way_type = '1'
