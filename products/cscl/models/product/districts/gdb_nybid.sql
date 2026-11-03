{{ config(
    materialized='table'
) }}

-- Business improvement districts, as stored in CSCL, not clipped. A BID whose source
-- polygon is invalid (BIDs 44 and 56 on 26c) is published as stored, as prod does, unless
-- make_valid_district_polygons is on, which publishes it made valid (CSCL-DISTRICTS-04).
-- SHAPE_Length and SHAPE_Area are the valid polygon's either way, as prod's are.

SELECT
    d.bidid::int AS "BIDID",
    d.bid AS "BID",
    d.borough AS "BOROUGH",
    {% if var('make_valid_district_polygons') -%}
        d.geom
    {%- else -%}
        coalesce(inv.source_geom, d.geom)
    {%- endif %} AS geom,
    st_perimeter(d.geom) AS "SHAPE_Length",
    st_area(d.geom) AS "SHAPE_Area"
FROM {{ ref('stg__businessimprovementdistrict') }} AS d
LEFT JOIN {{ ref('int__district_invalid_source_polygons') }} AS inv
    ON inv.source_layer = 'nybid' AND d.bidid::varchar = inv.key_value
