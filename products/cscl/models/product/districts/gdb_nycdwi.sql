{{ config(
    materialized='table'
) }}

-- Community districts, water included (the *wi variant). Boundaries come from the
-- AtomicPolygon topology (int__boundary__cdwi); columns, column order and types match
-- the published layer.
-- A district whose source polygon is invalid is published as stored, as prod copies it,
-- unless make_valid_district_polygons is on, which publishes the topology's (valid)
-- polygon (CSCL-DISTRICTS-04). SHAPE_Length and SHAPE_Area are the topology polygon's
-- either way, which match prod's.

WITH boundaries AS (
    SELECT
        entity_id,
        st_multi(geom) AS geom
    FROM {{ ref('int__boundary__cdwi') }}
)

SELECT
    b.entity_id::smallint AS "BoroCD",
    {% if var('make_valid_district_polygons') -%}
        b.geom
    {%- else -%}
        coalesce(st_multi(inv.source_geom), b.geom)
    {%- endif %} AS geom,
    st_perimeter(b.geom) AS "SHAPE_Length",
    st_area(b.geom) AS "SHAPE_Area"
FROM boundaries AS b
LEFT JOIN {{ ref('int__district_invalid_source_polygons') }} AS inv
    ON inv.source_layer = 'nycdwi' AND b.entity_id::varchar = inv.key_value
