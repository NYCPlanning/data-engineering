{{ config(
    materialized='table',
    indexes=[{'columns': ['geom'], 'type': 'gist'}]
) }}

-- Election districts (entity_id is assembly district * 1000 + ED). Boundaries come from
-- the AtomicPolygon topology (int__boundary__ed); columns, column order and types match
-- the published layer.

WITH boundaries AS (
    SELECT
        entity_id,
        st_multi(geom) AS geom
    FROM {{ ref('int__boundary__ed') }}
)

SELECT
    b.entity_id::int AS "ElectDist",
    b.geom,
    st_perimeter(b.geom) AS "SHAPE_Length",
    st_area(b.geom) AS "SHAPE_Area"
FROM boundaries AS b
