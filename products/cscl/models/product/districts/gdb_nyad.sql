{{ config(
    materialized='table',
    indexes=[{'columns': ['geom'], 'type': 'gist'}]
) }}

-- State assembly districts. Boundaries come from the AtomicPolygon topology
-- (int__boundary__ad); columns, column order and types match the published layer.

WITH boundaries AS (
    SELECT
        entity_id,
        st_multi(geom) AS geom
    FROM {{ ref('int__boundary__ad') }}
)

SELECT
    b.entity_id::smallint AS "AssemDist",
    b.geom,
    st_perimeter(b.geom) AS "SHAPE_Length",
    st_area(b.geom) AS "SHAPE_Area"
FROM boundaries AS b
