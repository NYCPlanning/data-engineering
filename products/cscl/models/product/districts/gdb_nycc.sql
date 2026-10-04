{{ config(
    materialized='table',
    indexes=[{'columns': ['geom'], 'type': 'gist'}]
) }}

-- City council districts. Boundaries come from the AtomicPolygon topology
-- (int__boundary__cc); columns, column order and types match the published layer.

WITH boundaries AS (
    SELECT
        entity_id,
        st_multi(geom) AS geom
    FROM {{ ref('int__boundary__cc') }}
)

SELECT
    b.entity_id::smallint AS "CounDist",
    b.geom,
    st_perimeter(b.geom) AS "SHAPE_Length",
    st_area(b.geom) AS "SHAPE_Area"
FROM boundaries AS b
