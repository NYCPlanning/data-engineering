{{ config(
    materialized='table',
    indexes=[{'columns': ['geom'], 'type': 'gist'}]
) }}

-- Hurricane evacuation zones. Boundaries come from the AtomicPolygon topology
-- (int__boundary__hez); columns, column order and types match the published layer.

WITH boundaries AS (
    SELECT
        entity_id,
        st_multi(geom) AS geom
    FROM {{ ref('int__boundary__hez') }}
)

SELECT
    b.entity_id::varchar(50) AS "HURRICANE_EVACUATION_ZONE",
    b.geom,
    st_perimeter(b.geom) AS "SHAPE_Length",
    st_area(b.geom) AS "SHAPE_Area"
FROM boundaries AS b
