{{ config(
    materialized='table',
    indexes=[{'columns': ['geom'], 'type': 'gist'}]
) }}

-- Congressional districts, water included (the *wi variant). Boundaries come from the
-- AtomicPolygon topology (int__boundary__cgwi); columns, column order and types match
-- the published layer.

WITH boundaries AS (
    SELECT
        entity_id,
        st_multi(geom) AS geom
    FROM {{ ref('int__boundary__cgwi') }}
)

SELECT
    b.entity_id::smallint AS "CongDist",
    b.geom,
    st_perimeter(b.geom) AS "SHAPE_Length",
    st_area(b.geom) AS "SHAPE_Area"
FROM boundaries AS b
