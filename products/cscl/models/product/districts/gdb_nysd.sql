{{ config(
    materialized='table',
    indexes=[{'columns': ['geom'], 'type': 'gist'}]
) }}

-- School districts; District 10 is published once per borough, every other district whole. Boundaries come from
-- the AtomicPolygon topology (int__boundary__sd_boro); columns, column order and types
-- match the published layer.

WITH boundaries AS (
    SELECT
        entity_id,
        st_multi(geom) AS geom
    FROM {{ ref('int__boundary__sd_boro') }}
)

SELECT
    split_part(b.entity_id, '-', 1)::smallint AS "SchoolDist",
    b.geom,
    st_perimeter(b.geom) AS "SHAPE_Length",
    st_area(b.geom) AS "SHAPE_Area"
FROM boundaries AS b
