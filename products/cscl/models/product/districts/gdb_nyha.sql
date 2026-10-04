{{ config(
    materialized='table',
    indexes=[{'columns': ['geom'], 'type': 'gist'}]
) }}

-- Health areas (entity_id is borocode(1) || health area(4)). Boundaries come from the
-- AtomicPolygon topology (int__boundary__ha); columns, column order and types match the
-- published layer.

WITH boundaries AS (
    SELECT
        entity_id,
        st_multi(geom) AS geom
    FROM {{ ref('int__boundary__ha') }}
)

SELECT
    left(b.entity_id, 1)::smallint AS "BoroCode",
    boro.boroname::varchar(32) AS "BoroName",
    right(b.entity_id, 4)::smallint AS "HealthArea",
    b.geom,
    st_perimeter(b.geom) AS "SHAPE_Length",
    st_area(b.geom) AS "SHAPE_Area"
FROM boundaries AS b
LEFT JOIN {{ ref('stg__borough') }} AS boro ON left(b.entity_id, 1) = boro.borocode
