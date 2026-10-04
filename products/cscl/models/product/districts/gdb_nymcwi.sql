{{ config(
    materialized='table',
    indexes=[{'columns': ['geom'], 'type': 'gist'}]
) }}

-- Municipal court districts (entity_id is borocode(1) || court(2); the '00' placeholder
-- never gets an id), water included (the *wi variant). Boundaries come from the
-- AtomicPolygon topology (int__boundary__mcwi); columns, column order and types match
-- the published layer.

WITH boundaries AS (
    SELECT
        entity_id,
        st_multi(geom) AS geom
    FROM {{ ref('int__boundary__mcwi') }}
)

SELECT
    left(b.entity_id, 1)::smallint AS "BoroCode",
    boro.boroname::varchar(32) AS "BoroName",
    substr(b.entity_id, 2)::varchar(2) AS "MuniCourt",
    b.geom,
    st_perimeter(b.geom) AS "SHAPE_Length",
    st_area(b.geom) AS "SHAPE_Area"
FROM boundaries AS b
LEFT JOIN {{ ref('stg__borough') }} AS boro ON left(b.entity_id, 1) = boro.borocode
