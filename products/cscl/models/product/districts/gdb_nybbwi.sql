{{ config(
    materialized='table',
    indexes=[{'columns': ['geom'], 'type': 'gist'}]
) }}

-- Borough boundaries, water included (the *wi variant). Boundaries come from the
-- AtomicPolygon topology (int__boundary__bbwi); columns, column order and types match
-- the published layer.

WITH boundaries AS (
    SELECT
        entity_id,
        st_multi(geom) AS geom
    FROM {{ ref('int__boundary__bbwi') }}
)

SELECT
    b.entity_id::smallint AS "BoroCode",
    boro.boroname::varchar(32) AS "BoroName",
    b.geom,
    st_perimeter(b.geom) AS "SHAPE_Length",
    st_area(b.geom) AS "SHAPE_Area"
FROM boundaries AS b
LEFT JOIN {{ ref('stg__borough') }} AS boro ON b.entity_id::text = boro.borocode
