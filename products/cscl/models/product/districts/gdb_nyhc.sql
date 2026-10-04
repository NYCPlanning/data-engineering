{{ config(
    materialized='table',
    indexes=[{'columns': ['geom'], 'type': 'gist'}]
) }}

-- Health center districts (the first digit is the borough). Boundaries come from the
-- AtomicPolygon topology (int__boundary__hc); columns, column order and types match the
-- published layer.

WITH boundaries AS (
    SELECT
        entity_id,
        st_multi(geom) AS geom
    FROM {{ ref('int__boundary__hc') }}
)

SELECT
    left(b.entity_id, 1)::smallint AS "BoroCode",
    boro.boroname::varchar(32) AS "BoroName",
    b.entity_id::smallint AS "HCentDist",
    b.geom,
    st_perimeter(b.geom) AS "SHAPE_Length",
    st_area(b.geom) AS "SHAPE_Area"
FROM boundaries AS b
LEFT JOIN {{ ref('stg__borough') }} AS boro ON left(b.entity_id, 1) = boro.borocode
