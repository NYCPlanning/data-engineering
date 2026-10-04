{{ config(
    materialized='table',
    indexes=[{'columns': ['geom'], 'type': 'gist'}]
) }}

-- Fire companies (entity_id is the company's UNIT_SHORT, e.g. 'E 14'). Boundaries come
-- from the AtomicPolygon topology (int__boundary__fc); columns, column order and types
-- match the published layer.

WITH boundaries AS (
    SELECT
        entity_id,
        st_multi(geom) AS geom
    FROM {{ ref('int__boundary__fc') }}
)

SELECT
    split_part(b.entity_id, ' ', 1)::varchar(1) AS "FireCoType",
    split_part(b.entity_id, ' ', 2)::smallint AS "FireCoNum",
    fc.fire_battalion::smallint AS "FireBN",
    fc.fire_division::smallint AS "FireDiv",
    b.geom,
    st_perimeter(b.geom) AS "SHAPE_Length",
    st_area(b.geom) AS "SHAPE_Area"
FROM boundaries AS b
LEFT JOIN {{ ref('thinfire_by_field_unformatted') }} AS fc ON b.entity_id = fc.unit_short
