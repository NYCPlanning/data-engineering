{{ config(
    materialized='table',
    indexes=[{'columns': ['geom'], 'type': 'gist'}]
) }}

-- 2020 census blocks (entity_id is BCTCB2020: borocode(1) || tract(6) || block(4)).
-- Boundaries come from the AtomicPolygon topology (int__boundary__cb2020); columns,
-- column order and types match the published layer.

WITH boundaries AS (
    SELECT
        entity_id,
        st_multi(geom) AS geom
    FROM {{ ref('int__boundary__cb2020') }}
)

SELECT
    right(b.entity_id, 4)::varchar(4) AS "CB2020",
    left(b.entity_id, 1)::varchar(1) AS "BoroCode",
    boro.boroname::varchar(32) AS "BoroName",
    substr(b.entity_id, 2, 6)::varchar(6) AS "CT2020",
    b.entity_id::varchar(11) AS "BCTCB2020",
    ('36' || boro.fips || substr(b.entity_id, 2))::varchar(15) AS "GEOID",
    b.geom,
    st_perimeter(b.geom) AS "SHAPE_Length",
    st_area(b.geom) AS "SHAPE_Area"
FROM boundaries AS b
LEFT JOIN {{ ref('stg__borough') }} AS boro ON left(b.entity_id, 1) = boro.borocode
