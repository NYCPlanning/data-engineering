{{ config(
    materialized='table',
    indexes=[{'columns': ['geom'], 'type': 'gist'}]
) }}

-- UNIT_SHORT packs company type and number into one field (e.g. 'E 14').

WITH clipped AS (
    SELECT
        split_part(d.unit_short, ' ', 1) AS "FireCoType",
        split_part(d.unit_short, ' ', 2)::int AS "FireCoNum",
        d.fire_battalion::int AS "FireBN",
        d.fire_division::int AS "FireDiv",
        {{ clipped_geom('d.geom') }} AS geom
    FROM {{ ref('stg__firecompany') }} AS d
    {{ clip_to_shoreline('d.geom') }}
)

SELECT
    *,
    st_perimeter(geom) AS "SHAPE_Length",
    st_area(geom) AS "SHAPE_Area"
FROM clipped
WHERE NOT st_isempty(geom)
