{{ config(
    materialized='table',
    indexes=[{'columns': ['geom'], 'type': 'gist'}]
) }}

WITH clipped AS (
    SELECT
        b.borocode::int AS "BoroCode",
        b.boroname AS "BoroName",
        d.healtharea::int AS "HealthArea",
        {{ clipped_geom('d.geom') }} AS geom
    FROM {{ ref('stg__healtharea') }} AS d
    INNER JOIN {{ ref('stg__borough') }} AS b ON d.borough = b.borocode
    {{ clip_to_shoreline('d.geom') }}
)

SELECT
    *,
    st_perimeter(geom) AS "SHAPE_Length",
    st_area(geom) AS "SHAPE_Area"
FROM clipped
WHERE NOT st_isempty(geom)
