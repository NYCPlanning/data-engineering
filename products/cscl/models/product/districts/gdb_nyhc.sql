{{ config(
    materialized='table',
    indexes=[{'columns': ['geom'], 'type': 'gist'}]
) }}

-- Health center districts are numbered by borough (e.g. 3x = Brooklyn); the source
-- carries no borough column, so the leading digit is the join key.

WITH clipped AS (
    SELECT
        b.borocode::int AS "BoroCode",
        b.boroname AS "BoroName",
        d.hcentdist::int AS "HCentDist",
        {{ clipped_geom('d.geom') }} AS geom
    FROM {{ ref('stg__healthcenterdistrict') }} AS d
    INNER JOIN {{ ref('stg__borough') }} AS b ON left(d.hcentdist, 1) = b.borocode
    {{ clip_to_shoreline('d.geom') }}
)

SELECT
    *,
    st_perimeter(geom) AS "SHAPE_Length",
    st_area(geom) AS "SHAPE_Area"
FROM clipped
WHERE NOT st_isempty(geom)
