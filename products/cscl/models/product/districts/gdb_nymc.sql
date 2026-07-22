{{ config(
    materialized='table',
    indexes=[{'columns': ['geom'], 'type': 'gist'}]
) }}

-- MUNICOURT '00' is an unassigned placeholder (6 of the 34 source rows) and is
-- excluded, which is what takes prod to 28 features.

WITH clipped AS (
    SELECT
        b.borocode::int AS "BoroCode",
        b.boroname AS "BoroName",
        d.municourt AS "MuniCourt",
        {{ clipped_geom('d.geom') }} AS geom
    FROM {{ ref('stg__municourtdistrict') }} AS d
    INNER JOIN {{ ref('stg__borough') }} AS b ON d.borough = b.borocode
    {{ clip_to_shoreline('d.geom') }}
    WHERE d.municourt != '00'
)

SELECT
    *,
    st_perimeter(geom) AS "SHAPE_Length",
    st_area(geom) AS "SHAPE_Area"
FROM clipped
WHERE NOT st_isempty(geom)
