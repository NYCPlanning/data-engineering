{{ config(
    materialized='table',
    indexes=[{'columns': ['geom'], 'type': 'gist'}]
) }}

-- MUNICOURT '00' is an unassigned placeholder (6 of the 34 source rows) and is
-- excluded, which is what takes prod to 28 features.

SELECT
    b.borocode::int AS "BoroCode",
    b.boroname AS "BoroName",
    d.municourt AS "MuniCourt",
    d.geom,
    st_perimeter(d.geom) AS "SHAPE_Length",
    st_area(d.geom) AS "SHAPE_Area"
FROM {{ ref('stg__municourtdistrict') }} AS d
INNER JOIN {{ ref('stg__borough') }} AS b ON d.borough = b.borocode
WHERE d.municourt != '00'
