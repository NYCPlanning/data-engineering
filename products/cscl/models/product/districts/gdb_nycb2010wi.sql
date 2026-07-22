{{ config(
    materialized='table',
    indexes=[{'columns': ['geom'], 'type': 'gist'}]
) }}

SELECT
    d.cb AS "CB2010",
    d.borocode AS "BoroCode",
    b.boroname AS "BoroName",
    d.ct AS "CT2010",
    d.bctcb AS "BCTCB2010",
    d.geom,
    st_perimeter(d.geom) AS "SHAPE_Length",
    st_area(d.geom) AS "SHAPE_Area"
FROM {{ ref('stg__censusblock2010') }} AS d
INNER JOIN {{ ref('stg__borough') }} AS b ON d.borocode = b.borocode
